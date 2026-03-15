# frozen_string_literal: true

require "json"
require "zlib"
require_relative "../pipeline"
require_relative "../pipeline_parser"

module Jrf
  class CLI
    class Runner
      RS_CHAR = "\x1e"
      DEFAULT_OUTPUT_BUFFER_LIMIT = 4096
      PARALLEL_FRAME_HEADER_BYTES = 4

      class RsNormalizer
        def initialize(input)
          @input = input
        end

        def read(length = nil, outbuf = nil)
          chunk = @input.read(length)
          return nil if chunk.nil?

          chunk.tr!(RS_CHAR, "\n")
          if outbuf
            outbuf.replace(chunk)
          else
            chunk
          end
        end
      end

      class ParallelFrameReader
        def initialize
          @buf = +""
          @offset = 0
        end

        def append(chunk)
          @buf << chunk
        end

        def each_payload
          while (payload = next_payload)
            yield payload
          end
        end

        def has_partial?
          @offset != @buf.bytesize
        end

        private

        def next_payload
          if @buf.bytesize - @offset < PARALLEL_FRAME_HEADER_BYTES
            compact!
            return nil
          end

          payload_len = @buf.byteslice(@offset, PARALLEL_FRAME_HEADER_BYTES).unpack1("N")
          frame_len = PARALLEL_FRAME_HEADER_BYTES + payload_len
          if @buf.bytesize - @offset < frame_len
            compact!
            return nil
          end

          payload = @buf.byteslice(@offset + PARALLEL_FRAME_HEADER_BYTES, payload_len)
          @offset += frame_len
          payload
        end

        def compact!
          if @offset > 0
            @buf.slice!(0, @offset)
            @offset = 0
          end
        end
      end

      def initialize(input: $stdin, out: $stdout, err: $stderr, lax: false, output_format: :json, atomic_write_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT)
        if input.is_a?(Array)
          @file_paths = input
          @stdin = nil
        else
          @file_paths = []
          @stdin = input
        end
        @out = out
        @err = err
        @lax = lax
        @output_format = output_format
        @atomic_write_bytes = atomic_write_bytes
        @output_buffer = +""
        @input_errors = false
      end

      def input_errors?
        @input_errors
      end

      def run(expression, parallel: 1, verbose: false)
        blocks = build_stage_blocks(expression, verbose: verbose)
        if @output_format == :tsv
          values = []
          process_values(blocks, parallel: parallel, verbose: verbose) do |value|
            values << value
          end
          emit_tsv(values)
        else
          process_values(blocks, parallel: parallel, verbose: verbose) do |value|
            emit_output(value)
          end
        end
      ensure
        write_output(@output_buffer)
      end

      private

      def build_stage_blocks(expression, verbose:)
        parsed = PipelineParser.new(expression).parse
        stages = parsed[:stages]
        dump_stages(stages) if verbose
        stages.map { |stage|
          eval("proc { #{stage[:src]} }", nil, "(jrf stage)", 1) # rubocop:disable Security/Eval
        }
      end

      def apply_pipeline(blocks, input_enum)
        pipeline = Pipeline.new(*blocks)
        Enumerator.new do |y|
          pipeline.call(input_enum) { |value| y << value }
        end
      end

      def each_input_enum
        Enumerator.new { |y| each_input_value { |v| y << v } }
      end

      def process_values(blocks, parallel:, verbose:, &block)
        if parallel <= 1 || @file_paths.length <= 1
          # Single file or no parallelism requested — serial is the only option.
          # This also covers the all-files-empty case: no files means no workers to spawn.
          dump_parallel_status("disabled", verbose: verbose)
          return apply_pipeline(blocks, each_input_enum).each(&block)
        end

        split_index, probe_stage = classify_parallel_stages(blocks)
        if split_index.nil?
          dump_parallel_status("disabled", verbose: verbose)
          return apply_pipeline(blocks, each_input_enum).each(&block)
        end

        # If the first reducer stage is decomposable, workers run everything up to
        # and including it (map prefix + reducer), emit partial accumulators, and the
        # parent merges. This covers both pure reducers (split_index == 0, e.g. `sum(_)`)
        # and map-then-reduce (split_index > 0, e.g. `select(...) >> sum(...)`).
        if probe_stage&.decomposable?
          worker_blocks = blocks[0..split_index]
          rest_blocks = blocks[(split_index + 1)..]
          return process_decomposable_parallel(worker_blocks, rest_blocks, probe_stage,
                                               parallel: parallel, verbose: verbose, &block)
        end

        if split_index == 0
          dump_parallel_status("disabled", verbose: verbose)
          return apply_pipeline(blocks, each_input_enum).each(&block)
        end

        map_blocks = blocks[0...split_index]
        reduce_blocks = blocks[split_index..]
        dump_parallel_status("enabled workers=#{parallel} files=#{@file_paths.length} split=#{split_index}/#{blocks.length}", verbose: verbose)
        input_enum = parallel_map_enum(map_blocks, parallel)
        (reduce_blocks.empty? ? input_enum : apply_pipeline(reduce_blocks, input_enum)).each(&block)
      end

      def dump_parallel_status(status, verbose:)
        @err.puts "parallel: #{status}" if verbose
      end

      # Returns [split_index, probe_stage] where split_index is the index of the
      # first reducer stage (or blocks.length if all are passthrough), and probe_stage
      # is the Stage object of that first reducer (nil if all passthrough or no input).
      def classify_parallel_stages(blocks)
        # Read the first row from the first file to probe stage modes
        first_value = nil
        open_file(@file_paths.first) do |stream|
          each_stream_value(stream) do |value|
            first_value = value
            break
          end
        end
        return [nil, nil] if first_value.nil?

        # Run the value through each stage independently to classify
        split_index = nil
        probe_stage = nil
        blocks.each_with_index do |block, i|
          probe_pipeline = Pipeline.new(block)
          probe_pipeline.call([first_value]) { |_| }
          stage = probe_pipeline.instance_variable_get(:@stages).first
          if stage.instance_variable_get(:@mode) == :reducer
            split_index = i
            probe_stage = stage
            break
          end
        end

        [split_index || blocks.length, probe_stage]
      end

      def process_decomposable_parallel(worker_blocks, rest_blocks, probe_stage, parallel:, verbose:, &block)
        dump_parallel_status("enabled workers=#{parallel} files=#{@file_paths.length} decompose=#{worker_blocks.length}/#{worker_blocks.length + rest_blocks.length}", verbose: verbose)

        # Workers run map prefix + reducer stage per file and emit partial accumulators.
        partials_list = []
        reducer_stage_index = worker_blocks.length - 1
        spawner = ->(path) do
          spawn_worker(worker_blocks, path) do |pipeline, input|
            pipeline.call(input) { |_| }
            # If the file was empty, the stage was never initialized (no reducers),
            # so skip emitting — the parent will simply not receive a partial for this worker.
            stage = pipeline.instance_variable_get(:@stages)[reducer_stage_index]
            partials = stage.partial_accumulators
            emit_parallel_frame(partials) unless partials.empty?
          end
        end
        children = run_parallel_worker_pool(parallel, spawner) { |v| partials_list << v }
        wait_for_parallel_children(children) if children
        return if partials_list.empty?

        # Reuse the probe stage (already initialized with reducer structure from classify).
        # Replace its accumulators with the first worker's partials, then merge the rest.
        probe_stage.replace_accumulators!(partials_list.first)
        partials_list.drop(1).each { |partials| probe_stage.merge_partials!(partials) }

        # Finish the reducer stage and pass results through any remaining stages.
        results = probe_stage.finish
        if rest_blocks.empty?
          results.each(&block)
        else
          apply_pipeline(rest_blocks, results.each).each(&block)
        end
      end

      # Forks a worker process that reads `path`, builds a pipeline from `blocks`,
      # and yields [pipeline, input_enum] to the caller's block for custom behavior.
      # Returns [read_io, pid].
      def spawn_worker(blocks, path)
        read_io, write_io = IO.pipe
        pid = fork do
          read_io.close
          @out = write_io
          @output_buffer = +""
          pipeline = Pipeline.new(*blocks)
          input_enum = Enumerator.new do |y|
            open_file(path) { |stream| each_stream_value(stream) { |v| y << v } }
          end
          worker_failed = false
          begin
            yield pipeline, input_enum
          rescue => e
            @err.puts "#{path}: #{e.message} (#{e.class})"
            worker_failed = true
          end
          write_output(@output_buffer)
          write_io.close
          exit!(worker_failed ? 1 : 0)
        end
        write_io.close
        [read_io, pid]
      end

      # Runs a pool of up to `num_workers` concurrent workers across all input files.
      # `spawner` is called with a file path and must return [read_io, pid].
      # Yields each decoded JSON value from worker output frames.
      def run_parallel_worker_pool(num_workers, spawner)
        file_queue = @file_paths.dup
        workers = {} # read_io => [reader, pid]
        children = []

        # Fill initial pool
        while workers.size < num_workers && !file_queue.empty?
          read_io, pid = spawner.call(file_queue.shift)
          workers[read_io] = [ParallelFrameReader.new, pid]
          children << pid
        end

        read_ios = workers.keys.dup

        until read_ios.empty?
          ready = IO.select(read_ios)
          ready[0].each do |io|
            reader = workers[io][0]
            chunk = io.read_nonblock(65536, exception: false)
            if chunk == :wait_readable
              next
            elsif chunk.nil?
              raise IOError, "truncated parallel frame from worker" if reader.has_partial?
              read_ios.delete(io)
              io.close
              workers.delete(io)

              # Spawn next worker if files remain
              unless file_queue.empty?
                read_io, pid = spawner.call(file_queue.shift)
                workers[read_io] = [ParallelFrameReader.new, pid]
                children << pid
                read_ios << read_io
              end
            else
              reader.append(chunk)
              reader.each_payload do |payload|
                yield JSON.parse(payload)
              end
            end
          end
        end

        children
      end

      def parallel_map_enum(map_blocks, num_workers)
        children = nil
        spawner = ->(path) do
          spawn_worker(map_blocks, path) do |pipeline, input|
            pipeline.call(input) { |value| emit_parallel_frame(value) }
          end
        end
        Enumerator.new do |y|
          children = run_parallel_worker_pool(num_workers, spawner) { |value| y << value }
        ensure
          wait_for_parallel_children(children) if children
        end
      end

      def wait_for_parallel_children(children)
        failed = false
        children.each do |pid|
          _, status = Process.waitpid2(pid)
          failed = true unless status.success?
        end
        exit(1) if failed
      end

      def emit_parallel_frame(value)
        payload = JSON.generate(value)
        buffer_output([payload.bytesize].pack("N") << payload)
      end

      def each_input_value
        each_input do |source|
          each_stream_value(source) { |value| yield value }
        end
      end

      def each_stream_value(stream)
        return each_stream_value_lax(stream) { |value| yield value } if @lax

        stream.each_line do |line|
          line.strip!
          next if line.empty?
          yield JSON.parse(line)
        end
      end

      def open_file(path)
        if path.end_with?(".gz")
          Zlib::GzipReader.open(path) { |source| yield source }
        else
          File.open(path, "rb") { |source| yield source }
        end
      end

      def each_stream_value_lax(stream)
        require "oj"
        Oj.sc_parse(streaming_json_handler_class.new { |value| yield value }, RsNormalizer.new(stream))
      rescue LoadError
        raise "oj is required for --lax mode (gem install oj)"
      rescue Oj::ParseError => e
        raise JSON::ParserError, e.message
      end

      def streaming_json_handler_class
        @streaming_json_handler_class ||= Class.new(Oj::ScHandler) do
          def initialize(&emit)
            @emit = emit
          end

          def hash_start = {}
          def hash_key(key) = key
          def hash_set(hash, key, value) = hash[key] = value
          def array_start = []
          def array_append(array, value) = array << value
          def add_value(value) = @emit.call(value)
        end
      end

      def dump_stages(stages)
        stages.each_with_index do |stage, i|
          @err.puts "stage[#{i}]: #{stage[:src]}"
        end
      end

      def each_input(&block)
        if @file_paths.empty?
          with_error_handling("<stdin>") { block.call(@stdin) }
        else
          @file_paths.each do |path|
            if path == "-"
              with_error_handling("<stdin>") { block.call(@stdin) }
            else
              with_error_handling(path) { open_file(path, &block) }
            end
          end
        end
      end

      def with_error_handling(name)
        yield
      rescue IOError, SystemCallError, Zlib::GzipFile::Error, JSON::ParserError => e
        @err.puts "#{name}: #{e.message} (#{e.class})"
        @input_errors = true
      end

      def emit_output(value)
        record = (@output_format == :pretty ? JSON.pretty_generate(value) : JSON.generate(value)) << "\n"
        buffer_output(record)
      end

      def emit_tsv(values)
        rows = values.flat_map { |value| value_to_rows(value) }
        rows.each do |row|
          buffer_output(row.join("\t") << "\n")
        end
      end

      def value_to_rows(value)
        case value
        when Hash
          value.map { |k, v|
            case v
            when Array
              [format_cell(k)] + v.map { |e| format_cell(e) }
            else
              [format_cell(k), format_cell(v)]
            end
          }
        when Array
          value.map { |row|
            case row
            when Array
              row.map { |e| format_cell(e) }
            else
              [format_cell(row)]
            end
          }
        else
          [[format_cell(value)]]
        end
      end

      def format_cell(value)
        case value
        when nil
          "null"
        when Numeric, String, true, false
          value.to_s
        else
          JSON.generate(value)
        end
      end

      def buffer_output(record)
        if @output_buffer.bytesize + record.bytesize <= @atomic_write_bytes
          @output_buffer << record
        else
          write_output(@output_buffer)
          @output_buffer = record
        end
      end

      def write_output(str)
        return if str.empty?

        total = 0
        while total < str.bytesize
          written = @out.syswrite(str.byteslice(total..))
          total += written
        end
      end
    end
  end
end
