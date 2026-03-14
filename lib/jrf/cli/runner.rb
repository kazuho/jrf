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
      PARALLEL_FRAME_COMPACT_THRESHOLD = 4096

      class RsNormalizer
        def initialize(input)
          @input = input
        end

        def read(length = nil, outbuf = nil)
          chunk = @input.read(length)
          return nil if chunk.nil?

          chunk = chunk.tr(RS_CHAR, "\n")
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

        def finish!
          compact!
          raise IOError, "truncated parallel frame from worker" unless @buf.empty?
        end

        private

        def next_payload
          return nil if @buf.bytesize - @offset < PARALLEL_FRAME_HEADER_BYTES

          payload_len = @buf.byteslice(@offset, PARALLEL_FRAME_HEADER_BYTES).unpack1("N")
          frame_len = PARALLEL_FRAME_HEADER_BYTES + payload_len
          return nil if @buf.bytesize - @offset < frame_len

          payload = @buf.byteslice(@offset + PARALLEL_FRAME_HEADER_BYTES, payload_len)
          @offset += frame_len
          compact!
          payload
        end

        def compact!
          return if @offset.zero?

          if @offset == @buf.bytesize
            @buf.clear
            @offset = 0
          elsif @offset >= PARALLEL_FRAME_COMPACT_THRESHOLD && @offset >= @buf.bytesize / 2
            @buf = @buf.byteslice(@offset..)
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

      def run(expression, parallel: nil, verbose: false)
        blocks = build_stage_blocks(expression, verbose: verbose)
        emit_values(processed_values(blocks, parallel: parallel, verbose: verbose))
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

      def processed_values(blocks, parallel:, verbose:)
        unless parallel_enabled?(parallel)
          dump_parallel_status("disabled", verbose: verbose)
          return apply_pipeline(blocks, each_input_enum)
        end

        # Parallelize the longest map-only prefix; reducers stay in the parent.
        split_index = classify_parallel_stages(blocks)
        if split_index.nil? || split_index == 0
          dump_parallel_status("disabled", verbose: verbose)
          return apply_pipeline(blocks, each_input_enum)
        end

        map_blocks = blocks[0...split_index]
        reduce_blocks = blocks[split_index..] || []
        dump_parallel_status("enabled workers=#{parallel} files=#{@file_paths.length} split=#{split_index}/#{blocks.length}", verbose: verbose)
        input_enum = parallel_map_enum(map_blocks, parallel)
        reduce_blocks.empty? ? input_enum : apply_pipeline(reduce_blocks, input_enum)
      end

      def parallel_enabled?(parallel)
        parallel && parallel > 1 && @file_paths.length > 1
      end

      def dump_parallel_status(status, verbose:)
        @err.puts "parallel: #{status}" if verbose
      end

      def classify_parallel_stages(blocks)
        # Read the first row from the first file to probe stage modes
        first_value = read_parallel_probe_value(@file_paths.first)
        return nil if first_value.nil?

        # Run the value through each stage independently to classify
        split_index = nil
        blocks.each_with_index do |block, i|
          probe_pipeline = Pipeline.new(block)
          probe_pipeline.call([first_value]) { |_| }
          stage = probe_pipeline.instance_variable_get(:@stages).first
          if stage.instance_variable_get(:@mode) == :reducer
            split_index = i
            break
          end
        end

        split_index || blocks.length
      end

      def read_parallel_probe_value(path)
        open_file(path) do |source|
          first_stream_value(source)
        end
      end

      def open_file(path)
        if path.end_with?(".gz")
          Zlib::GzipReader.open(path) { |source| yield source }
        else
          File.open(path, "rb") { |source| yield source }
        end
      end

      def spawn_parallel_worker(blocks, path)
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
            pipeline.call(input_enum) { |value| emit_parallel_frame(value) }
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

      def run_parallel_worker_pool(blocks, num_workers)
        file_queue = @file_paths.dup
        workers = {} # read_io => [reader, pid]
        children = []

        # Fill initial pool
        while workers.size < num_workers && !file_queue.empty?
          read_io, pid = spawn_parallel_worker(blocks, file_queue.shift)
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
              reader.finish!
              read_ios.delete(io)
              io.close
              workers.delete(io)

              # Spawn next worker if files remain
              unless file_queue.empty?
                read_io, pid = spawn_parallel_worker(blocks, file_queue.shift)
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
        Enumerator.new do |y|
          children = run_parallel_worker_pool(map_blocks, num_workers) { |value| y << value }
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

      def emit_values(input_enum)
        if @output_format == :tsv
          values = []
          input_enum.each { |value| values << value }
          emit_tsv(values)
        else
          input_enum.each { |value| emit_output(value) }
        end
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

        stream.each_line do |raw_line|
          line = raw_line.strip
          next if line.empty?
          yield JSON.parse(line)
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

      def first_stream_value(stream)
        result = nil
        each_stream_value(stream) do |value|
          result = value
          break
        end
        result
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
