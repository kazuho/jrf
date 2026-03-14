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

      def run(expression, verbose: false)
        parsed = PipelineParser.new(expression).parse
        stages = parsed[:stages]
        dump_stages(stages) if verbose

        blocks = stages.map { |stage|
          eval("proc { #{stage[:src]} }", nil, "(jrf stage)", 1) # rubocop:disable Security/Eval
        }
        pipeline = Pipeline.new(*blocks)

        input_enum = Enumerator.new { |y| each_input_value { |v| y << v } }

        if @output_format == :tsv
          values = []
          pipeline.call(input_enum) { |value| values << value }
          emit_tsv(values)
        else
          pipeline.call(input_enum) { |value| emit_output(value) }
        end
      ensure
        write_output(@output_buffer)
      end

      def run_parallel(expression, file_paths, num_workers, verbose: false)
        parsed = PipelineParser.new(expression).parse
        stages = parsed[:stages]
        dump_stages(stages) if verbose

        blocks = stages.map { |stage|
          eval("proc { #{stage[:src]} }", nil, "(jrf stage)", 1) # rubocop:disable Security/Eval
        }

        # Classify stages by feeding the first row from the first file
        split_index = classify_stages(blocks, file_paths)

        if split_index.nil? || split_index == 0
          # No map stages or all stages are reducers — run single-threaded
          pipeline = Pipeline.new(*blocks)
          input_enum = Enumerator.new { |y| each_input_value { |v| y << v } }
          run_pipeline(pipeline, input_enum)
          return
        end

        if split_index >= blocks.length
          # All stages are map stages — workers write directly to output
          run_parallel_map_only(blocks, file_paths, num_workers)
        else
          # Split: workers run map stages, parent runs reducer stages
          map_blocks = blocks[0...split_index]
          reduce_blocks = blocks[split_index..]
          run_parallel_map_reduce(map_blocks, reduce_blocks, file_paths, num_workers)
        end
      ensure
        write_output(@output_buffer)
      end

      private

      def run_pipeline(pipeline, input_enum)
        if @output_format == :tsv
          values = []
          pipeline.call(input_enum) { |value| values << value }
          emit_tsv(values)
        else
          pipeline.call(input_enum) { |value| emit_output(value) }
        end
      end

      def classify_stages(blocks, file_paths)
        # Read the first row from the first file to probe stage modes
        first_value = read_first_value(file_paths.first)
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

      def read_first_value(path)
        open_file(path) do |source|
          if @lax
            require "oj"
            result = nil
            handler = Class.new(Oj::ScHandler) do
              define_method(:initialize) { |&emit| @emit = emit }
              def hash_start = {}
              def hash_key(key) = key
              def hash_set(hash, key, value) = hash[key] = value
              def array_start = []
              def array_append(array, value) = array << value
              def add_value(value) = @emit.call(value)
            end
            begin
              Oj.sc_parse(handler.new { |v| result = v; raise StopIteration }, RsNormalizer.new(source))
            rescue StopIteration
              # got our first value
            end
            result
          else
            source.each_line do |raw_line|
              line = raw_line.strip
              next if line.empty?
              return JSON.parse(line)
            end
            nil
          end
        end
      end

      def open_file(path)
        if path.end_with?(".gz")
          Zlib::GzipReader.open(path) { |source| yield source }
        else
          File.open(path, "rb") { |source| yield source }
        end
      end

      def spawn_worker(blocks, path)
        read_io, write_io = IO.pipe
        pid = fork do
          read_io.close
          @out = write_io
          @output_buffer = +""
          @output_format = :json
          pipeline = Pipeline.new(*blocks)
          input_enum = Enumerator.new do |y|
            open_file(path) { |source| each_source_values(source) { |v| y << v } }
          end
          worker_failed = false
          begin
            pipeline.call(input_enum) { |value| emit_output(value) }
          rescue => e
            @err.puts "#{path}: #{e.message} (#{e.class})"
            worker_failed = true
          end
          write_output(@output_buffer)
          write_io.close
          exit!(worker_failed ? 1 : 0)
        end
        write_io.close
        [read_io, +(+""), pid]
      end

      def run_worker_pool(blocks, file_paths, num_workers)
        file_queue = file_paths.dup
        workers = {} # read_io => [buf, pid]
        children = []

        # Fill initial pool
        while workers.size < num_workers && !file_queue.empty?
          read_io, buf, pid = spawn_worker(blocks, file_queue.shift)
          workers[read_io] = [buf, pid]
          children << pid
        end

        read_ios = workers.keys.dup

        until read_ios.empty?
          ready = IO.select(read_ios)
          ready[0].each do |io|
            buf = workers[io][0]
            chunk = io.read_nonblock(65536, exception: false)
            if chunk == :wait_readable
              next
            elsif chunk.nil?
              # EOF — process any trailing data
              unless buf.empty?
                buf.each_line { |line| yield line.strip unless line.strip.empty? }
              end
              read_ios.delete(io)
              io.close
              workers.delete(io)

              # Spawn next worker if files remain
              unless file_queue.empty?
                read_io, new_buf, pid = spawn_worker(blocks, file_queue.shift)
                workers[read_io] = [new_buf, pid]
                children << pid
                read_ios << read_io
              end
            else
              buf << chunk
              # yield complete lines, keep trailing partial line in buffer
              while (nl = buf.index("\n"))
                line = buf.slice!(0, nl + 1).strip
                yield line unless line.empty?
              end
            end
          end
        end

        children
      end

      def run_parallel_map_only(blocks, file_paths, num_workers)
        values = []
        children = run_worker_pool(blocks, file_paths, num_workers) { |line| values << JSON.parse(line) }
        if @output_format == :tsv
          emit_tsv(values)
        else
          values.each { |value| emit_output(value) }
        end
        wait_for_children(children)
      end

      def run_parallel_map_reduce(map_blocks, reduce_blocks, file_paths, num_workers)
        reduce_pipeline = Pipeline.new(*reduce_blocks)
        children = nil
        input_enum = Enumerator.new do |y|
          children = run_worker_pool(map_blocks, file_paths, num_workers) { |line| y << JSON.parse(line) }
        end
        run_pipeline(reduce_pipeline, input_enum)

        wait_for_children(children)
      end

      def each_source_values(source)
        if @lax
          require "oj"
          handler = Class.new(Oj::ScHandler) do
            define_method(:initialize) { |&emit| @emit = emit }
            def hash_start = {}
            def hash_key(key) = key
            def hash_set(hash, key, value) = hash[key] = value
            def array_start = []
            def array_append(array, value) = array << value
            def add_value(value) = @emit.call(value)
          end
          Oj.sc_parse(handler.new { |value| yield value }, RsNormalizer.new(source))
        else
          source.each_line do |raw_line|
            line = raw_line.strip
            next if line.empty?
            yield JSON.parse(line)
          end
        end
      end

      def wait_for_children(children)
        failed = false
        children.each do |pid|
          _, status = Process.waitpid2(pid)
          failed = true unless status.success?
        end
        exit(1) if failed
      end

      def each_input_value
        return each_input_value_lax { |value| yield value } if @lax

        each_input_value_ndjson { |value| yield value }
      end

      def each_input_value_ndjson
        each_input do |source|
          source.each_line do |raw_line|
            line = raw_line.strip
            next if line.empty?

            yield JSON.parse(line)
          end
        end
      end

      def each_input_value_lax
        require "oj"
        handler = Class.new(Oj::ScHandler) do
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
        each_input do |source|
          Oj.sc_parse(handler.new { |value| yield value }, RsNormalizer.new(source))
        end
      rescue LoadError
        raise "oj is required for --lax mode (gem install oj)"
      rescue Oj::ParseError => e
        raise JSON::ParserError, e.message
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
