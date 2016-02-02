class MyObfuscate
  module InsertStatementParser

    def parse(obfuscator, config, input_io, output_io)
      buffer          = Array.new
      buffer_size     = 0
      max_buffer_size = Parallel.processor_count
      input_io.each do |line|
        buffer.push line
        buffer_size += 1
        if buffer_size > max_buffer_size
          do_parse_parallel(buffer, obfuscator, config, output_io)
          buffer_size = 0
          buffer.clear
        end
      end
      if buffer_size > 0
        do_parse_parallel(buffer, obfuscator, config, output_io)
      end
    end

    def do_parse_parallel(buffer, obfuscator, config, output_io)
      output_buffer = Array.new(buffer.length)
      Parallel.each_with_index(buffer, :in_threads => Parallel.processor_count) do |line, index|
        result               = do_parse(line, obfuscator, config)
        output_buffer[index] = result
        # STDOUT.write "INSIDE: index:\r\n\t#{index}\r\nlength\r\n\t#{output_buffer.length}\r\n"
        # STDOUT.write "INSIDE\r\n"
        output_buffer.each_with_index do |ob, i|
          # STDOUT.write "\tI#{i}\t#{ob}\r\n"
        end
      end
      # STDOUT.write "OUTSIDE:\r\n"

      # output_buffer.each_with_index do |ob, i|
      #   STDOUT.write "\tO#{i}:\t#{ob}\r\n"
      # end

      output_buffer.each do |output_line|
        # STDOUT.write output_line
        output_io.puts output_line
      end
    end

    def do_parse(line, obfuscator, config)
      table_data = parse_insert_statement(line)
      if table_data
        table_name = table_data[:table_name]
        columns    = table_data[:column_names]
        ignore     = table_data[:ignore]
        if config[table_name]
          result = obfuscator.obfuscate_bulk_insert_line(line, table_name, columns, ignore)
        else
          $stderr.puts "Deprecated: #{table_name} was not specified in the config.  A future release will cause this to be an error.  Please specify the table definition or set it to :keep."
          result = line
        end
      else
        result = line
      end
      result
    end
  end
end

