module BulkInsert
  require 'rubygems'
  require 'active_record'
  require 'active_support/all'
  require 'highline'

  def self.included(klass)
    klass.class_eval do
      attr_reader :threshold, :table_name, :columns, :confirm_flag, :insert_sql, :h

      def prepare(options={})
        @h = create_highline

        set_threshold options.delete(:threshold) || 100
        set_table_name options.delete(:table_name)
        set_columns options.delete(:columns)

        if @threshold.blank? || @table_name.blank? || @columns.blank?
          puts h.color("set threshold, table_name and columns", :red)
          exit
        end

        set_connection(options)
      end

      %W(threshold table_name columns).each do |name|
        define_method("set_#{name}") do |value|
          instance_variable_set("@#{name}", value)
        end
      end

      def parse(filepath, separater=',')
        if block_given?
          File.open(filepath){|f|
            f.each_line do |line|
              line.chomp!
              yield line.split(/#{separater}/)
            end
          }
        else
          raise "no block"
        end

        finalize
      end

      def format(array, options={})
        options = { :dry_run => false }.merge(options)
        @confirm_flag = options.delete(:dry_run)

        @insert_sql = "INSERT INTO #{table_name} (#{columns.map{|c| c.gsub(/_\w$/, '')}.join(', ')}) VALUES" unless @insert_sql
        @conditions = [] unless @conditions

        format_array = array.map{|col|
          case columns[array.index(col)]
          when /_s$/
            col = '"' + col + '"'
          when /_i$/
            col = col.to_i
          end
        }

        @conditions << %Q!(#{format_array.join(', ')})!

        if @conditions.size >= threshold
          if confirm_flag
            puts "#{@insert_sql} #{@conditions.join(', ')}"
          else
            puts "Insert #{@conditions.size} rows..."
            ActiveRecord::Base.connection.execute("#{@insert_sql} #{@conditions.join(', ')}")
          end
          @conditions = []
        end
      end

      private

      def finalize
        if @conditions.size > 0
          if confirm_flag
            puts "#{@insert_sql} #{@conditions.join(', ')}"
          else
            puts "Insert #{@conditions.size} rows..."
            ActiveRecord::Base.connection.execute("#{@insert_sql} #{@conditions.join(', ')}")
          end
          @conditions = []
        end
      end

      def create_highline
        HighLine.track_eof = false
        HighLine.new
      end

      def set_connection(_options={})
        if File.exist?("#{ENV['HOME']}/.bulk_insert.yml")
          puts h.color("\nUsing from #{ENV['HOME']}/.bulk_insert.yml...\n", :yellow)

          options = YAML.load_file("#{ENV['HOME']}/.bulk_insert.yml")
        else 
          puts h.color("\nPlease tell me database connection info...\n", :yellow)

          database = h.ask('1. Database: ')
          username = h.ask('2. Username: ')
          password = h.ask('3. Password: ') {|q| q.echo = false} 
          adapter  = h.ask('4. Adapter: ') {|q| q.default = 'mysql'}
          encoding = h.ask('5. Encoding: ') {|q| q.default = 'utf8'}
          socket   = h.ask('6. Socket: ') {|q| q.default = '/opt/local/var/run/mysql5/mysqld.sock'}

          options = {
            'adapter'  => adapter,
            'encoding' => encoding,
            'username' => username,
            'password' => password.blank? ? nil : password,
            'database' => database,
            'socket'   => socket
          }

          puts h.color("\nCreating config file to #{ENV['HOME']}/.bulk_insert.yml...\n", :yellow)

          File.open("#{ENV['HOME']}/.bulk_insert.yml", 'w'){|f|
            f.puts options.to_yaml
          }
        end

        options = options.merge(_options)
        ActiveRecord::Base.establish_connection(options)
      end

    end
  end
end
