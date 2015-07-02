#!ruby --
require 'getoptlong'
require 'csv'
require 'net/http'
require 'json'

# globals
# default: use all columns of CSV.  Otherwise, a comma separated list of columns to use
@cols = nil

# Elasticsearch host and port
@host = 'localhost'
@port = 9200

# Elasticsearch index to write to
@index = nil

# Elasticsearch type of documents created
@type = nil

# CSV file to load
@file = nil

# reads ARGV, writes globals
def parse_args()
  opts = GetoptLong.new(
    ['--host', GetoptLong::OPTIONAL_ARGUMENT],
    ['--port', GetoptLong::OPTIONAL_ARGUMENT],
    ['--columns', GetoptLong::OPTIONAL_ARGUMENT],
    ['--index', GetoptLong::REQUIRED_ARGUMENT],
    ['--type', GetoptLong::REQUIRED_ARGUMENT]
  )

  opts.each do |opt, arg|
    case opt
    when '--host'
      @host = arg
    when '--port'
      @port = arg
    when '--columnns'
      @cols = arg.split(",")
    when '--index'
      @index = arg
    when '--type'
      @type = arg
    end
  end

  if @index.nil?
    STDERR.puts 'missing argument: --index'
    exit 1
  end

  if @type.nil?
    STDERR.puts 'missing argument: --type'
    exit 1
  end

  if ARGV.length != 1
    STDERR.puts 'Missing argument: file'
    exit 1
  end

  @file = ARGV.shift
end

def import_file(file)
  path = "/#{@index}/#{@type}/"
  uri = URI("http://#{@host}:#{@port}#{@path}")

  first = true
  lines  = 0
  column_map = []
  created=0
  fails=0

  Net::HTTP.start(uri.hostname, uri.port) do |http|

    CSV.foreach(file) do |row|
      if first
        column_map = row.map{|col| includeColumn?(col) ? {field: columnName_to_fieldname(col)} : nil}
        first = false
      else
        form = {}
        row.each_with_index do |v,i|
          if column_map[i]
            form[column_map[i][:field]] = v
          end
        end

        res = http.post(path, JSON.generate(form))
        if res.code == "201"
          created+=1
        else
          fails+=1
          puts res.code
          puts res.body
        end
                              
        lines+=1
      end
    end
  end

  puts "#{lines} lines read. #{created} documents created.  #{fails} failures"
end

# true if this csv column should be imported
def includeColumn? (col)
  return @cols.nil? ? true : @cols.member?(col)
end

# map from csv column name to elasticsearch field name.
# space mapped to dash
# fields names are downcased, but that might be a unnecessary
def columnName_to_fieldname (name)
  return name.downcase.gsub(' ','-')
end

def main
  parse_args()
  import_file(@file)
end

main if __FILE__ == $0
