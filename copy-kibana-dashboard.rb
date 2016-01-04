#!ruby --
require 'getoptlong'
require 'net/http'
require 'json'

# Copy a Kibana 4 dashboard and its visualizations and searched from one cluster to another
# Does not check that either cluster actually has kibana version 4
# Does not copy the index being visualized.  You need to do that yourself.
# The MIT License (MIT)
#
# Copyright (c) 2015 Jim Davis (jrd3@alum.mit.edu)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

@verbose = true

# Kibana types
DASHBOARD="dashboard"
VISUALIZATION="visualization"
SEARCH="search"

def main
  id, old, new = parse_args()

  if (old[:host] == new[:host] &&
      old[:index] == new[:index] &&
      old[:port] == new[:port])
    STDERR.puts "The source and destination clusters are the same."
    usage()
    exit 1
  end

  dashboard=get_dashboard(old, id)
  put_single_object(new, DASHBOARD, id, dashboard["_source"])

  ids = JSON.parse(dashboard["_source"]["panelsJSON"]).map{|panel| panel["id"]}
  visualizations =  get_objects(old, VISUALIZATION, ids)
  put_objects(new, VISUALIZATION, map_objects(visualizations))

  saved_search_ids =  visualizations.map{|v| v["_source"]["savedSearchId"]}.reject{|x| x.nil?}
  saved_search = get_objects(old, SEARCH, saved_search_ids)
  if new[:saved_search_index]
    saved_search.each {|s| set_saved_search_index(s, new[:saved_search_index])}
  end
  put_objects(new, SEARCH, map_objects(saved_search))
end

def parse_args
  from_cluster = {host: "localhost", port: 9200, index: ".kibana"}
  to_cluster   = {host: "localhost", port: 9200, index: ".kibana"}
  dashboard_id = nil

  opts = GetoptLong.new(
    ['--verbose',         GetoptLong::NO_ARGUMENT],
    ['--quiet',           GetoptLong::NO_ARGUMENT],
    ['--dashboard', '-d', GetoptLong::REQUIRED_ARGUMENT],
    ['--from-host',       GetoptLong::REQUIRED_ARGUMENT],
    ['--from-port',       GetoptLong::REQUIRED_ARGUMENT],
    ['--from-index',      GetoptLong::REQUIRED_ARGUMENT],
    ['--to-host',         GetoptLong::REQUIRED_ARGUMENT],
    ['--to-port',         GetoptLong::REQUIRED_ARGUMENT],
    ['--to-index',        GetoptLong::REQUIRED_ARGUMENT],
    ['--to-saved-search-index', GetoptLong::REQUIRED_ARGUMENT],
    ['--help', '-h',      GetoptLong::NO_ARGUMENT]
  )

  opts.each do |opt, arg|
    case opt
    when '--help'
      usage()
      exit 0
    when '--verbose'
      @verbose = true
    when '--quiet'
      @verbose = false
    when '--dashboard'
      dashboard_id = arg
    when '--from-host'
      from_cluster[:host] = arg
    when '--from-port'
      from_cluster[:port] = Integer(arg)
    when '--from-index'
      from_cluster[:index] = arg
    when '--to-host'
      to_cluster[:host] = arg
    when '--to-port'
      to_cluster[:port] = Integer(arg)
    when '--to-index'
      to_cluster[:index] = arg
    when '--to-saved-search-index'
      to_cluster[:saved_search_index] = arg
    end
  end

  if dashboard_id.nil?
    STDERR.puts "Missing argument --dashboard"
    usage()
    exit 1
  end

  [dashboard_id, from_cluster, to_cluster]
end

def usage
  STDERR.puts <<-EOF
Usage:
--dashboard ID
--from-host HOST (default: localhost)
--from-port POST (default: 9200)
--from-index INDEX (default: .kibana)
--to-host HOST (default: localhost)
--to-port POST (default: 9200)
--to-index INDEX (default: .kibana)
--to-saved-search-index (default: don't change)
    change index for saved search
--verbose
    print object keys as they are copied
--quiet

At least one optional argument must be provided
EOF
end

def get_dashboard(cluster, id)
  Net::HTTP.start(cluster[:host], cluster[:port]) do |http|
	dashboard_encode = URI::encode(id)
    res = http.get("#{cluster[:index]}/dashboard/#{dashboard_encode}")
    if res.code == "200"
      JSON.parse(res.body)
    else
      STDERR.puts "Fail #{res.code} #{res.message}"
      exit 1
    end
  end
end

def get_objects (cluster, type, ids)
  q = JSON.generate({from: 0, size: 1000, query: {filtered: {filter: {ids: {values: ids}}}}})
  Net::HTTP.start(cluster[:host], cluster[:port]) do |http|
    res = http.post("#{cluster[:index]}/#{type}/_search", q)
    if res.code == "200"
      JSON.parse(res.body)["hits"]["hits"]
    else
      raise "Failed to get #{type} objects #{ids} #{res.code} #{res.message}"
    end
  end
end

def map_objects(objects)
  new_objects = {}
  objects.each do |doc|
    new_objects[doc["_id"]] = doc["_source"]
  end
  new_objects
end

def put_objects(cluster, type, objects)
  Net::HTTP.start(cluster[:host], cluster[:port]) do |http|
    objects.keys.each do |key|
      put_object(http, cluster[:index], type, key, objects[key])
    end
  end
end

def put_single_object(cluster, type, id, object)
  Net::HTTP.start(cluster[:host], cluster[:port]) do |http|
    put_object(http, cluster[:index], type, id, object)
  end
end

def put_object(http, index, type, id, object)
  if @verbose
    STDOUT.write "Writing #{type} #{id} "
  end
  res = http.put("#{index}/#{type}/#{id}", JSON.generate(object))
  case res.code
  when "200"
    if @verbose
      STDOUT.puts "updated"
    end
  when "201"
    if @verbose
      STDOUT.puts "created"
    end
  else
    STDOUT.puts res.class.name
    STDERR.puts "Result #{res.code} #{res.message}"
  end
end

def set_saved_search_index(saved_search, new_index)
  ssj = JSON.parse(saved_search["_source"]["kibanaSavedObjectMeta"]["searchSourceJSON"])
  ssj["index"] = new_index
  saved_search["_source"]["kibanaSavedObjectMeta"]["searchSourceJSON"] = JSON.generate(ssj)
end

main if __FILE__ == $0
