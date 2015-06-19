Simple native memcached cleint for Nim programming language

Examples:
import memcache

var memcache = newMemcache()
memcache.connect(Connection(host: "127.0.0.1", port: Port(11211)))
memcache.add("hello", "world")
echo memcache.get("hello") # => "world"
echo memcache.exists("hello") # => true
