Simple native memcached cleint for Nim programming language

Examples:
```Nimrod
import memcache

var memcache = newMemcache()
memcache.connect(Connection(host: "127.0.0.1", port: Port(11211)))
memcache.add("hello", "world")
echo memcache.get("hello") # => "world"
echo memcache.exists("hello") # => true
memcache.touch("hello")
memcache.delete("hello")

# with syntaxic sugar
memcache["hello"] = ("world", 5.Sec) # memcache.set("hello", "world", 5.Sec)
assert "hello" in memcache # memcache.contains("hello")
assert "not exists" notin memcache
assert memcache["hello"] == "world"
```