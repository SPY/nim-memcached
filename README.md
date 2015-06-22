Simple native memcached cleint for Nim programming language

Examples:
```Nimrod
import memcache

var memcache = newMemcache()
memcache.connect(host = "127.0.0.1", port = 11211.Port)
memcache.add("hello", "world")
echo memcache.get("hello") # => "world"
echo memcache.contains("hello") # => true
memcache.touch("hello")
memcache.delete("hello")

# with syntax sugar
memcache["hello"] = ("world", 5.Sec) # memcache.set("hello", "world", 5.Sec)
assert "hello" in memcache # memcache.contains("hello")
assert "not exists" notin memcache
assert memcache["hello"] == "world"
```