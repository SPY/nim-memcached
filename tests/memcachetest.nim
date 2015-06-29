import unittest
from os import sleep

import "../memcache" as memcached

const expireTest = false
const touchTest = false
const hello = "hello"
const world = "world"
var memcache = newMemcache()

suite "Sync memcache client testcase":
  test "It should connect to local memcached":
    memcache.connect(host = "127.0.0.1", port = 11211.Port)

  test "Remove old test data if it exists":
    memcache.delete(hello)

  test "Add test data to memcache":
    memcache.add(hello, world)

  test "It will fail if data is added second time":
    expect(KeyAlreadyExistsError):
      memcache.add(hello, world)

  test "It should return test data":
    check(memcache.get(hello) == world)

  test "Getter way works too":
    check(memcache[hello] == world)

  test "Setter should change existed value":
    memcache[hello] = "another"
    check(memcache[hello] == "another")

  test "Contains method return true for test value":
    require memcache.contains(hello)

  test "Remove existed test data":
    memcache.delete(hello)

  test "Test data not in memcache":
    require(hello notin memcache)

  test "Get fail on fetch non-existed value":
    expect(KeyNotFoundError):
      discard memcache.get(hello)

  when expireTest:
    test "Add test value with expiration":
      memcache.add(hello, world, 2.Sec)
      memcache["zippy"] = ("second", 2.Sec)

    test "Check values in memcache till":
      sleep(1000)
      require hello in memcache
      require memcache.contains("zippy")

    test "Keys expired":
      sleep(1500)
      check hello notin memcache
      check(not memcache.contains("zippy"))

  when touchTest:
    test "Touch non-existed value returns false":
      check(not memcache.touch(hello))

    test "Touch existed value return true":
      memcache[hello] = (world, 2.Sec)
      sleep(1000)
      require memcache.touch(hello, 3.Sec)

    test "Test value in memcache after first time expiration, but before updated expiration passed":
      sleep(1500)
      require hello in memcache

proc main() =
  echo "Tests"