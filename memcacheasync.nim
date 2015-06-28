import asyncnet, asyncdispatch, tables
from os import sleep
from net import Port
import netdef, memcacheproto

type MemcacheConnectionError* = object of IOError
type ConnectionClosedError* = object of IOError
type NotConnectedError* = object of IOError
type KeyNotFoundError* = object of IOError

type MemcacheAsyncClient* = object
  socket: AsyncSocket

proc newMemcache*(): MemcacheAsyncClient =
  MemcacheAsyncClient(socket: newAsyncSocket())

proc connect*(client: MemcacheAsyncClient, host: string = "127.0.0.1", port: Port = 11211.Port) {. async .} =
  try:
    await client.socket.connect(host, port)
  except:
    raise newException(MemcacheConnectionError, "Couldn't connect to server")

proc send(socket: AsyncSocket, data: RawData) {. async .} =
  var dataStr = newString(data.size)
  copyMem(addr dataStr[0], cast[pointer](data.data), data.size)
  asyncCheck socket.send(dataStr)

proc justSendCommand(
    client: MemcacheAsyncClient,
    opcode: CommandOpcode,
    extras: RawData = empty,
    key: RawData = empty,
    value: RawData = empty
  ) {. async .} =
  var header = newRequestHeader(
    opcode = opcode,
    extrasLength = extras.size.uint8(),
    keyLength = key.size.uint16(),
    totalBodyLength = uint32(extras.size + key.size + value.size)
  )
  var headerStr = newString(sizeof(RequestHeader))
  copyMem(addr(headerStr[0]), cast[pointer](header), sizeof(RequestHeader))
  await client.socket.send(headerStr)
  if extras.size > 0:
    await client.socket.send(extras)
  if key.size > 0:
    await client.socket.send(key)
  if value.size > 0:
    await client.socket.send(value)

proc waitForResponse(client: MemcacheAsyncClient): Future[Response] {. async .} =
  var headerStr = await client.socket.recv(sizeof(ResponseHeader))
  var header = ResponseHeader()
  copyMem(cast[pointer](addr(header)), addr(headerStr[0]), sizeof(RequestHeader))

  # cast sizes to int for making compiler happy
  let bodySize: int = header.totalBodyLength.int
  let keySize: int = header.keyLength.int
  let extrasSize: int = header.extrasLength.int

  var extras: string = nil
  var key: string = nil
  var value: string = nil
  if bodySize > 0:
    var data = await client.socket.recv(bodySize)
    if extrasSize > 0:
      extras = data[0..extrasSize-1]
    if keySize > 0:
      key = data[extrasSize .. extrasSize + keySize - 1]
    if bodySize - extrasSize - keySize > 0:
      value = data[extrasSize + keySize .. bodySize - 1]
  result = Response(header: header, extras: extras, key: key, value: value)

proc sendCommand(
    client: MemcacheAsyncClient,
    opcode: CommandOpcode,
    extras: RawData = empty,
    key: RawData = empty,
    value: RawData = empty
  ): Future[Response] {. async .} =
  await client.justSendCommand(opcode, extras, key, value)
  result = await client.waitForResponse()

proc version*(client: MemcacheAsyncClient): Future[string] {. async .} =
  let response = await client.sendCommand(CommandOpcode.Version)
  result = response.value

proc add*(client: MemcacheAsyncClient, key: string, value: string, expiration: Sec = Sec(0)): Future[AddStatus] {. async .} =
  var extras = newAddExtras(expiration = expiration.uint32())
  let response = await client.sendCommand(CommandOpcode.Add, extras.toRawData(), key.toRawData(), value.toRawData())
  case response.header.status:
    of ResponseStatus.NoError: result = Added
    of ResponseStatus.KeyExists: result = AlreadyExists
    else: result = AddError

proc get*(client: MemcacheAsyncClient, key: string): Future[string] =
  let res = newFuture[string]()
  client.sendCommand(CommandOpcode.Get, key = key.toRawData()).callback = proc(future: Future[Response]) {.closure, gcsafe.} =
    let response = future.read()
    if response.header.status == ResponseStatus.KeyNotFound:
      res.fail(newException(KeyNotFoundError, "Key " & key & " is not found"))
    else:
      res.complete(response.value)
  res

proc set*(client: MemcacheAsyncClient, key: string, value: string, expiration: Sec = Sec(0)): Future[bool] {. async .} =
  var extras = newAddExtras(expiration = expiration.uint32())
  let response = await client.sendCommand(CommandOpcode.Set, extras.toRawData(), key.toRawData(), value.toRawData())
  result = response.header.status == ResponseStatus.NoError

proc touch*(client: MemcacheAsyncClient, key: string, expiration: Sec = Sec(0)) {. async .} =
  var exp = expiration.int32.htonl()
  var extras = RawData(data: addr exp, size: sizeof(expiration))
  let _ = await client.sendCommand(CommandOpcode.Touch, extras = extras, key = key.toRawData())

proc delete*(client: MemcacheAsyncClient, key: string) {. async .} =
  let _ = await client.sendCommand(CommandOpcode.Delete, key = key.toRawData())

proc contains*(client: MemcacheAsyncClient, key: string): Future[bool] {. async .} =
  let response = await client.sendCommand(CommandOpcode.Get, key = key.toRawData())
  result = response.header.status == ResponseStatus.NoError

when isMainModule:
  proc test(): Future[void] {. async .} =
    const expireTest = false
    const touchTest = false
    const hello = "hello"
    const world = "world"
    var memcache = newMemcache()
    await memcache.connect(host = "127.0.0.1", port = 11211.Port)
    await memcache.delete(hello)
    assert(await(memcache.add(hello, world)) == Added, "Key shouldn't exist before")
    assert(await(memcache.get(hello)) == world, "Get should return correct value")
    assert(await memcache.set(hello, "another"))
    assert(await(memcache.get(hello)) == "another", "Get should return correct another value")
    await memcache.touch(hello, 10.Sec)
    try:
      let value = await memcache.get("unknown")
      assert false
    except KeyNotFoundError:
      assert true
    assert(await memcache.contains(hello))
    assert(not (await memcache.contains("unknown")))
    echo "All tests passed"
  waitFor test()