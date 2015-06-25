import net, tables
from os import sleep
import netdef, memcacheproto

type MemcacheConnectionError* = object of IOError
type AlreadyConnectedError* = object of IOError
type ConnectionClosedError* = object of IOError
type NotConnectedError* = object of IOError
type KeyNotFoundError* = object of IOError

type ConnectionStatus = enum
  stNew, stConnected

type MemcacheClient* = object
  socket: Socket
  status: ConnectionStatus

proc newMemcache*(): MemcacheClient =
  MemcacheClient(socket: newSocket(), status: stNew)

proc connect*(client: var MemcacheClient, host: string = "127.0.0.1", port: Port = 11211.Port)
  {. raises: [MemcacheConnectionError, AlreadyConnectedError] .} =
  if client.status == stConnected:
    raise newException(AlreadyConnectedError, "Memcache client is connected")
  try:
    client.socket.connect(host, port)
    client.status = stConnected
  except OSError:
    raise newException(MemcacheConnectionError, "Couldn't connect to server")

proc waitForResponse(client: MemcacheClient): Response =
  var header = ResponseHeader()
  if client.socket.recv(cast[pointer](addr header), sizeof(ResponseHeader)) == 0:
    raise newException(ConnectionClosedError, "Connection to memcache was closed")

  # cast sizes to int for making compiler happy
  let bodySize: int = header.totalBodyLength.int
  let keySize: int = header.keyLength.int
  let extrasSize: int = header.extrasLength.int

  var extras: string = nil
  var key: string = nil
  var value: string = nil
  if bodySize > 0:
    var data = newString(bodySize)
    if client.socket.recv(data, bodySize) == 0:
      raise newException(ConnectionClosedError, "Connection to memcache was closed")
    if extrasSize > 0:
      extras = data[0..extrasSize-1]
    if keySize > 0:
      key = data[extrasSize .. extrasSize + keySize - 1]
    if bodySize - extrasSize - keySize > 0:
      value = data[extrasSize + keySize .. bodySize - 1]
  Response(header: header, extras: extras, key: key, value: value)


proc send(socket: Socket, data: RawData) =
  discard socket.send(data.data, data.size)

proc justSendCommand(
    client: MemcacheClient,
    opcode: CommandOpcode,
    extras: RawData = empty,
    key: RawData = empty,
    value: RawData = empty
  ): void =
  if client.status != stConnected:
    raise newException(NotConnectedError, "Memcache is not connected. Call connect function before")
  var header = newRequestHeader(
    opcode = opcode,
    extrasLength = extras.size.uint8(),
    keyLength = key.size.uint16(),
    totalBodyLength = uint32(extras.size + key.size + value.size)
  )
  discard client.socket.send(cast[pointer](header), sizeof(RequestHeader))
  if extras.size > 0:
    client.socket.send(extras)
  if key.size > 0:
    client.socket.send(key)
  if value.size > 0:
    client.socket.send(value)

proc sendCommand(
    client: MemcacheClient,
    opcode: CommandOpcode,
    extras: RawData = empty,
    key: RawData = empty,
    value: RawData = empty
  ): Response =
  client.justSendCommand(opcode, extras, key, value)
  client.waitForResponse()

proc version*(client: MemcacheClient): string =
  client.sendCommand(CommandOpcode.Version).value

proc stats*(client: MemcacheClient): Table[string, string] =
  result = initTable[string, string]()
  var response = client.sendCommand(CommandOpcode.Stat)
  while response.header.totalBodyLength.int > 0:
    result.add(response.key, response.value)
    response = client.waitForResponse()

proc stats*(client: MemcacheClient, key: string): Table[string, string] =
  result = initTable[string, string]()
  var response = client.sendCommand(CommandOpcode.Stat, key = key.toRawData())
  if response.header.status != ResponseStatus.NoError:
    raise newException(KeyNotFoundError, "Stats for key " & key & " wasn't found")
  while response.header.totalBodyLength.int > 0:
    result.add(response.key, response.value)
    response = client.waitForResponse()

proc add*(client: MemcacheClient, key: string, value: string, expiration: Sec = Sec(0)): AddStatus {. discardable .} =
  var extras = newAddExtras(expiration = expiration.uint32())
  let response = client.sendCommand(CommandOpcode.Add, extras.toRawData(), key.toRawData(), value.toRawData())
  case response.header.status:
    of ResponseStatus.NoError: Added
    of ResponseStatus.KeyExists: AlreadyExists
    else: AddError

proc get*(client: MemcacheClient, key: string): string 
  {. raises: [KeyNotFoundError, NotConnectedError, ConnectionClosedError, TimeoutError, OSError] .} =
  let response = client.sendCommand(CommandOpcode.Get, key = key.toRawData())
  if response.header.status == ResponseStatus.KeyNotFound:
    raise newException(KeyNotFoundError, "Key " & key & " is not found")
  response.value

proc `[]`*(client: MemcacheClient, key: string): string =
  client.get(key)

proc set*(client: MemcacheClient, key: string, value: string, expiration: Sec = Sec(0)): bool {. discardable .} =
  var extras = newAddExtras(expiration = expiration.uint32())
  let response = client.sendCommand(CommandOpcode.Set, extras.toRawData(), key.toRawData(), value.toRawData())
  response.header.status == ResponseStatus.NoError

proc `[]=`*(client: MemcacheClient, key: string, value: string): void =
  client.set(key, value)

proc `[]=`*(client: MemcacheClient, key: string, value: tuple[value: string, expiration: Sec]): void =
  client.set(key, value.value, value.expiration)

proc contains*(client: MemcacheClient, key: string): bool
  {. raises: [ConnectionClosedError, NotConnectedError, TimeoutError, OSError] .} =
  try:
    discard client.get(key)
    return true
  except KeyNotFoundError:
    return false

proc delete*(client: MemcacheClient, key: string): void =
  discard client.sendCommand(CommandOpcode.Delete, key = key.toRawData())

proc touch*(client: MemcacheClient, key: string, expiration: Sec = Sec(0)): bool {. discardable .} =
  var exp = expiration.int32.htonl()
  var extras = RawData(data: addr exp, size: sizeof(expiration))
  let response = client.sendCommand(CommandOpcode.Touch, extras = extras, key = key.toRawData())
  response.header.status == ResponseStatus.NoError

when isMainModule:
  const expireTest = false
  const touchTest = false
  const hello = "hello"
  const world = "world"
  var memcache = newMemcache()
  memcache.connect(host = "127.0.0.1", port = 11211.Port)
  assert memcache.status == stConnected, "Status should be changed"
  memcache.delete(hello)
  assert memcache.add(hello, world) == Added, "Key shouldn't exist before"
  assert memcache.get(hello) == world, "Get should return value"
  assert memcache[hello] == world, "Getter should work like get"
  memcache[hello] = "another"
  assert memcache[hello] == "another", "Setter should change value"
  assert memcache.contains(hello), "Key should be in memcache"
  memcache.delete(hello)
  assert hello notin memcache, "Key shouldn't be in memcache after remove"
  try:
    discard memcache.get(hello)
    assert false, "Get of removed value should raise exception"
  except KeyNotFoundError:
    assert true
  when expireTest:
    memcache.add(hello, world, 2.Sec)
    memcache["zippy"] = ("second", 2.Sec)
    sleep(1000)
    assert hello in memcache, "Key still should be in memcache"
    assert memcache.contains("zippy"), "Key setted by tuple setter should set expiration too"
    sleep(1500)
    assert(hello notin memcache, "Should be expired")
    assert(not memcache.contains("zippy"), "Should be expired")
  when touchTest:
    assert(not memcache.touch(hello), "Touch non-existed key should return false")
    memcache[hello] = (world, 2.Sec)
    sleep(1000)
    assert memcache.touch(hello, 3.Sec), "Touch existed key should return true"
    sleep(1500)
    assert hello in memcache, "Key should be in memcach after touch"
