import net, tables
from sockets import htonl
import netdef, memcacheproto

export net.Port
export memcacheproto.Sec

type MemcacheConnectionError* = object of IOError
type AlreadyConnectedError* = object of IOError
type ConnectionClosedError* = object of IOError
type NotConnectedError* = object of IOError
type KeyNotFoundError* = object of IOError
type KeyAlreadyExistsError* = object of IOError

type ConnectionStatus = enum
  stNew, stConnected

type MemcacheClient* = object
  socket: Socket
  status: ConnectionStatus

proc newMemcache*(): MemcacheClient =
  MemcacheClient(socket: newSocket(), status: stNew)

proc connect*(client: var MemcacheClient, host: string = "127.0.0.1", port: Port = 11211.Port)
  {. raises: [MemcacheConnectionError, AlreadyConnectedError] .} =
  ## Connect to memcache server
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

proc sendCommand(
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

proc executeCommand(
    client: MemcacheClient,
    opcode: CommandOpcode,
    extras: RawData = empty,
    key: RawData = empty,
    value: RawData = empty
  ): Response =
  client.sendCommand(opcode, extras, key, value)
  client.waitForResponse()

proc version*(client: MemcacheClient): string =
  ## returns memcache server version
  client.executeCommand(CommandOpcode.Version).value

proc stats*(client: MemcacheClient): Table[string, string] =
  ## returns default memcache server statistic data
  result = initTable[string, string]()
  var response = client.executeCommand(CommandOpcode.Stat)
  while response.header.totalBodyLength.int > 0:
    result.add(response.key, response.value)
    response = client.waitForResponse()

proc stats*(client: MemcacheClient, key: string): Table[string, string] =
  ## request statistic by special key
  result = initTable[string, string]()
  var response = client.executeCommand(CommandOpcode.Stat, key = key.toRawData())
  if response.header.status != ResponseStatus.NoError:
    raise newException(KeyNotFoundError, "Stats for key " & key & " wasn't found")
  while response.header.totalBodyLength.int > 0:
    result.add(response.key, response.value)
    response = client.waitForResponse()

proc add*(client: MemcacheClient, key: string, value: string, expiration: Sec = Sec(0)) {. discardable .} =
  ## put value to memcache
  var extras = newAddExtras(expiration = expiration.uint32())
  let response = client.executeCommand(CommandOpcode.Add, extras.toRawData(), key.toRawData(), value.toRawData())
  if response.header.status == ResponseStatus.KeyExists:
    raise newException(KeyAlreadyExistsError, "Key has already exist")

proc get*(client: MemcacheClient, key: string): string
  {. raises: [KeyNotFoundError, NotConnectedError, ConnectionClosedError, TimeoutError, OSError] .} =
  ## request memcache for key
  ## if key doesn't exists or has been expiered
  ## KeyNotFoundError would be raised
  let response = client.executeCommand(CommandOpcode.Get, key = key.toRawData())
  if response.header.status == ResponseStatus.KeyNotFound:
    raise newException(KeyNotFoundError, "Key " & key & " is not found")
  response.value

proc `[]`*(client: MemcacheClient, key: string): string =
  ## alias for `get` procedure
  client.get(key)

proc set*(client: MemcacheClient, key: string, value: string, expiration: Sec = Sec(0)): bool {. discardable .} =
  ## set key value
  ## if item doesn't exist, it will be created
  ## `expiration` will be replaced by parameter, even if it was ommited(then expiration == 0 or never expiered)
  var extras = newAddExtras(expiration = expiration.uint32())
  let response = client.executeCommand(CommandOpcode.Set, extras.toRawData(), key.toRawData(), value.toRawData())
  response.header.status == ResponseStatus.NoError

proc `[]=`*(client: MemcacheClient, key: string, value: string): void =
  ## alias for `set` procedure with defalut expiration equals to 0
  client.set(key, value)

proc `[]=`*(client: MemcacheClient, key: string, value: tuple[value: string, expiration: Sec]): void =
  ## alias for `set` procedure
  client.set(key, value.value, value.expiration)

proc contains*(client: MemcacheClient, key: string): bool
  {. raises: [ConnectionClosedError, NotConnectedError, TimeoutError, OSError] .} =
  ## checks if key exists in memcache
  try:
    discard client.get(key)
    return true
  except KeyNotFoundError:
    return false

proc delete*(client: MemcacheClient, key: string): void =
  ## remove key from memcache
  discard client.executeCommand(CommandOpcode.Delete, key = key.toRawData())

proc touch*(client: MemcacheClient, key: string, expiration: Sec = Sec(0)): bool {. discardable .} =
  ## change key expiration
  ## if key doesn't exists, false will be returned
  var exp = expiration.int32.htonl()
  var extras = RawData(data: addr exp, size: sizeof(expiration))
  let response = client.executeCommand(CommandOpcode.Touch, extras = extras, key = key.toRawData())
  response.header.status == ResponseStatus.NoError
