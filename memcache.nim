import net, tables
from sockets import ntohl, ntohs, htons, htonl

type MemcacheConnectionError* = object of IOError
type AlreadyConnectedError* = object of IOError
type ConnectionClosedError* = object of IOError
type KeyNotFoundError* = object of IOError

type Connection = object
  host: string
  port: Port

type ConnectionStatus = enum
  stNew, stConnected

type MemcacheClient = object
  socket: Socket
  status: ConnectionStatus

type RequestHeader = object {. pure .}
  magic: uint8
  opcode: uint8
  keyLength: uint16
  extrasLength: uint8
  dataType: uint8
  vbucket: uint16
  totalBodyLength: uint32
  opaque: uint32
  cas: uint64

type ResponseHeader = object {. pure .}
  magic: uint8
  opcode: uint8
  keyLength: uint16
  extrasLength: uint8
  dataType: uint8
  status: uint16
  totalBodyLength: uint32
  opaque: uint32
  cas: uint64

const ReqMagic:uint8 = 0x80
const ResMagic:uint8 = 0x81

type ResponseStatus = enum
  NoError                = 0x0000
  KeyNotFound            = 0x0001
  KeyExists              = 0x0002
  ValueTooLarge          = 0x0003
  InvalidArguments       = 0x0004
  ItemNotStored          = 0x0005
  IncDecNonNimericValue  = 0x0006
  VBucketOnAnotherServer = 0x0007
  AuthenticationError    = 0x0008
  AuthenticationContinue = 0x0009
  UknownCommand          = 0x0081
  OutOfMemory            = 0x0082
  NotSupported           = 0x0083
  InternalError          = 0x0084
  Busy                   = 0x0085
  TemporaryFailure       = 0x0086

type CommandOpcode = enum
  Get       = 0x00
  Set       = 0x01
  Add       = 0x02
  Replace   = 0x03
  Delete    = 0x04
  Increment = 0x05
  Decrement = 0x06
  Quit      = 0x07
  Flush     = 0x08
  GetQ      = 0x09
  Noop      = 0x0a
  Version   = 0x0b
  Stat      = 0x10

type DataType = enum
  Raw = 0x00

type Response = object
  header: ResponseHeader
  extras: string
  key: string
  value: string

proc newMemcache*(): MemcacheClient =
  MemcacheClient(socket: newSocket(), status: stNew)

proc connect*(client: var MemcacheClient, connection: Connection) {. raises: [MemcacheConnectionError, AlreadyConnectedError] .} =
  if client.status == stConnected:
    raise newException(AlreadyConnectedError, "Memcache client is connected")
  try:
    client.socket.connect(connection.host, connection.port)
    client.status = stConnected
  except OSError:
    raise newException(MemcacheConnectionError, "Couldn't connect to server")

proc waitForResponse(client: MemcacheClient): Response =
  var header = new(ResponseHeader)
  if client.socket.recv(cast[pointer](header), sizeof(ResponseHeader)) == 0:
    raise newException(ConnectionClosedError, "Connection to memcache was closed")

  # from network endian to local
  let bodySize: int = header.totalBodyLength.int32.ntohl()
  let keySize: int = header.keyLength.int16.ntohs()
  let extrasSize: int = header.extrasLength.int

  # update sizes in header
  header.totalBodyLength = bodySize.uint32
  header.keyLength = keySize.uint16
  header.status = header.status.int16.ntohs().uint16

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
      key = data[int(extrasSize)..int(extrasSize + keySize - 1)]
    if bodySize - extrasSize - keySize > 0:
      value = data[int(extrasSize + keySize) .. int(bodySize - 1)]
  Response(header: header[], extras: extras, key: key, value: value)

type RawData = object
  data: pointer
  size: int

const empty: RawData = RawData(data: nil, size: 0)

proc sendCommand(client: MemcacheClient, opcode: CommandOpcode, extras: RawData = empty, key: RawData = empty, value: RawData = empty): Response =
  var header = new(RequestHeader)
  header.magic = ReqMagic
  header.opcode = ord(opcode)
  header.extrasLength = extras.size.uint8
  header.keyLength = key.size.int16.htons().uint16
  header.totalBodyLength = int32(extras.size + key.size + value.size).htonl().uint32
  discard client.socket.send(cast[pointer](header), sizeof(RequestHeader))
  if extras.size > 0:
    discard client.socket.send(extras.data, extras.size)
  if key.size > 0:
    discard client.socket.send(key.data, key.size)
  if value.size > 0:
    discard client.socket.send(value.data, value.size)
  client.waitForResponse()

proc version*(client: MemcacheClient): string =
  client.sendCommand(Version).value

proc stats*(client: MemcacheClient): Table[string, string] =
  result = initTable[string, string]()
  var response = client.sendCommand(Stat)
  while response.header.totalBodyLength.int > 0:
    result.add(response.key, response.value)
    response = client.waitForResponse()

# in network format
const Flags = 0xefbeadde

type GetExtras = object {. pure .}
  flags, expiration: uint32

proc toRawData(extras: ptr GetExtras): RawData =
  RawData(data: cast[pointer](extras), size: sizeof(GetExtras))

proc toRawData(str: string): RawData =
  RawData(data: str.cstring, size: str.len())

type AddStatus* = enum
  Added, AlreadyExists

proc add*(client: MemcacheClient, key: string, value: string, expiration: uint32 = 0): AddStatus {. discardable .} =
  var extras = GetExtras(flags: Flags, expiration: expiration.int32.htonl().uint32)
  let response = client.sendCommand(Add, toRawData(addr extras), key.toRawData(), value.toRawData())
  if response.header.status == ord(NoError):
    return Added
  if response.header.status == ord(KeyExists):
    return AlreadyExists

proc get*(client: MemcacheClient, key: string): string {. raises: [KeyNotFoundError, ConnectionClosedError, TimeoutError, OSError] .} =
  let response = client.sendCommand(Get, key = key.toRawData())
  if response.header.status == ord(KeyNotFound):
    raise newException(KeyNotFoundError, "Key " & key & " is not found")
  response.value

proc exists*(client: MemcacheClient, key: string): bool {. raises: [ConnectionClosedError, TimeoutError, OSError] .} =
  try:
    discard client.get(key)
    return true
  except KeyNotFoundError:
    return false

when isMainModule:
  assert sizeof(GetExtras) == 8
  var memcache = newMemcache()
  memcache.connect(Connection(host: "127.0.0.1", port: Port(11211)))
  assert memcache.status == stConnected
  memcache.add("hello", "world")
  assert memcache.get("hello") == "world"
  assert memcache.exists("hello")
  var success = false
  try:
    discard memcache.get("something")
    success = true
  except KeyNotFoundError:
    success = false
  assert(not success)
