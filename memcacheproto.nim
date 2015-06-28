import netdef

# https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Magic_Byte
const ReqMagic*:uint8 = 0x80
const ResMagic*:uint8 = 0x81

type DataType* = enum
  Raw = 0x00

# https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Response_Status
type ResponseStatus* {. pure .} = enum
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

# https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Command_Opcodes
type CommandOpcode* {. pure .} = enum
  Get                    = 0x00
  Set                    = 0x01
  Add                    = 0x02
  Replace                = 0x03
  Delete                 = 0x04
  Increment              = 0x05
  Decrement              = 0x06
  Quit                   = 0x07
  Flush                  = 0x08
  GetQ                   = 0x09
  Noop                   = 0x0a
  Version                = 0x0b
  GetK                   = 0x0c
  GetKQ                  = 0x0d
  Append                 = 0x0e
  Prepend                = 0x0f
  Stat                   = 0x10
  SetQ                   = 0x11
  AddQ                   = 0x12
  ReplaceQ               = 0x13
  DeleteQ                = 0x14
  IncrementQ             = 0x15
  DecrementQ             = 0x16
  QuitQ                  = 0x17
  FlushQ                 = 0x18
  AppendQ                = 0x19
  PrependQ               = 0x1a
  Verbosity              = 0x1b
  Touch                  = 0x1c
  GAT                    = 0x1d
  GATQ                   = 0x1e
  SASLList               = 0x20
  SASLAuth               = 0x21
  SASLStep               = 0x22
  RGet                   = 0x30
  RSet                   = 0x31
  RSetQ                  = 0x32
  RAppend                = 0x33
  RAppendQ               = 0x34
  RPrepend               = 0x35
  RPrependQ              = 0x36
  RDelete                = 0x37
  RDeleteQ               = 0x38
  RIncr                  = 0x39
  RIncrQ                 = 0x3a
  RDecr                  = 0x3b
  RDecrQ                 = 0x3c
  SetVBucket             = 0x3d
  GetVBucket             = 0x3e
  DelVBucket             = 0x3f
  TAPConnect             = 0x40
  TAPMutation            = 0x41
  TAPDelete              = 0x42
  TAPFlush               = 0x43
  TAPOpaque              = 0x44
  TAPVBucketSet          = 0x45
  TAPCheckpointStart     = 0x46
  TAPCheckpointEnd       = 0x47

# https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Request_header
network struct pub RequestHeader:
  pub magic: uint8 = ReqMagic
  pub opcode: CommandOpcode(uint8)
  pub keyLength: uint16
  pub extrasLength: uint8
  pub dataType: DataType(uint8) = Raw
  pub vbucket: uint16
  pub totalBodyLength: uint32
  pub opaque: uint32
  pub cas: uint64

# https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Response_header
network struct pub ResponseHeader:
  pub magic: uint8 = ResMagic
  pub opcode: CommandOpcode(uint8)
  pub keyLength: uint16
  pub extrasLength: uint8
  pub dataType: DataType(uint8) = Raw
  pub status: ResponseStatus(uint16)
  pub totalBodyLength: uint32
  pub opaque: uint32
  pub cas: uint64

export ResponseHeader

type Sec* = distinct uint32

type Response* = object
  header*: ResponseHeader
  extras*: string
  key*: string
  value*: string

type RawData* = object
  data*: pointer
  size*: int

const empty*: RawData = RawData(data: nil, size: 0)

proc toRawData*(str: string): RawData =
  RawData(data: str.cstring, size: str.len())

network struct pub AddExtras:
  pub flags: uint32
  pub expiration: uint32

type AddStatus* = enum
  Added, AlreadyExists, AddError

proc toRawData*(extras: ref AddExtras): RawData =
  RawData(data: cast[pointer](extras), size: sizeof(AddExtras))