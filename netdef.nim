import macros, strutils
from sockets import ntohl, ntohs, htons, htonl
import endians

export sockets.ntohl
export sockets.ntohs
export sockets.htonl
export sockets.htons

const bigInts = ["int16", "uint16", "int32", "uint32", "int", "uint", "int64", "uint64"]

proc isBigInt(strType: string): bool =
  return strType in bigInts

proc htonll*(val: int64): int64 =
  var inp = val
  bigEndian64(addr result, addr inp)

proc ntohll*(val: int64): int64 =
  var inp = val
  littleEndian64(addr result, addr inp)

proc getterTypes(strType: string): tuple[t, procName: string] =
  case strType:
  of "int16", "uint16": return ("int16", "ntohs")
  of "int32", "uint32": return ("int32", "ntohl")
  of "int64", "uint64": return ("int64", "ntohll")
  of "int", "uint":
    case sizeof(int):
    of 4: return ("int32", "ntohl")
    of 8: return ("int64", "ntohll")
    else: discard

proc setterTypes(strType: string): tuple[t, procName: string] =
  case strType:
  of "int16", "uint16": return ("int16", "htons")
  of "int32", "uint32": return ("int32", "htonl")
  of "int64", "uint64": return ("int64", "htonll")
  of "int", "uint":
    case sizeof(int):
    of 4: return ("int32", "htonl")
    of 8: return ("int64", "htonll")
    else: discard

macro network*(command, body: stmt): stmt {. immediate .} =
  let commandType = command[0]
  case $commandType:
    of "struct":
      let typeName = command[1]
      result = quote do:
        type `typeName` = object {. pure .}
      var recList = newNimNode(nnkRecList)
      for i in body.children:
        let fieldName = i[0]
        let fieldType = i[1][0]
        if isBigInt($fieldType):
          let capFieldName = ident("big" & capitalize($fieldName))
          recList.add(newIdentDefs(capFieldName, fieldType))
          block getter:
            let (convType, conv) = getterTypes($fieldType)
            let (convTypeIdent, convIdent) = (ident(convType), ident(conv))
            let getterProc = quote do:
              proc `fieldName`(self: `typeName`): `fieldType` =
                self.`capFieldName`.`convTypeIdent`.`convIdent`.`fieldType`
            let getterProcRef = quote do:
              proc `fieldName`(self: ref `typeName`): `fieldType` =
                self[].`capFieldName`.`convTypeIdent`.`convIdent`.`fieldType`
            result.add(getterProc[0])
            result.add(getterProcRef[0])
          block setter:
            let (convType, conv) = setterTypes($fieldType)
            let (convTypeIdent, convIdent) = (ident(convType), ident(conv))
            var setterName = newNimNode(nnkAccQuoted)
            setterName.add(fieldName)
            setterName.add(ident("="))
            let setterProc = quote do:
              proc `setterName`(self: ref `typeName`, value: `fieldType`) =
                self.`capFieldName` = value.`convTypeIdent`.`convIdent`.`fieldType`
            result.add(setterProc[0])
        else:
          recList.add(newIdentDefs(fieldName, fieldType))
      result[0][0][2][2] = recList

when isMainModule:
  network struct RequestHeader:
    magic: uint8
    opcode: uint8
    keyLength: uint16
    extrasLength: uint8
    dataType: uint8
    vbucket: uint16
    totalBodyLength: uint32
    opaque: uint32
    cas: uint64

  assert sizeof(RequestHeader) == 24
  var header = new(RequestHeader)
  header.keyLength = 8
  assert header.bigKeyLength.int() == (8 shl 8)
  assert header.keyLength.int() == 8