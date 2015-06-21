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
  of "int8", "uint8": return (strType, strType)
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
  of "int8", "uint8": return (strType, strType)
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
      let ctorName = ident("new" & capitalize($typeName))
      let value = genSym(nskVar, "value")
      var ctor = quote do:
        proc `ctorName`(): ref `typeName` =
          var `value` = new(`typeName`)
      var ctorParams = ctor[0][3]
      var ctorBody = ctor[0][6]
      # result.add(ctor)
      var recList = newNimNode(nnkRecList)
      for i in body.children:
        let fieldName = i[0]
        # let fieldType = i[1][0]
        var fieldType, defValue: NimNode
        var isEnum = false
        var enumType: NimNode
        case i[1][0].kind
        of nnkIdent:
          fieldType = i[1][0]
          defValue = newIntLitNode(0)
        of nnkAsgn:
          fieldType = i[1][0][0]
          defValue = i[1][0][1]
        of nnkCall:
          fieldType = i[1][0][1]
          isEnum = true
          enumType = i[1][0][0]
          let def = quote do:
            low(`enumType`)
          defValue = def[0]
        else: discard
        let assign = quote do:
          `value`.`fieldName` = `fieldName`
        let valType = if isEnum: enumType else: fieldType
        ctorParams.add(newIdentDefs(fieldName, valType, defValue))
        ctorBody.add(assign[0])
        if isBigInt($fieldType) or isEnum:
          let capFieldName = ident("big" & capitalize($fieldName))
          recList.add(newIdentDefs(capFieldName, fieldType))
          block getter:
            let (convType, conv) = getterTypes($fieldType)
            let (convTypeIdent, convIdent) = (ident(convType), ident(conv))
            let retType = if isEnum: enumType else: fieldType
            let getterProc = quote do:
              proc `fieldName`(self: `typeName`): `retType` =
                self.`capFieldName`.`convTypeIdent`.`convIdent`.`retType`
            let getterProcRef = quote do:
              proc `fieldName`(self: ref `typeName`): `retType` =
                self[].`capFieldName`.`convTypeIdent`.`convIdent`.`retType`
            result.add(getterProc[0])
            result.add(getterProcRef[0])
          block setter:
            let (convType, conv) = setterTypes($fieldType)
            let (convTypeIdent, convIdent) = (ident(convType), ident(conv))
            var setterName = newNimNode(nnkAccQuoted)
            setterName.add(fieldName)
            setterName.add(ident("="))
            let setterProc = quote do:
              proc `setterName`(self: ref `typeName`, value: `valType`) =
                self.`capFieldName` = ord(value).`convTypeIdent`.`convIdent`.`fieldType`
            result.add(setterProc[0])
        else:
          recList.add(newIdentDefs(fieldName, fieldType))
      ctorBody.add(value)
      result[0][0][2][2] = recList
      result.add(ctor[0])

when isMainModule:
  type Opcode = enum
    One = 3, Two = 7, Three = 8

  network struct RequestHeader:
    magic: uint8 = 0x80
    opcode: Opcode(uint8)
    keyLength: uint16
    extrasLength: uint8
    dataType: uint8
    vbucket: uint16
    totalBodyLength: uint32
    opaque: uint32
    cas: uint64

  assert sizeof(RequestHeader) == 24
  var header = newRequestHeader(keyLength = 8)
  assert header.opcode == One
  assert header.bigKeyLength.int() == (8 shl 8)
  assert header.keyLength.int() == 8