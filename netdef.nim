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

proc makeGetter(typeName, fieldName, fieldType, enumType: NimNode, isEnum, isPub: bool): tuple[get, getref: NimNode] {. compileTime .} =
  let (convType, conv) = getterTypes($fieldType)
  let (convTypeIdent, convIdent) = (ident(convType), ident(conv))
  let retType = if isEnum: enumType else: fieldType
  let capFieldName = ident("big" & capitalize($fieldName))
  let fieldNameWithPub = if isPub: postfix(fieldName, "*") else: fieldName
  let getterProc = quote do:
    proc `fieldNameWithPub`(self: `typeName`): `retType` =
      self.`capFieldName`.`convTypeIdent`.`convIdent`.`retType`
  let getterProcRef = quote do:
    proc `fieldNameWithPub`(self: ref `typeName`): `retType` =
      self[].`capFieldName`.`convTypeIdent`.`convIdent`.`retType`
  (getterProc[0], getterProcRef[0])

proc makeSetter(typeName, fieldName, fieldType, enumType: NimNode, isEnum, isPub: bool): NimNode {. compileTime .} =
  let valType = if isEnum: enumType else: fieldType
  let (convType, conv) = setterTypes($fieldType)
  let (convTypeIdent, convIdent) = (ident(convType), ident(conv))
  let retType = if isEnum: enumType else: fieldType
  let capFieldName = ident("big" & capitalize($fieldName))
  var setterName = newNimNode(nnkAccQuoted)
  setterName.add(fieldName)
  setterName.add(ident("="))
  let setterNameWithPub = if isPub: postfix(setterName, "*") else: setterName
  let setterProc = quote do:
    proc `setterNameWithPub`(self: ref `typeName`, value: `valType`) =
      self.`capFieldName` = ord(value).`convTypeIdent`.`convIdent`.`fieldType`
  setterProc[0]

proc parseHeader(header: NimNode): tuple[pub: bool, name: string] {. compileTime .} =
  assert(
    header.kind == nnkCommand and $header[0] == "struct",
    "Def header should be in format \"network struct [pub|priv] <type>\""
  )
  let def = header[1]
  case def.kind
  of nnkIdent:
    # just identifier: network struct TypeName
    result = (false, $def)
  of nnkCommand:
    # with access quantificator: network struct pub TypeName
    let access = $def[0]
    assert(
      access == "pub" or access == "priv",
      "Only two access quantificators available: pub and priv"
    )
    assert(
      def[1].kind == nnkIdent,
      "Struct type name should be identifier"
    )
    result = (access == "pub", $def[1])
  else:
    assert(false, "Wrong network struct header format")

type FieldType = tuple[isEnum: bool, storeType, pubType: string, default: NimNode]
type Field = tuple[pub: bool, name: string, fieldType: FieldType]
type NetworkStruct = tuple[pub: bool, name: string, fields: seq[Field]]

proc parseFieldType(fieldType: NimNode): FieldType {. compileTime .} =
  case fieldType[0].kind
  of nnkIdent:
    # just integer type
    let typeName = $fieldType[0]
    result = (false, typeName, typeName, newIntLitNode(0))
  of nnkAsgn:
    # field with default value
    case fieldType[0][0].kind
    of nnkIdent:
      # int field with defalut value
      let typeName = $fieldType[0][0]
      result = (false, typeName, typeName, fieldType[0][1])
    of nnkCall:
      # enum field with defalut value
      result = (true, $fieldType[0][0][1], $fieldType[0][0][0], fieldType[0][1])
    else:
      assert(false, "Wrong field type default value format")
  of nnkCall:
    # enum field without default
    let enumType = fieldType[0][0]
    let def = quote do:
      low(`enumType`)
    result = (true, $fieldType[0][1], $enumType, def[0])
  else:
    assert(false, "Wrong field type format")

proc parseField(field: NimNode): Field {. compileTime .} =
  case field.kind
  of nnkCall:
    # without access: field: <type>
    result = (false, $field[0], parseFieldType(field[1]))
  of nnkCommand:
    let access = $field[0]
    assert(
      access == "pub" or access == "priv",
      "Only two access quantificators available: pub and priv"
    )
    result = (access == "pub", $field[1], parseFieldType(field[2]))
  else:
    assert(false, "Wrong field format")

proc parseDefAst(header: NimNode, body: NimNode): NetworkStruct {. compileTime .} =
  let (access, name) = parseHeader(header)
  var fields = newSeq[Field]()
  for field in body.children:
    fields.add(parseField(field))
  result = (access, name, fields)

proc `&`(str: string): NimNode {. compileTime .} =
  result = ident(str)

proc makeCounstructor(typeName: NimNode, fields: seq[Field], isPub: bool): NimNode {. compileTime .} =
  let ctorIdent = ident("new" & capitalize($typeName))
  let ctorName = if isPub: postfix(ctorIdent, "*") else: ctorIdent
  let value = genSym(nskVar, "value")
  var ctor = quote do:
    proc `ctorName`(): ref `typeName` =
      var `value` = new(`typeName`)
  var ctorParams = ctor[0][3]
  var ctorBody = ctor[0][6]
  for field in fields:
    let fieldName = &field.name
    let assign = quote do:
      `value`.`fieldName` = `fieldName`
    let valType = &field.fieldType.pubType
    ctorParams.add(newIdentDefs(fieldName, valType, field.fieldType.default))
    ctorBody.add(assign[0])
  ctorBody.add(value)
  ctor[0]

macro network*(command, body: stmt): stmt {. immediate .} =
  let def = parseDefAst(command, body)
  let typeName = if def.pub: postfix(&def.name, "*") else: &def.name
  result = quote do:
    type `typeName` = object {. pure .}
  var recList = newNimNode(nnkRecList)
  for field in def.fields:
    if isBigInt(field.fieldType.storeType) or field.fieldType.isEnum:
      let capFieldName = ident("big" & capitalize(field.name))
      recList.add(newIdentDefs(capFieldName, &field.fieldType.storeType))
      let (get, getref) = makeGetter(
        &def.name,
        &field.name,
        &field.fieldType.storeType,
        &field.fieldType.pubType,
        field.fieldType.isEnum,
        field.pub
      )
      result.add(get)
      result.add(getref)
      result.add(makeSetter(
        &def.name,
        &field.name,
        &field.fieldType.storeType,
        &field.fieldType.pubType,
        field.fieldType.isEnum,
        field.pub
      ))
    else:
      let fieldName = if field.pub: postfix(&field.name, "*") else: &field.name
      recList.add(newIdentDefs(fieldName, &field.fieldType.storeType))
  result[0][0][2][2] = recList
  result.add(makeCounstructor(&def.name, def.fields, def.pub))

when isMainModule:
  type Opcode = enum One = 3, Two = 7, Three = 8

  network struct RequestHeader:
    magic: uint8 = 0x80
    opcode: Opcode(uint8) = Two
    keyLength: uint16
    extrasLength: uint8
    dataType: uint8
    vbucket: uint16
    totalBodyLength: uint32
    opaque: uint32
    cas: uint64

  assert sizeof(RequestHeader) == 24
  var header = newRequestHeader(keyLength = 8)
  assert header.opcode == Two
  header.opcode = Three
  assert header.opcode.int() == 8
  assert header.bigKeyLength.int() == (8 shl 8)
  assert header.keyLength.int() == 8