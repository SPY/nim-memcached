import macros, strutils
import endians

type FieldType = tuple[isEnum: bool, storeType, pubType: string, default: NimNode]
type Field = tuple[pub: bool, name: string, fieldType: FieldType]
type NetworkStruct = tuple[pub: bool, name: string, fields: seq[Field]]

const bigInts = ["int16", "uint16", "int32", "uint32", "int64", "uint64"]

proc isBigInt(strType: string): bool =
  return strType in bigInts

proc netToHost(val: int8): int8 = val
proc hostToNet(val: int8): int8 = val
proc netToHost(val: uint8): uint8 = val
proc hostToNet(val: uint8): uint8 = val

macro makeEndianSwapper(capacity: int, typeStr: string): stmt =
  let intType = ident($typeStr)
  let swapper = ident("swapEndian" & $intVal(capacity))
  let res = genSym(nskVar, "res")
  result = quote do:
    proc netToHost(val: `intType`): `intType` =
      var inp = val
      var `res`: `intType`
      `swapper`(addr `res`, addr inp)
      `res`
  result.add quote do:
    proc hostToNet(val: `intType`): `intType` =
      var inp = val
      var `res`: `intType`
      `swapper`(addr `res`, addr inp)
      `res`

makeEndianSwapper(16, "int16")
makeEndianSwapper(16, "uint16")
makeEndianSwapper(32, "int32")
makeEndianSwapper(32, "uint32")
makeEndianSwapper(64, "int64")
makeEndianSwapper(64, "uint64")

proc `&`(str: string): NimNode {. compileTime .} =
  result = ident(str)

proc makeCounstructor(def: NetworkStruct): NimNode {. compileTime .} =
  let ctorIdent = ident("new" & capitalize(def.name))
  let ctorName = if def.pub: postfix(ctorIdent, "*") else: ctorIdent
  let value = genSym(nskVar, "value")
  let typeName = &def.name
  var ctor = quote do:
    proc `ctorName`(): ref `typeName` =
      var `value` = new(`typeName`)
  var ctorParams = ctor[0][3]
  var ctorBody = ctor[0][6]
  for field in def.fields:
    let fieldName = &field.name
    let assign = quote do:
      `value`.`fieldName` = `fieldName`
    ctorParams.add(newIdentDefs(fieldName, &field.fieldType.pubType, field.fieldType.default))
    ctorBody.add(assign[0])
  ctorBody.add(value)
  ctor[0]

proc makeGetter(name: string, field: Field): tuple[get, getref: NimNode] {. compileTime .} =
  let retType = &field.fieldType.pubType
  let capFieldName = ident("big" & capitalize(field.name))
  let fieldNameWithPub = if field.pub: postfix(&field.name, "*") else: &field.name
  let typeName = &name
  let getterProc = quote do:
    proc `fieldNameWithPub`(self: `typeName`): `retType` =
      self.`capFieldName`.netToHost().`retType`
  let getterProcRef = quote do:
    proc `fieldNameWithPub`(self: ref `typeName`): `retType` =
      self[].`capFieldName`.netToHost().`retType`
  (getterProc[0], getterProcRef[0])

proc makeSetter(name: string, field: Field): NimNode {. compileTime .} =
  let valType = &field.fieldType.pubType
  let capFieldName = ident("big" & capitalize(field.name))
  var setterName = newNimNode(nnkAccQuoted)
  setterName.add(&field.name)
  setterName.add(&"=")
  let setterNameWithPub = if field.pub: postfix(setterName, "*") else: setterName
  let typeName = &name
  let fieldType = &field.fieldType.storeType
  let setterProc = quote do:
    proc `setterNameWithPub`(self: ref `typeName`, value: `valType`) =
      self.`capFieldName` = ord(value).`fieldType`.hostToNet()
  setterProc[0]

proc parseHeader(header: NimNode): tuple[pub: bool, name: string] {. compileTime .} =
  if header.kind != nnkCommand or $header[0] != "struct":
    error "Def header should be in format \"network struct [pub|priv] <type>\""
  let def = header[1]
  case def.kind
  of nnkIdent:
    # just identifier: network struct TypeName
    result = (false, $def)
  of nnkCommand:
    # with access quantificator: network struct pub TypeName
    let access = $def[0]
    if access != "pub" and access != "priv":
      error "Only two access quantificators available: pub and priv"
    if def[1].kind != nnkIdent:
      error "Struct type name should be identifier"
    result = (access == "pub", $def[1])
  else:
    error "Wrong network struct header format"

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
      error "Wrong field type default value format"
  of nnkCall:
    # enum field without default
    let enumType = fieldType[0][0]
    let def = quote do:
      low(`enumType`)
    result = (true, $fieldType[0][1], $enumType, def[0])
  else:
    error "Wrong field type format. Desired: [pub/priv] <name>: <int-type>|Enum(<int-type) [= <default>]"

proc parseField(field: NimNode): Field {. compileTime .} =
  case field.kind
  of nnkCall:
    # without access: field: <type>
    result = (false, $field[0], parseFieldType(field[1]))
  of nnkCommand:
    let access = $field[0]
    if access != "pub" and access != "priv":
      error "Only two access quantificators available: pub and priv"
    result = (access == "pub", $field[1], parseFieldType(field[2]))
  else:
    error "Wrong field format"

proc parseDefAst(header: NimNode, body: NimNode): NetworkStruct {. compileTime .} =
  let (access, name) = parseHeader(header)
  var fields = newSeq[Field]()
  for field in body.children:
    fields.add(parseField(field))
  result = (access, name, fields)

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
      let (get, getref) = makeGetter(def.name, field)
      result.add(get)
      result.add(getref)
      result.add(makeSetter(def.name, field))
    else:
      let fieldName = if field.pub: postfix(&field.name, "*") else: &field.name
      recList.add(newIdentDefs(fieldName, &field.fieldType.storeType))
  result[0][0][2][2] = recList
  result.add(makeCounstructor(def))

when isMainModule:
  type Opcode = enum One = 3, Two = 7, Three = 8

  network struct RequestHeader:
    magic: uint8 = 0x80
    pub opcode: Opcode(uint8) = Two
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
  header.totalBodyLength = 0xdeadbeef