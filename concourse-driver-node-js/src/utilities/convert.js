'use strict'

const _$isArray = require('lodash/isArray')
const _$isBoolean = require('lodash/isBoolean')
const _$isInteger = require('lodash/isInteger')
const _$isMap = require('lodash/isMap')
const _$isNull = require('lodash/isNull')
const _$isNumber = require('lodash/isNumber')
const _$isString = require('lodash/isString')
const _$map = require('lodash/map')
const BN = require('bn.js')
const buffer = require('buffer')
const concourseThriftDataTypes = require('../thrift/data_types')
const concourseThriftSharedTypes = require('../thrift/shared_types')
const Link = require('../link')
const Long = require('../long')
const luxon = require('luxon')
const Tag = require('../tag')

const { Buffer } = buffer
const { DateTime } = luxon

const Convert = {}

Convert.javascriptify = function javascriptify(thriftData) {
  if (_$isMap(thriftData)) {
    const result = new Map()
    for (let [ thriftDataKey, thriftDataValue ] of thriftData) {
      const dataKey = Convert.javascriptify(thriftDataKey)
      const dataValue = Convert.javascriptify(thriftDataValue)
      result.set(dataKey, dataValue)
    }
    return result
  } else if (_$isArray(thriftData)) {
    return _$map(thriftData, (dataValue) => {
      return Convert.javascriptify(dataValue)
    })
  } else if (thriftData instanceof concourseThriftDataTypes.TObject) {
    return Convert.tobjectToJavascript(thriftData)
  } else {
    return thriftData
  }
}

Convert.javascriptToTobject = function javascriptToTobject(object) {
  if (_$isNull(object)) {
    return new concourseThriftDataTypes.TObject({
      data: Buffer.alloc(0),
      type: concourseThriftSharedTypes.Type.NULL
    })
  } else if (_$isBoolean(object)) {
    const boolean = object
    const booleanBuffer = Buffer.alloc(1)
    booleanBuffer.writeUInt8(boolean ? 1 : 0)
    return new concourseThriftDataTypes.TObject({
      data: booleanBuffer,
      type: concourseThriftSharedTypes.Type.BOOLEAN
    })
  } else if (Link.isLink(object)) {
    const link = object
    const { buffer: linkBuffer } = link
    return new concourseThriftDataTypes.TObject({
      data: linkBuffer,
      type: concourseThriftSharedTypes.Type.LINK
    })
  } else if (Long.isLong(object)) {
    const long = object
    const { buffer: longBuffer } = long
    return new concourseThriftDataTypes.TObject({
      data: longBuffer,
      type: concourseThriftSharedTypes.Type.LONG
    })
  } else if (_$isNumber(object)) {
    const number = object
    if (_$isInteger(number)) {
      const integerBuffer = Buffer.alloc(4)
      integerBuffer.writeInt32BE(number)
      return new concourseThriftDataTypes.TObject({
        data: integerBuffer,
        type: concourseThriftSharedTypes.Type.INTEGER
      })
    } else {
      const doubleBuffer = Buffer.alloc(8)
      doubleBuffer.writeDoubleBE(number)
      return new concourseThriftDataTypes.TObject({
        data: doubleBuffer,
        type: concourseThriftSharedTypes.Type.DOUBLE
      })
    }
  } else if (Tag.isTag(object)) {
    const tag = object
    const tagString = String(tag)
    const tagBuffer = Buffer.from(tagString, 'utf8')
    return new concourseThriftDataTypes.TObject({
      data: tagBuffer,
      type: concourseThriftSharedTypes.Type.TAG
    })
  } else if (_$isString(object)) {
    const string = object
    const stringBuffer = Buffer.from(string, 'utf8')
    return new concourseThriftDataTypes.TObject({
      data: stringBuffer,
      type: concourseThriftSharedTypes.Type.STRING
    })
  } else if (DateTime.isDateTime(object)) {
    const dateTime = object
    const millisecondTimestamp = dateTime.toMillis()
    const millisecondTimestampBigNumber = new BN(millisecondTimestamp, 10, 'be')
    const microsecondTimestampBigNumber = millisecondTimestampBigNumber.mul(
      new BN(1000, 10, 'be'))
    const microsecondTimestamp = Long(
      microsecondTimestampBigNumber.toString('hex'))
    const { buffer: microsecondTimestampBuffer } = microsecondTimestamp
    return new concourseThriftDataTypes.TObject({
      data: microsecondTimestampBuffer,
      type: concourseThriftSharedTypes.Type.TIMESTAMP
    })
  }
}

Convert.thriftify = function thriftify(object) {
  if (_$isMap(object)) {
    const mapData = object
    const result = new Map()
    for (let [ mapDatumKey, mapDatumValue ] of mapData) {
      const datumKey = Convert.thriftify(mapDatumKey)
      const datumValue = Convert.thriftify(mapDatumValue)
      result.set(datumKey, datumValue)
    }
    return result
  } else if (_$isArray(object)) {
    const arrayData = object
    return _$map(arrayData, (datum) => {
      return Convert.thriftify(datum)
    })
  } else if (! (object instanceof concourseThriftDataTypes.TObject)) {
    return Convert.javascriptToTobject(object)
  } else {
    const data = object
    return data
  }
}

Convert.tobjectToJavascript = function tobjectToJavascript(tobject) {
  switch (tobject.type) {
    case concourseThriftSharedTypes.Type.BOOLEAN: {
      const value = tobject.data.readUInt8(0)
      return (value === 1) ? true : false
    }
    case concourseThriftSharedTypes.Type.DOUBLE: {
      return tobject.data.readDoubleBE(0)
    }
    case concourseThriftSharedTypes.Type.FLOAT: {
      return tobject.data.readFloatBE(0)
    }
    case concourseThriftSharedTypes.Type.INTEGER: {
      return tobject.data.readInt32BE(0)
    }
    case concourseThriftSharedTypes.Type.LINK: {
      const record = Long(tobject.data)
      return Link.to(record)
    }
    case concourseThriftSharedTypes.Type.LONG: {
      return Long(tobject.data)
    }
    case concourseThriftSharedTypes.Type.NULL: {
      return null
    }
    case concourseThriftSharedTypes.Type.STRING: {
      return tobject.data.toString('utf8')
    }
    case concourseThriftSharedTypes.Type.TAG: {
      const value = tobject.data.toString('utf8')
      return Tag.create(value)
    }
    case concourseThriftSharedTypes.Type.TIMESTAMP: {
      const microsecondTimestamp = Long(tobject.data)
      const millisecondTimestamp = (microsecondTimestamp / 1000) // TODO(@jonathanmarvens): Figure out a way to do this without losing any bits.
      return DateTime.fromMillis(millisecondTimestamp, { zone: 'utc' })
    }
  }
}

exports = module.exports = Convert
