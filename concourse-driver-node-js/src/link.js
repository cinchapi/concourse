'use strict'

const _$isBuffer = require('lodash/isBuffer')
const _$isObject = require('lodash/isObject')
const Long = require('./long')

const Link = function Link(record) {
  if (_$isObject(record) &&
      _$isBuffer(record.buffer)) {
    record = record.buffer
  }
  Long.call(this, record)
}

Link.prototype = Object.create(Long.prototype)
Link.prototype.constructor = Link

Link.prototype[Symbol.toPrimitive] = function (hint) {
  return this.valueOf()
}

Link.isLink = function isLink(value) {
  return value instanceof Link
}

Link.to = function to(...args) {
  return new Link(...args)
}

Link.toWhere = function toWhere(ccl) {
  return `@${ccl}@`
}

Link.prototype.toString = function toString() {
  const value = Long.prototype.toString.call(this)
  return `@${value}`
}

exports = module.exports = Link
