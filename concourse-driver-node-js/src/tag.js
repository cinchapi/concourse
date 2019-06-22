'use strict'

const Tag = function Tag(value) {
  String.call(this, value)
  Object.defineProperty(this, 'value', { value })
  Object.defineProperty(this, 'length', {
    get() {
      return this.value.length
    }
  })
}

Tag.prototype = Object.create(String.prototype)
Tag.prototype.constructor = Tag

Tag.prototype[Symbol.toPrimitive] = function (hint) {
  return this.valueOf()
}

Tag.create = function create(...args) {
  return new Tag(...args)
}

Tag.isTag = function isTag(value) {
  return value instanceof Tag
}

Tag.prototype.toString = function toString() {
  return this.value
}

Tag.prototype.valueOf = function valueOf() {
  return this.value
}

exports = module.exports = Tag
