'use strict'

const _$assign = require('lodash/assign')
const _$isFunction = require('lodash/isFunction')
const _$pick = require('lodash/pick')

class ConnectionError extends Error {
  constructor(options) {
    options = _$assign({}, {
      host: null,
      originalError: null,
      port: null
    }, options)
    options = _$pick(options, [
      'host',
      'originalError',
      'port'
    ])
    const message = `There was error connecting to the Concourse Server at ${options.host}:${options.port}`
    Error.call(this, message)
    Object.defineProperties(this, {
      extra: {
        enumerable: true,
        value: _$assign({}, options)
      },
      message: {
        enumerable: true,
        value: message
      },
      name: {
        enumerable: true,
        value: `Client.${this.constructor.name}`
      }
    })
    Object.freeze(this.extra)
    if (_$isFunction(Error.captureStackTrace)) {
      Error.captureStackTrace(this, this.constructor)
    } else {
      const { stack } = new Error(message)
      Object.defineProperty(this, 'stack', {
        enumerable: true,
        value: stack
      })
    }
  }
}

class ThriftClientMethodError extends Error {
  constructor(options) {
    options = _$assign({}, {
      arguments: null,
      error: null,
      method: null
    }, options)
    options = _$pick(options, [
      'arguments',
      'error',
      'method'
    ])
    const message = `There was error calling the method \`${options.method}\` on the Thrift client`
    Error.call(this, message)
    Object.defineProperties(this, {
      extra: {
        enumerable: true,
        value: _$assign({}, options)
      },
      message: {
        enumerable: true,
        value: message
      },
      name: {
        enumerable: true,
        value: `Client.${this.constructor.name}`
      }
    })
    Object.freeze(this.extra)
    if (_$isFunction(Error.captureStackTrace)) {
      Error.captureStackTrace(this, this.constructor)
    } else {
      const { stack } = new Error(message)
      Object.defineProperty(this, 'stack', {
        enumerable: true,
        value: stack
      })
    }
  }
}

exports = module.exports = {
  ConnectionError,
  ThriftClientMethodError
}
