'use strict'

const _$assign = require('lodash/assign')
const _$forEach = require('lodash/forEach')
const _$isArray = require('lodash/isArray')
const _$isNumber = require('lodash/isNumber')
const _$isString = require('lodash/isString')
const _$isUndefined = require('lodash/isUndefined')
const _$pick = require('lodash/pick')
const ConcourseThriftService = require('../thrift/ConcourseService')
const errors = require('./errors')
const javaProperties = require('java-properties')
const path = require('path')
const Promise = require('bluebird')
const thrift = require('thrift')
const utilities = require('../utilities')

class Client {
  constructor(options) {
    return (async (options) => {
      options = _$assign({}, options)
      const defaultOptions = {
        environment: '',
        host: 'localhost',
        password: 'admin',
        port: 1717,
        username: 'admin'
      }
      if (_$isString(options.prefs)) {
        options.prefs = path.resolve(options.prefs)
        const preferencesFileProperties = javaProperties.of(options.prefs)
        delete options.prefs
        const preferencesFileOptions = {
          environment: preferencesFileProperties.get('environment'),
          host: preferencesFileProperties.get('host'),
          password: preferencesFileProperties.get('password'),
          port: preferencesFileProperties.getInt('port'),
          username: preferencesFileProperties.get('username')
        }
        _$forEach(defaultOptions, (defaultOptionValue, optionKey) => {
          if (_$isUndefined(options[optionKey])) {
            options[optionKey] = (! _$isUndefined(preferencesFileOptions[optionKey])) ? preferencesFileOptions[optionKey] : defaultOptionValue
          }
        })
      } else {
        _$forEach(defaultOptions, (defaultOptionValue, optionKey) => {
          options[optionKey] = (! _$isUndefined(options[optionKey])) ? options[optionKey] : defaultOptionValue
        })
      }
      options = _$pick(options, [
        'environment',
        'host',
        'password',
        'port',
        'username'
      ])
      _$forEach(options, (optionValue, optionKey) => {
        Object.defineProperty(this, optionKey, { value: optionValue })
      })
      await this.connect()
      Object.defineProperty(this, 'credentials', {
        value: await this.authenticate()
      })
      Object.defineProperty(this, 'transaction', {
        value: null,
        writable: true
      })
      return this
    })(options)
  }

  async _callThriftClientMethod(name, arguments_, reject) {
    try {
      return await this.client[name](...arguments_)
    } catch (error) {
      error = new Client.ThriftClientMethodError({
        arguments: arguments_,
        error,
        method: name
      })
      reject(error)
    }
  }

  async authenticate() {
    return new Promise(async (resolve, reject) => {
      const credentials = await this._callThriftClientMethod('login', [
        this.username,
        this.password,
        this.environment
      ], reject)
      if (_$isUndefined(credentials)) {
        return
      }
      resolve(credentials)
    })
  }

  async connect() {
    return new Promise((resolve, reject) => {
      Object.defineProperty(this, 'connection', {
        value: thrift.createConnection(this.host, this.port, {
          protocol: thrift.TBinaryProtocol,
          transport: thrift.TBufferedTransport
        })
      })
      this.connection.on('error', (error) => {
        error = new Client.ConnectionError({
          host: this.host,
          originalError: error,
          port: this.port
        })
        reject(error)
      })
      Object.defineProperty(this, 'client', {
        value: thrift.createClient(ConcourseThriftService.Client, this.connection)
      })
      this.connection.on('connect', () => {
        resolve()
      })
    })
  }

  select(options) {
    return new Promise(async (resolve, reject) => {
      options = _$assign({}, options)
      let { criteria } = options
      const keys = (! _$isUndefined(options.keys)) ? options.keys : options.key
      let records = (! _$isUndefined(options.records)) ? options.records : options.record
      const { timestamp } = options
      let data
      if (_$isArray(records) &&
          _$isUndefined(keys) &&
          _$isUndefined(timestamp)) {
        data = await this._callThriftClientMethod('selectRecords', [
          records,
          this.credentials,
          this.transaction,
          this.environment
        ], reject)
        if (_$isUndefined(data)) { return }
      } else if (_$isArray(records) &&
                 _$isNumber(timestamp) &&
                 _$isUndefined(keys)) {
        data = await this._callThriftClientMethod('selectRecordsTime', [
          records,
          timestamp,
          this.credentials,
          this.transaction,
          this.environment
        ], reject)
        if (_$isUndefined(data)) { return }
      } else if (_$isArray(records) &&
                 _$isString(timestamp) &&
                 _$isUndefined(keys)) {
        data = await this._callThriftClientMethod('selectRecordsTimestr', [
          records,
          timestamp,
          this.credentials,
          this.transaction,
          this.environment
        ], reject)
        if (_$isUndefined(data)) { return }
      }// else if () {
        //
      //}
      data = utilities.Convert.javascriptify(data)
      resolve(data)
    })
  }

  toString() {
    return `Connected to ${this.host}:${this.port} as ${this.username}`
  }
}

_$assign(Client, errors)

Client.connect = (...args) => new Client(...args)

exports = module.exports = Client
