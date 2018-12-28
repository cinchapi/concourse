/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const isEmpty = require('lodash.isempty');
const isObject = require('lodash.isobject');
const utils = require('./utils');

 /**
  * Concourse constructor.
  * 
  * The exports object of the `concourse-driver-node` module is an instance of this
  * class.
  */
 function Concourse(host = 'localhost', port = 1717, username = 'admin', password = 'admin', environment = '') {
  var kwargs = arguments[0];
  var prefs;
  if(isObject(kwargs)){
    host = 'localhost';
    prefs = utils.findInKwargsByAlias('prefs', kwargs);
    if(!isEmpty(prefs)){
      prefs = require('@cinchapi/configurator')([prefs]) //TODO: expand path
    }
  }
  else {
    kwargs = {};
    prefs = require('@cinchapi/configurator')([]);
  }
  // order of precedence for args: prefs -> kwargs -> positional -> default
  this.host = prefs.get('host') || kwargs['host'] || host;
  this.port = prefs.get('port') || kwargs['port'] || port;
  this.username = prefs.get('username') || utils.findInKwargsByAlias('username', kwargs) || username;
  this.password = prefs.get('password') || utils.findInKwargsByAlias('password', kwargs) || password;
  this.environment = prefs.get('environment') || kwargs['environment'] || environment;
 }

 /**
  * 
  */
Concourse.connect = function(host = 'localhost', port = 1717, username = 'admin', password = 'admin', environment = '') {
  // TODO: support either positional args or kwargs
  return new Concourse(host, port, username, password, environment);
}

 Concourse.prototype.insert = function() {
   dispatch.call(this, arguments)
 }

 function dispatch() {
   //TODO add dispatch logic
   // class function should invoke like dispatch.call(this, arguments)
   // how to handle if one arg is an object but is legitimately not for kwargs
   console.log(this)
  //  console.log(this.host)
  //  console.log(arguments)
 }

 //TODO: will need to support dynamic dispatch in the same way as the PHP driver

 module.exports = exports = Concourse