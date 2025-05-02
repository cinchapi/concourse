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

const Client = require('./thrift/ConcourseService');
const isEmpty = require('lodash.isempty');
const isObject = require('lodash.isobject');
const thrift = require('thrift');
const utils = require('./utils');

/**
 * A 2D mapping from each driver method to all possible Thrift 
 * dispatch routes and each route to the precise method signature.
 */
const SIGNATURES = {};

/**
 * The dispatchable Thrift methods defined in ConcourseServiceClient.
 */
var THRIFT_METHODS = null;

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
    // The user has provided kwargs, so check to see if a preferences file 
    // has been declared.
    prefs = utils.findInKwargsByAlias('prefs', kwargs);
    if(!isEmpty(prefs)){
      prefs = require('@cinchapi/configurator')([prefs]) //TODO: expand path
    }
  }
  kwargs = kwargs || {};
  prefs = prefs || require('@cinchapi/configurator')([]);
  // order of precedence for args: prefs -> kwargs -> positional -> default
  this.host = prefs.get('host') || kwargs['host'] || host;
  this.port = prefs.get('port') || kwargs['port'] || port;
  this.username = prefs.get('username') || utils.findInKwargsByAlias('username', kwargs) || username;
  this.password = prefs.get('password') || utils.findInKwargsByAlias('password', kwargs) || password;
  this.environment = prefs.get('environment') || kwargs['environment'] || environment;

  // Create Thrift connection
  this.connection = thrift.createConnection(this.host, this.port, {
    transport: thrift.TBufferedTransport,
    protocol: thrift.TBinaryProtocol
  });
  this.connection.on('error', function(err){
    console.log(err);
    //TODO will need to inspect error type and respond accordingly
    throw new Error('Could not connect to the Concourse Server at '+this.host+":"+this.port);
  });
  this.client = thrift.createClient(Client, this.connection);
  //TODO: authenticate
 }

 /**
  * 
  */
Concourse.connect = function(host = 'localhost', port = 1717, username = 'admin', password = 'admin', environment = '') {
  // TODO: support either positional args or kwargs
  return new Concourse(host, port, username, password, environment);
}

/**
 * Terminate the client's session and close the connection.
 * This is an alias for the {@link #exit()} method.
 */
Concourse.prototype.close = async function() {
  //TODO logout
  this.connection.end();
}

/**
 * Terminate the client's session and close the connection.
 * This is an alias for the {@link #close()} method.
 */
Concourse.prototype.exit = Concourse.prototype.close;

// TODO: use https://caolan.github.io/async/docs.html#asyncify to make async versions of all the sync functions

Concourse.prototype.getServerVersion = async function() {
  return this.client.getServerVersion();;
}

 Concourse.prototype.insert = async function() {
   return dispatch.call(this, 'insert', arguments)
 }

 /**
  * Dynamically dispatch to the appropriate Thrift method based on the {@code method}
  * and arguments provided.
  * @param {string} method 
  */
 function dispatch(method) {
   enableDynamicDispatch.call(this, method);

   // Gather the arguments that were passed to the original function
   let args = Object.values(arguments);
   args.shift();
   let kwargs;
   if(isObject(args[arg.length - 1])){
    kwargs = args.pop();
   }
   else {
     kwargs = [];
   }
   const okwargs = kwargs;
   kwargs = resolveKwargAliases(kwargs); //TODO: implement
   kwargs = sortKwargs(kwargs); //TODO: implement


   console.log(args);
   //TODO add dispatch logic
   // class function should invoke like dispatch.call(this, arguments)
   // how to handle if one arg is an object but is legitimately not for kwargs
  //  console.log(this)
  //  console.log(this.host)
  //  console.log(arguments)
 }

 /**
  * Ensure that the driver is setup to dynamically dispatch invocations 
  * of {@code method}.
  * @param {string} method 
  */
 function enableDynamicDispatch(method) {
   if(isEmpty(SIGNATURES[method])){
     // Capture all possible Thrift dispatch routes for #method on the fly
     SIGNATURES[method] = [];
     if(isEmpty(THRIFT_METHODS)){
       let methods = [];
       let obj = this.client;
       do {
         methods = methods.concat(Object.getOwnPropertyNames(obj));
       }
       while(obj = Object.getPrototypeOf(obj));
       THRIFT_METHODS = methods.filter(function(name) {
        return !name.startsWith('send_') && !name.startsWith('recv_') && !name.indexOf('Criteria') > -1;
       });
     }
     THRIFT_METHODS.forEach(function(tmethod){
      if(tmethod.startsWith(method) && tmethod != 'getServerVersion' && tmethod != 'getServerEnvironment') {
        let args = tmethod.match(/((?:^|[A-Z])[a-z]+)/g);
        args.shift();
        let signature = [];
        args.forEach(function(arg) {
          let type = getArgType(arg);
          signature[arg] = type;
        });
        SIGNATURES[method][tmethod] = signature;
      }
     });
   } 
 }

 /**
  * Return the expected argument type based on the argument name
  * @param {string} arg 
  */
 function getArgType(arg) {
   if(arg.endsWith('str') || ['Key', 'Ccl', 'Phrase'].includes(arg)) {
     return 'string';
   }
   else if(arg == 'Value') {
     return 'TObject' ; //TODO 
   }
   else if(arg == 'Operator') {
    return 'Operator' //TODO
   }
   else if(['Record', 'Time', 'Start', 'End'].includes(arg)){
     return 'int';
   }
   else if(arg.endsWith('s')){
     return 'array';
   }
   else {
     return arg.toLowerCase();
   }
 }

 //TODO: will need to support dynamic dispatch in the same way as the PHP driver

 module.exports = exports = Concourse