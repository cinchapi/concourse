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

const isEmpty = require('lodash.isEmpty');

/**
 * A mapping from preferred kwarg names to a list of acceptable
 * aliases.
 */
var KWARG_ALIASES = {
  'ccl': ['criteria', 'where', 'query'],
  'time': ['timestamp', 'ts'],
  'username': ['user', 'uname'],
  'password': ['pass', 'pword'],
  'prefs': ['file', 'filename', 'config', 'path'],
  'expected': ['value', 'current', 'old'],
  'replacement': ['new', 'other', 'value2'],
  'json': ['data'],
  'record': ['id'],
};

module.exports = exports = {

  /**
   * Find a value for a key in the provided {@code kwargs} by the key
   * itself or one of the defined aliases.
   * 
   * @param key the key to search for
   * @param kwargs the kwargs that are provided to the function
   * @return the value found for the key or an alias, if it exists
   */
  findInKwargsByAlias: function(key, kwargs){
    var value = kwargs[key];
    if(isEmpty(value)){
      let aliases = kwargs[key] || [];
      for(var alias in aliases){
        value = kwargs[aliases];
        if(!isEmpty(value)){
          break;
        }
      }
    }
    return value;
  },

};