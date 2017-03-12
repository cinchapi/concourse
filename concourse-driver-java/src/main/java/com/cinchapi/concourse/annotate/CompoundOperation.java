/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.annotate;

/**
 * A {@link com.cinchapi.concourse.Concourse Concourse} operation that combines
 * two or more operations in a <strong>non-atomic</strong> manner. This means
 * that certain parts of the operation may fail without affecting the success of
 * other parts.
 * 
 * @author Jeff Nelson
 */
public @interface CompoundOperation {

}
