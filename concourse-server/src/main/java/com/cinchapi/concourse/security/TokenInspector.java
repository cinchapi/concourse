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
package com.cinchapi.concourse.security;

import com.cinchapi.concourse.thrift.AccessToken;

/**
 * A public interface that defines limited methods for {@link AccessToken token}
 * inspection. In practice, the implementation of this interface will likely
 * read through to a {@link UserService}, but this wrapper is intended to
 * prevent the need for direct exposure to the main controller for user account
 * and token information.
 *
 * @author Jeff Nelson
 */
public interface TokenInspector {
    
    public boolean isValidToken(AccessToken token);
    
    public Role getTokenUserRole(AccessToken token);

}
