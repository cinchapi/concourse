/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server;

import com.cinchapi.concourse.security.Permission;
import com.cinchapi.concourse.security.Role;
import com.cinchapi.concourse.security.UserService;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;

/**
 * A public interface that defines limited methods for {@link ConcourseServer
 * server} inspection. In practice, the implementation of this interface will
 * likely read through to a {@link UserService}, but this wrapper is intended to
 * prevent the need for direct exposure to the main controller for user account
 * and token information.
 *
 * @author Jeff Nelson
 */
public interface Inspector {

    /**
     * Return the {@link Role role} of the user represented by the
     * {@code token}.
     * 
     * @param token
     * @return the represented user's role
     */
    public Role getTokenUserRole(AccessToken token);

    /**
     * Return {@code true} if the {@code token} is valid.
     * 
     * @param token
     * @return {@code true} if the {@code token} is valid
     */
    public boolean isValidToken(AccessToken token);

    /**
     * Return {@code true} if the specified {@code transaction} exists.
     * 
     * @param transaction
     * @return {@code true} if the {@code transaction} exists.
     */
    public boolean isValidTransaction(TransactionToken transaction);

    /**
     * Return {@code true} if the {@code token} user has the specified
     * {@code permission} in the specified {@code environment}.
     * 
     * @param token
     * @param permission
     * @param environment
     * @return {@code true} if the token user has the permission
     */
    public boolean tokenUserHasPermission(AccessToken token,
            Permission permission, String environment);

}
