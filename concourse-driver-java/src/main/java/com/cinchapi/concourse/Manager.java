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
package com.cinchapi.concourse;

import java.nio.ByteBuffer;
import java.util.List;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * The client-side interface for Concourse's management capabilities.
 *
 * @author jeff
 */
public class Manager {

    /**
     * The parent driver that contains the connection to thrift.
     */
    private final ConcourseThriftDriver concourse;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    Manager(Concourse concourse) {
        Preconditions.checkArgument(concourse instanceof ConcourseThriftDriver);
        this.concourse = (ConcourseThriftDriver) concourse;
    }

    /**
     * Create a user with the specified {@code username}, {@code password} and
     * {@code role}.
     * 
     * @param username
     * @param password
     * @param role
     */
    public void createUser(String username, String password, String role) {
        ByteBuffer uname = ByteBuffers.fromUtf8String(username);
        ByteBuffer pword = ByteBuffers.fromUtf8String(password);
        List<ComplexTObject> params = ImmutableList.of(
                ComplexTObject.fromJavaObject(uname),
                ComplexTObject.fromJavaObject(pword),
                ComplexTObject.fromJavaObject(role));
        concourse.execute(() -> concourse.thrift()
                .invokeManagement("createUser", params, concourse.creds()));
    }

    /**
     * Grant the {@code permission} to the user with {@code username} in the
     * default environment.
     * 
     * @param username
     * @param permission
     */
    public final void grant(String username, String permission) {
        grant(username, permission, "");
    }

    /**
     * Grant the {@code permission} to the user with {@code username} in the
     * specified {@code environment}.
     * 
     * @param username
     * @param permission
     * @param environment
     */
    public void grant(String username, String permission, String environment) {
        ByteBuffer uname = ByteBuffers.fromUtf8String(username);
        List<ComplexTObject> params = ImmutableList.of(
                ComplexTObject.fromJavaObject(uname),
                ComplexTObject.fromJavaObject(permission),
                ComplexTObject.fromJavaObject(environment));
        concourse.execute(() -> concourse.thrift().invokeManagement("grant",
                params, concourse.creds()));
    }

}
