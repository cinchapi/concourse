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
package com.cinchapi.concourse.server.aop;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.cinchapi.concourse.security.Permission;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.Inspector;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.PermissionException;

/**
 * Intercepts method invocations to verify that the provided {@link AccessToken}
 * represents a user with the appropriate permission.
 *
 * @author Jeff Nelson
 */
public class PermissionVerifier implements MethodInterceptor {

    /**
     * The permission to check for.
     */
    private final Permission permission;

    /**
     * Construct a new instance.
     * 
     * @param permission
     */
    public PermissionVerifier(Permission permission) {
        this.permission = permission;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        AccessToken token = null;
        String environment = null;
        Object[] args = invocation.getArguments();
        int i = 0;
        while ((token == null || environment == null) && i < args.length) {
            Object arg = args[i];
            if(token == null && arg instanceof AccessToken) {
                token = (AccessToken) arg;
            }
            else if(token != null && environment == null
                    && arg instanceof String) {
                // This relies on the convention that the environment parameter
                // always comes after the AccessToken parameter
                environment = (String) arg;
            }
            ++i;
        }
        if(token != null && environment != null) {
            ConcourseServer concourse = (ConcourseServer) invocation.getThis();
            Inspector inspector = concourse.inspector();
            if(inspector.tokenUserHasPermission(token, permission,
                    environment)) {
                return invocation.proceed();
            }
            else {
                throw new PermissionException("Insufficient Permission");
            }
        }
        else {
            throw new IllegalStateException(
                    "Cannot verify permissions without an AccessToken and environment");
        }
    }

}
