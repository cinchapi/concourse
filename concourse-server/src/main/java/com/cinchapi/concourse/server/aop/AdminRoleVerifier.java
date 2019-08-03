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

import com.cinchapi.concourse.security.Role;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.Inspector;
import com.cinchapi.concourse.thrift.AccessToken;

/**
 * Intercepts method invocations to verify that the provided {@link AccessToken}
 * represents a user with the {@link Role#ADMIN ADMIN} role.
 *
 * @author Jeff Nelson
 */
public class AdminRoleVerifier implements MethodInterceptor {

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        AccessToken token = null;
        for (Object arg : invocation.getArguments()) {
            if(arg instanceof AccessToken) {
                token = (AccessToken) arg;
                break;
            }
            else {
                continue;
            }
        }
        if(token != null) {
            ConcourseServer concourse = (ConcourseServer) invocation.getThis();
            Inspector inspector = concourse.inspector();
            if(inspector.getTokenUserRole(token) == Role.ADMIN) {
                return invocation.proceed();
            }
            else {
                throw new SecurityException("Unauthorized");
            }
        }
        else {
            throw new SecurityException(
                    "No token was provided to a method that requires a user with the ADMIN role");
        }
    }

}
