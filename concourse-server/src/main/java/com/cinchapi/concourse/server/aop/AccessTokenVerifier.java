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

import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TransactionToken;

/**
 * Intercepts method invocations to verify that the provided {@link AccessToken}
 * is valid and associated with the correct transaction (if necessary).
 *
 * @author Jeff Nelson
 */
public class AccessTokenVerifier implements MethodInterceptor {

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        AccessToken token = null;
        TransactionToken transaction = null;
        Object[] args = invocation.getArguments();
        int index = 0;
        while (token == null && (transaction == null || index < args.length)) {
            Object arg = args[index];
            if(arg instanceof AccessToken) {
                token = (AccessToken) arg;
            }
            else if(arg instanceof TransactionToken) {
                transaction = (TransactionToken) arg;
            }
            ++index;
        }
        if(token != null) {
            ConcourseServer concourse = (ConcourseServer) invocation.getThis();
            if(concourse.inspector().isValidToken(token)) {
                if(transaction == null || (transaction != null
                        && transaction.getAccessToken().equals(token)
                        && concourse.inspector()
                                .isValidTransaction(transaction))) {
                    return invocation.proceed();
                }
                else {
                    throw new IllegalArgumentException("Invalid transaction");
                }
            }
            else {
                throw new SecurityException("Invalid access token");
            }
        }
        else {
            throw new SecurityException("Unauthorized");
        }
    }

}
