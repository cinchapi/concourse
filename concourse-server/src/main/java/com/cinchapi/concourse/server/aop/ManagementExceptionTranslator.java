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

import com.cinchapi.concourse.thrift.ManagementException;
import com.cinchapi.concourse.thrift.SecurityException;
import com.google.common.base.Throwables;

/**
 * A {@link MethodInterceptor} that delegates to the underlying annotated
 * method, but catches specific exceptions and translates them to the
 * appropriate Thrift counterparts.
 */
public class ManagementExceptionTranslator implements MethodInterceptor {

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        try {
            return invocation.proceed();
        }
        catch (SecurityException e) {
            throw e;
        }
        catch (ManagementException e) {
            throw e;
        }
        catch (Exception e) {
            Throwable cause = Throwables.getRootCause(e);
            ManagementException ex = new ManagementException(
                    cause.getMessage());
            ex.setStackTrace(cause.getStackTrace());
            throw ex;
        }
    }

}