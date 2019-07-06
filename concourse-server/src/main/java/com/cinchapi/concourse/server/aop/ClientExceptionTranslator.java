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
import org.apache.thrift.TException;

import com.cinchapi.ccl.SyntaxException;
import com.cinchapi.concourse.server.plugin.PluginException;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.TransactionStateException;
import com.cinchapi.concourse.thrift.InvalidArgumentException;
import com.cinchapi.concourse.thrift.ParseException;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TransactionException;
import com.cinchapi.concourse.util.Logger;
import com.google.gson.JsonParseException;

/**
 * A {@link MethodInterceptor} that delegates to the underlying annotated
 * method, but catches specific exceptions and translates them to the
 * appropriate Thrift counterparts.
 */
public class ClientExceptionTranslator implements MethodInterceptor {

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        try {
            return invocation.proceed();
        }
        catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(e.getMessage());
        }
        catch (AtomicStateException e) {
            // If an AtomicStateException makes it here, then it must really
            // be a TransactionStateException.
            assert e.getClass() == TransactionStateException.class;
            throw new TransactionException();
        }
        catch (java.lang.SecurityException e) {
            throw new SecurityException(e.getMessage());
        }
        catch (IllegalStateException | JsonParseException | SyntaxException e) {
            // java.text.ParseException is checked, so internal server
            // classes don't use it to indicate parse errors. Since most
            // parsing using some sort of state machine, we've adopted the
            // convention to throw IllegalStateExceptions whenever a parse
            // error has occurred.
            // CON-609: External SyntaxException should be propagated as
            // ParseException
            throw new ParseException(e.getMessage());
        }
        catch (PluginException e) {
            throw new TException(e);
        }
        catch (TException e) {
            // This clause may seem unnecessary, but some of the server
            // methods manually throw TExceptions, so we need to catch them
            // here and re-throw so that they don't get propagated as
            // TTransportExceptions.
            throw e;
        }
        catch (Throwable t) {
            Logger.warn(
                    "The following exception occurred "
                            + "but was not propagated to the client: {}",
                    t.getMessage(), t);
            throw t instanceof RuntimeException ? t : new RuntimeException(t);
        }
    }

}