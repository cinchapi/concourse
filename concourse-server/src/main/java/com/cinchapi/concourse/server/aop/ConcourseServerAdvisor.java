/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import com.cinchapi.concourse.security.Permission;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.ops.CommandIntrospectionAdvice;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

/**
 * The {@link ConcourseServerAdvisor} is an implementation of an aspect-oriented
 * programming concept (https://en.wikipedia.org/wiki/Advice_(programming)) that
 * allows for common functionality to be injected into methods without modifying
 * those methods.
 * <p>
 * This advisor, binds {@link MethodInterceptor interceptors} to methods to
 * {@link ConcourseServer} to inject functionality, logging, exception handling,
 * etc.
 * </p>
 * 
 * A {@link com.google.inject.Module Module} that configures AOP
 * interceptors and injectors that handle Thrift specific needs.
 */
public class ConcourseServerAdvisor extends AbstractModule {

    @Override
    protected void configure() {
        // Intercept client exceptions and re-throw them in a thrift
        // friendly manner
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(TranslateClientExceptions.class),
                new ClientExceptionTranslationAdvice());

        // Intercept management exceptions and re-throw them in a thrift
        // friendly manner
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(TranslateManagementExceptions.class),
                new ManagementExceptionTranslationAdvice());

        // Enforce access restrictions on method invocations.
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyAccessToken.class),
                new AccessTokenVerificationAdvice());
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyAdminRole.class),
                new AdminRoleVerificiationAdvice());
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyReadPermission.class),
                new PermissionVerificationAdvice(Permission.READ));
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyWritePermission.class),
                new PermissionVerificationAdvice(Permission.WRITE));

        // Introspect the current Command and bind it to the current thread
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.not(Matchers.annotatedWith(Internal.class)),
                new CommandIntrospectionAdvice());

    }

}