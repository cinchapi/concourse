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

import com.cinchapi.concourse.security.Permission;
import com.cinchapi.concourse.server.ConcourseServer;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

/**
 * A {@link com.google.inject.Module Module} that configures AOP
 * interceptors and injectors that handle Thrift specific needs.
 */
public class AnnotationBasedInjector extends AbstractModule {

    @Override
    protected void configure() {
        // Intercept client exceptions and re-throw them in a thrift
        // friendly manner
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(ThrowsClientExceptions.class),
                new ClientExceptionTranslator());

        // Intercept management exceptions and re-throw them in a thrift
        // friendly manner
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(ThrowsManagementExceptions.class),
                new ManagementExceptionTranslator());

        // Enforce access restrictions on method invocations.
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyAccessToken.class),
                new AccessTokenVerifier());
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyAdminRole.class),
                new AdminRoleVerifier());
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyReadPermission.class),
                new PermissionVerifier(Permission.READ));
        bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                Matchers.annotatedWith(VerifyWritePermission.class),
                new PermissionVerifier(Permission.WRITE));

    }

}