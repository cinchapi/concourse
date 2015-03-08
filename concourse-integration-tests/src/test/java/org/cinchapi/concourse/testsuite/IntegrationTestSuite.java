/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.testsuite;

import org.cinchapi.concourse.AddTest;
import org.cinchapi.concourse.AtomicOperationWofkflowTest;
import org.cinchapi.concourse.AuditTest;
import org.cinchapi.concourse.BrowseTest;
import org.cinchapi.concourse.CachedConnectionPoolTest;
import org.cinchapi.concourse.ChronologizeTest;
import org.cinchapi.concourse.ClearTest;
import org.cinchapi.concourse.CompoundOperationTest;
import org.cinchapi.concourse.CounterTest;
import org.cinchapi.concourse.FindCriteriaTest;
import org.cinchapi.concourse.InsertTest;
import org.cinchapi.concourse.ReferentialIntegrityTest;
import org.cinchapi.concourse.SecurityExceptionTest;
import org.cinchapi.concourse.SmokeTest;
import org.cinchapi.concourse.FixedConnectionPoolTest;
import org.cinchapi.concourse.TransactionIsolationTest;
import org.cinchapi.concourse.TransactionWorkflowTest;
import org.cinchapi.concourse.VerifyOrSetTest;
import org.cinchapi.concourse.FindOperatorConversionTest;
import org.cinchapi.concourse.thrift.ThriftComplianceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * A collection of integrations tests that deal with the client and server.
 * 
 * @author jnelson
 */
@RunWith(Suite.class)
@SuiteClasses({ TransactionWorkflowTest.class,
        AtomicOperationWofkflowTest.class, CachedConnectionPoolTest.class,
        CompoundOperationTest.class, SmokeTest.class,
        FixedConnectionPoolTest.class, ChronologizeTest.class, ClearTest.class,
        ReferentialIntegrityTest.class, BrowseTest.class,
        SecurityExceptionTest.class, ThriftComplianceTest.class,
        FindCriteriaTest.class, VerifyOrSetTest.class, AddTest.class,
        InsertTest.class, TransactionIsolationTest.class, CounterTest.class,
        FindOperatorConversionTest.class, AuditTest.class })
public class IntegrationTestSuite {

}
