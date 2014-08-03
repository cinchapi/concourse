/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.testsuite;

import org.cinchapi.concourse.AddTest;
import org.cinchapi.concourse.AtomicOperationWofkflowTest;
import org.cinchapi.concourse.BrowseTest;
import org.cinchapi.concourse.CachedConnectionPoolTest;
import org.cinchapi.concourse.ChronologizeTest;
import org.cinchapi.concourse.ClearTest;
import org.cinchapi.concourse.CompoundOperationTest;
import org.cinchapi.concourse.FindCriteriaTest;
import org.cinchapi.concourse.ReferentialIntegrityTest;
import org.cinchapi.concourse.SecurityExceptionTest;
import org.cinchapi.concourse.SmokeTest;
import org.cinchapi.concourse.FixedConnectionPoolTest;
import org.cinchapi.concourse.ThriftComplianceTest;
import org.cinchapi.concourse.TransactionWorkflowTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 
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
        FindCriteriaTest.class, AddTest.class })
public class IntegrationTestSuite {

}
