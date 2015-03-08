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

import org.cinchapi.concourse.bugrepro.CON108;
import org.cinchapi.concourse.bugrepro.CON167;
import org.cinchapi.concourse.bugrepro.CON171;
import org.cinchapi.concourse.bugrepro.CON173;
import org.cinchapi.concourse.bugrepro.CON217;
import org.cinchapi.concourse.bugrepro.CON52;
import org.cinchapi.concourse.bugrepro.CON55;
import org.cinchapi.concourse.bugrepro.CON72;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 
 * 
 * @author Jeff Nelson
 */
@RunWith(Suite.class)
@SuiteClasses({ CON52.class, CON55.class, CON72.class, CON108.class,
        CON167.class, CON173.class, CON171.class, CON217.class })
public class BugReproSuite {

}
