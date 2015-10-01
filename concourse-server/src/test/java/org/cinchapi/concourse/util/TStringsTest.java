/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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
package org.cinchapi.concourse.util;

import org.cinchapi.concourse.test.ConcourseBaseTest;
import org.cinchapi.concourse.test.Variables;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TStrings} util class.
 * 
 * @author Jeff Nelson
 */
public class TStringsTest extends ConcourseBaseTest {

    @Test
    public void testIsSubStringReproA() {
        Assert.assertTrue(TStrings
                .isSubString(
                        "qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we",
                        "b6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qrqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4web6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qr"));
    }
    
    @Test
    public void testIsSubString(){
        String needle = Variables.register("needle", TestData.getString());
        String haystack = Variables.register("haystack", TestData.getString());
        Assert.assertEquals(haystack.contains(needle), TStrings.isSubString(needle, haystack));
    }

}
