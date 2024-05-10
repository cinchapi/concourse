/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.search;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for {@link Infingram Infingrams}.
 *
 * @author Jeff Nelson
 */
public class InfingramTest {

    @Test
    public void foo() {
        Infingram needle = new Infingram("complex the complex");
        Assert.assertTrue(needle.in("complex simplethesimple complex"));
    }

    @Test
    public void testInEmptyString() {
        Infingram infingram = new Infingram("");
        Assert.assertFalse(infingram.in("foo bar"));
    }

    @Test
    public void testNumTokens() {
        Infingram infingram = new Infingram("foo bar baz");
        Assert.assertEquals(3, infingram.numTokens());
    }

    @Test
    public void testNumTokensEmpty() {
        Infingram infingram = new Infingram("");
        Assert.assertEquals(0, infingram.numTokens());
    }

    @Test
    public void testInRandomNonMatch() {
        String haystack = Random.getString();
        String needle = Random.getString();
        while (haystack.contains(needle)) {
            needle = Random.getString();
        }
        Infingram infingram = new Infingram(needle);
        Assert.assertFalse(infingram.in(haystack));
    }

    @Test
    public void testInEmptyHaystack() {
        String needle = Random.getSimpleString();
        Infingram infingram = new Infingram(needle);
        Assert.assertFalse(infingram.in(""));
    }

    @Test
    public void testInExactMatch() {
        Infingram infingram = new Infingram(
                "adventurous explorer discovers hidden");
        Assert.assertTrue(infingram
                .in("The adventurous explorer discovers hidden treasures"));
    }

    @Test
    public void testInPrefixMatch() {
        Infingram infingram = new Infingram("advent explo disco hidd");
        Assert.assertTrue(infingram
                .in("The adventurous explorer discovers hidden treasures"));
    }

    @Test
    public void testInInfixMatch() {
        Infingram infingram = new Infingram("ventu plore scove dden");
        Assert.assertTrue(infingram
                .in("The adventurous explorer discovers hidden treasures"));
    }

    @Test
    public void testInPrefixInfixMatch() {
        Infingram infingram = new Infingram("adv exp dis hid");
        Assert.assertTrue(infingram
                .in("The adventurous explorer discovers hidden treasures"));
    }

    @Test
    public void testInScatteredMatch() {
        Infingram infingram = new Infingram(
                "adventurous discovers explorer hidden");
        Assert.assertFalse(infingram
                .in("The adventurous explorer discovers hidden treasures"));
    }

    @Test
    public void testInNoMatch() {
        Infingram infingram = new Infingram("hidden explorer treasures");
        Assert.assertFalse(infingram.in("The adventurous discoverer"));
    }

    @Test
    public void testInMultiplePhrases() {
        Infingram infingram = new Infingram(
                "fearless adventurer explores uncharted regions");
        Assert.assertTrue(infingram.in(
                "The fearless adventurer explores uncharted regions with determination"));
    }

    @Test
    public void testInPartialPhraseMatch() {
        Infingram infingram = new Infingram("advent explo disco hidd treas");
        Assert.assertTrue(infingram
                .in("The adventurous explorer discovers hidden treasures"));
    }

    @Test
    public void testInNonContiguousMatch() {
        Infingram infingram = new Infingram("majestic eagle soars");
        Assert.assertFalse(infingram
                .in("The majestic eagle flies high and soars through the sky"));
    }

    @Test
    public void testInCaseInsensitiveMatch() {
        Infingram infingram = new Infingram(
                "harmony and tranquility serenity reigns");
        Assert.assertTrue(infingram.in(
                "In the garden of harmony and tranquility, serenity reigns supreme"));
    }

    @Test
    public void testInExactMatchWithStopwords() {
        Infingram infingram = new Infingram(
                "mysterious ancient artifact discovered remote location");
        Assert.assertFalse(infingram.in(
                "mysterious ancient artifact was discovered in a remote location"));
    }

    @Test
    public void testInExactMatchWithStopwordsInBoth() {
        Infingram infingram = new Infingram(
                "mysterious ancient artifact discovered in a remote location");
        Assert.assertFalse(infingram.in(
                "mysterious ancient artifact was discovered in a remote location"));
    }

    @Test
    public void testInPrefixMatchWithStopwords() {
        Infingram infingram = new Infingram("scie break unlo secr to univ");
        Assert.assertTrue(infingram
                .in("scientists breakthrough unlocks secrets to universe"));
    }

    @Test
    public void testInInfixMatchWithStopwords() {
        Infingram infingram = new Infingram("ste crim mast elud auth f dec");
        Assert.assertTrue(infingram.in(
                "master criminal mastermind eludes authorities for decades"));
    }

    @Test
    public void testInPrefixInfixMatchWithStopwords() {
        Infingram infingram = new Infingram(
                "ground rese prov evid of anci civi");
        Assert.assertTrue(infingram.in(
                "groundbreaking research provides evidence of ancient civilization"));
    }

    @Test
    public void testInNoMatchWithStopwords() {
        Infingram infingram = new Infingram(
                "innovative technology revolutionizes healthcare industry");
        Assert.assertFalse(infingram.in(
                "cutting edge advancements transform the education sector"));
    }

    @Test
    public void testInMultiplePhrasesWithStopwords() {
        Infingram infingram = new Infingram(
                "renowned chef creates culinary masterpiece the");
        Assert.assertFalse(infingram.in(
                "renowned chef creates a culinary masterpiece that leaves diners awestruck"));
    }

    @Test
    public void testInPartialPhraseMatchWithStopwords() {
        Infingram infingram = new Infingram("natu wond insp art cen");
        Assert.assertTrue(
                infingram.in("natural wonders inspire artists centuries"));
    }

    @Test
    public void testInNonContiguousMatchWithStopword() {
        Infingram infingram = new Infingram(
                "historic landmark attracts tourists worldwide");
        Assert.assertFalse(
                infingram.in("historic site worldwide attracts tourists"));
    }

    @Test
    public void testInCaseInsensitiveMatchWithStopwords() {
        Infingram infingram = new Infingram(
                "Dedicated Scientists Unravel COMPLEX Mysteries".toLowerCase());
        Assert.assertTrue(infingram
                .in("dedicated scientists unravel coMPlex mysteries nature"
                        .toLowerCase()));
    }

    @Test
    public void testInEmptyInfingram() {
        Infingram infingram = new Infingram("");
        Assert.assertFalse(infingram
                .in("rare astronomical event captivates stargazers worldwide"));
    }

    @Test
    public void testInSingleTokenInfingram() {
        Infingram infingram = new Infingram("phenomenon");
        Assert.assertTrue(
                infingram.in("rare phenomenon baffles scientists worldwide"));
    }

    @Test
    public void testInStopwordsOnlyInfingram() {
        Infingram infingram = new Infingram("the and is of");
        Assert.assertFalse(infingram.in(
                "controversial theory sparks intense debate scientific community"));
    }

    @Test
    public void testReproA() {
        Infingram infingram = new Infingram("w  8");
        Assert.assertFalse(infingram.in(
                "uo0qgmr6r66mfuligawh08f33ce63uubwuaue186r6x0g9bwwqg9c4wooctgu72a5kksbepajevzkfpjny2osj6pu0ryk3o"));
    }

}
