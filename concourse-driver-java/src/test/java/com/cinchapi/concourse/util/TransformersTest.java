package com.cinchapi.concourse.util;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.google.common.base.Function;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link Transformers} class.
 * 
 * @author chandresh.pancholi
 */
public class TransformersTest extends ConcourseBaseTest {

    @Test
    public void testLazyTransformSet() {
        int count = Random.getScaleCount();
        Set<String> original = Sets.newHashSet();
        for (int i = 0; i < count; ++i) {
            original.add(Random.getString());
        }

        Function<String, String> function = new Function<String, String>() {

            @Override
            public String apply(String input) {
                return StringUtils.reverse(input);
            }
        };

        Assert.assertEquals(Transformers.transformSet(original, function),
                Transformers.transformSetLazily(original, function));
    }

}
