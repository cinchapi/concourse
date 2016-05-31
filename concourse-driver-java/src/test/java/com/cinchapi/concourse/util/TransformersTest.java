package com.cinchapi.concourse.util;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import com.google.common.base.Function;

public class TransformersTest {

    @Test
    public void testLazyTransformSet() {
        
        Set<String> original = Sets.newSet("John", "Jane", "Adam", "Tom");
        Function<String, String> function = new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input+"HI";
            }
        };
        
        Set<String> output = Transformers.transformSetLazily(original, function);
        Set<String> transformedSetOutput = Transformers.transformSet(original, function);
        
        Iterator<String> it1 = output.iterator();
        Iterator<String> it2 = transformedSetOutput.iterator();
        
        while(it1.hasNext() && it2.hasNext()) {
            assertTrue(it1.next().equals(it2.next()));
        }
    }
    
    
 }
