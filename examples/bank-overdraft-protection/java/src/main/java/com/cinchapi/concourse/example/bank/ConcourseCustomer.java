/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.cinchapi.concourse.example.bank;

import com.cinchapi.concourse.Concourse;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * An implementation of the {@link Customer} interface that uses Concourse for
 * data storage.
 * 
 * @author Jeff Nelson
 */
public class ConcourseCustomer implements Customer {

    // NOTE: For the purpose of this example, we don't store any data about the
    // Customer locally so that we can illustrate how you would query Concourse
    // to get the data. In a real application, you always want to cache
    // repeatedly used data locally.

    /**
     * Since Concourse does not have tables, we store a special key in each
     * record to indicate the class to which the record/object belongs. This
     * isn't necessary, but it helps to ensure logical consistency between the
     * application and the database.
     */
    private final static String CLASS_KEY_NAME = "_class";

    /**
     * The id of the Concourse record that holds the data for an instance of
     * this class.
     */
    private final long id;

    /**
     * This constructor creates a new record in Concourse and inserts the data
     * expressed in the parameters.
     * 
     * @param firstName the customer's first name
     * @param lastName the customer's last name
     */
    public ConcourseCustomer(String firstName, String lastName) {
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            Multimap<String, Object> data = HashMultimap.create();
            data.put(CLASS_KEY_NAME, getClass().getName());
            data.put("first_name", firstName);
            data.put("last_name", lastName);
            this.id = concourse.insert(data);
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    /**
     * This constructor loads an existing object from Concourse.
     * 
     * @param id the id of the record that holds the data for the object we want
     *            to load
     */
    public ConcourseCustomer(long id) {
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            Preconditions.checkArgument(getClass().getName().equals(
                    concourse.get(CLASS_KEY_NAME, id)));
            this.id = id;
            // NOTE: If this were a real application, it would be a god idea to
            // preload frequently used attributes here and cache them locally
            // (or maybe even in an external cache like Memcache or Redis).
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    @Override
    public long getId() {
        return id;
    }
    
    @Override
    public String toString(){
        return getClass().getName() + " "+ id;
    }

}
