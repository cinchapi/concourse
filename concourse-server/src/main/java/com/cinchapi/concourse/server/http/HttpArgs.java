package com.cinchapi.concourse.server.http;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;

/**
 * Represents the arguments passed via an HTTP requests either in the request
 * body or the URL parameters.
 * 
 * @author dubex
 **/
public class HttpArgs {

    /**
     * Parse the objects into the appropriate arguments.
     * 
     * @param objects
     * @return the HttpArgs
     */
    @SuppressWarnings("unchecked")
    public static HttpArgs parse(Object... objects) {
        HttpArgs args = new HttpArgs();
        for (Object obj : objects) {
            if(obj == null) {
                continue;
            }
            else if(obj instanceof List) {
                List<?> list = (List<?>) obj;
                String temp = (String) Iterables.getFirst(list, null);
                if(list.size() > 1) {
                    if(Longs.tryParse(temp) == null) {
                        args.keys = (List<String>) obj;
                    }
                    else {
                        args.records = (List<Long>) obj;
                    }
                }
                else if(list.size() == 1) {
                    if(Longs.tryParse(temp) == null) {
                        args.key = (String) obj;
                    }
                    else {
                        args.record = (Long) obj;
                    }
                }
            }
            else if(obj instanceof String) {
                String str = (String) obj;
                Long record = Longs.tryParse(str);
                if(record != null){
                    args.record = record;
                }
                else{
                    args.key = str;
                }
            }
        }
        return args;
    }

    /**
     * The single key.
     */
    @Nullable
    private String key = null;

    /**
     * Multiple keys.
     */
    @Nullable
    private List<String> keys = null;

    /**
     * The single record.
     */
    @Nullable
    private Long record = null;

    /**
     * Multiple records.
     */
    @Nullable
    private List<Long> records = null;

    /**
     * Return the key arg, if it exists.
     * 
     * @return the key
     */
    @Nullable
    public String getKey() {
        return this.key;
    }

    /**
     * Return the keys arg, if it exists
     * 
     * @return the keys
     */
    public List<String> getKeys() {
        return this.keys;
    }

    /**
     * Return the record arg, if it exists.
     * 
     * @return the record
     */
    @Nullable
    public Long getRecord() {
        return this.record;
    }

    /**
     * Return the records arg, if it exists.
     * 
     * @return the records
     */
    public List<Long> getRecords() {
        return this.records;
    }

}
