package org.cinchapi.concourse.server.http;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;

/**
  * Given two arguments, figure out which is the key and which is the record.
  * This method returns an array where the first element is the key and the
  * second is the record.
  * 
  * @param arg1
  * @param arg2
  * @return an array with the key followed by the record
  * @author dubex
**/
public class KeyRecordArgs {
    private Long record;
    private String key;
    private List<Long> records;
    private List<String> keys;

    /**
     * 
     * @param objects
     */
    public static KeyRecordArgs parse(Object... objects) {
        KeyRecordArgs kra = new KeyRecordArgs();
        for (Object obj : objects) {
            if(obj instanceof List) {
                List<?> list = (List<?>) obj;
                String temp = (String) Iterables.getFirst(list, null);
                if(list.size() > 1) {
                    if(Longs.tryParse(temp) == null) {
                       kra.keys = (List<String>) obj;
                    }
                    else {
                        kra.records = (List<Long>) obj;
                    }
                }
                else if(list.size() == 1) {
                    if(Longs.tryParse(temp) == null) {
                        kra.key = (String) obj;
                    }
                    else {
                        kra.record = (Long) obj;
                    }
                }
            }
            else if(obj instanceof String) {
                kra.record = Longs.tryParse((String) obj);
                if(kra.record == null) {
                    kra.key = (String) obj;
                }
            }
        }
        return kra;
    }

    public Long getRecord() {
        return this.record;
    }

    public String getKey() {
        return this.key;
    }

    public List<Long> getRecords() {
        return this.records;
    }

    public List<String> getKeys() {
        return this.keys;
    }

}
