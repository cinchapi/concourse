package com.cinchapi.concourse.exporter.interactors;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.exporter.helpers.Helper;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Exporter {
    private final Concourse concourse;
    private final boolean showPrimaryKey;

    public Exporter(Concourse concourse, boolean showPrimaryKey) {
        this.concourse = concourse;
        this.showPrimaryKey = showPrimaryKey;
    }

    public String perform() {
        return format(concourse.select(concourse.inventory()));
    }

    public String perform(List<Long> records) {
        return format(concourse.select(records));
    }

    public String perform(String ccl) {
        return format(concourse.select(ccl));
    }

    public String perform(List<Long> records, String ccl) {
        return format(Helper.filter(concourse.select(ccl), (k,v) ->
                records.contains(k)));
    }

    private String format(Map<Long, Map<String, Set<Object>>> keyedRecords) {
        final Iterable<Map<String, Set<Object>>> records =
                getRecords(keyedRecords);
    }

    private Iterable<Map<String, Set<Object>>> getRecords(
            Map<Long, Map<String, Set<Object>>> keyedRecords
    ) {
        return Helper.map(keyedRecords, (id, xs) ->
                Helper.toMap(Helper.map(xs, (k, v) -> Helper.tuple(
                        showPrimaryKey ? id.toString() + "," + k : k,
                        v))));
    }
}
