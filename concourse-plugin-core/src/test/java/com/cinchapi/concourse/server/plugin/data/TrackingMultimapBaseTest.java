package com.cinchapi.concourse.server.plugin.data;

import com.cinchapi.concourse.test.ConcourseBaseTest;

public class TrackingMultimapBaseTest extends ConcourseBaseTest {
    
    protected TrackingMultimap<? super Object, ? super Object> map;

    @Override
    public void beforeEachTest() {
        map = TrackingLinkedHashMultimap.create();
    }

    @Override
    public void afterEachTest() {
        map = null;
    }

}
