package com.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class Point implements Comparable<Point> {
    
    private float latitude;
    private float longitude;
    
    public Point(float latitude, float longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }
    
    public static Point to(float latitude, float longitude) {
        return new Point(latitude, longitude);
    }
    
    public float getLatitude() {
        return latitude;
    }
    
    public float getLongitude() {
        return longitude;
    }
    
    @Override
    public int compareTo(Point o) {
        //TODO: Fix
        return getLatitude() == o.getLatitude() && getLongitude() == o.getLongitude() ? 0 : 1;
    }

}
