/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class Point implements Comparable<Point> {

    public static float MIN_LATITUDE = Float.valueOf("-90.0000");
    public static float MAX_LATITUDE = Float.valueOf("90.0000");
    public static float MIN_LONGITUDE = Float.valueOf("-180.0000");
    public static float MAX_LONGITUDE = Float.valueOf("180.0000");

    private float latitude;
    private float longitude;

    public Point(float latitude, float longitude) {
        if(isValidLatitude(latitude) && isValidLongitude(longitude)) {
            this.latitude = latitude;
            this.longitude = longitude;
        }
        else {
            throw new IllegalArgumentException(
                    "The specified latitude and longitude contain inproper values, please respect min, max values.");
        }
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

    public static boolean isValidLatitude(float latitude) {
        return latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE;
    }

    public static boolean isValidLongitude(float longitude) {
        return longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE;
    }

    @Override
    public int compareTo(Point o) {
        // TODO: Fix
        return getLatitude() == o.getLatitude()
                && getLongitude() == o.getLongitude() ? 0 : 1;
    }

}
