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
public final class Point {

    /**
     * Valid latitude values are between -90 and 90, both inclusive.
     */
    public static float MIN_Y = Float.valueOf("-90.0000");
    public static float MAX_Y = Float.valueOf("90.0000");

    /**
     * Valid longitude values are between -180 and 180, both inclusive.
     */
    public static float MIN_X = Float.valueOf("-180.0000");
    public static float MAX_X = Float.valueOf("180.0000");

    private float x;
    private float y;

    public Point(float x, float y) {
        if(isValidY(x) && isValidX(y)) {
            this.x = x;
            this.y = y;
        }
        else {
            throw new IllegalArgumentException(
                    "The specified x and y contain inproper values, please respect min, max values.");
        }
    }

    public static Point to(float x, float y) {
        return new Point(x, y);
    }

    public float getY() {
        return y;
    }

    public float getX() {
        return x;
    }

    public static boolean isValidY(float latitude) {
        return latitude >= MIN_Y && latitude <= MAX_Y;
    }

    public static boolean isValidX(float longitude) {
        return longitude >= MIN_X && longitude <= MAX_X;
    }

}
