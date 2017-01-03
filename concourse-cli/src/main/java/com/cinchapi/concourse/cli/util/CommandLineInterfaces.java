/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.cli.util;

import java.text.DecimalFormat;

import com.google.common.base.Preconditions;

/**
 * Utils for command line interfaces.
 * 
 * @author Jeff Nelson
 */
public final class CommandLineInterfaces {

    /**
     * Render a progress bar that shows something is {@code percent} done.
     * 
     * @param percent the percent done
     * @return the progress bar to print
     */
    public static String renderPercentDone(double percent) {
        Preconditions.checkArgument(percent >= 0 && percent <= 100);
        DecimalFormat format = new DecimalFormat("##0.00");
        StringBuilder sb = new StringBuilder();
        sb.append(format.format(percent)).append("% ");
        sb.append("[");
        for (int i = 0; i < percent; ++i) {
            sb.append("=");
        }
        sb.append(">");
        for (int i = (int) Math.ceil(percent); i < 100; ++i) {
            sb.append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    private CommandLineInterfaces() {/* no-op */}

}
