/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.util.StringSplitter;
import com.cinchapi.concourse.util.Strings;

/**
 * A {@link PluginId} contains information to uniquely identify a plugin point.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class PluginId {

    /**
     * Parse a {@link PluginId} from a name. Fully qualified names
     * should be in the form of
     * (com|org|net).(company).(concourse?).(module).(...).ClassName to produce
     * a PluginId with the following properties:
     * <ul>
     * <li><strong>group-id:</strong> (com|org|net).company</li>
     * <li><strong>module:</strong> module</li>
     * <li><strong>cls:</strong> ClassName</li>
     * </ul>
     * 
     * @param name the name of the class for which the {@link PluginId} should
     *            be generated
     * @return a {@link PluginId} for the class {@code name}
     */
    public static PluginId forName(String name) {
        String group = null;
        String module = null;
        String cls = null;
        StringSplitter it = new StringSplitter(name, '.');
        String previous = null;
        String next = null;
        while (it.hasNext()) {
            previous = next;
            next = it.next();
            if(group == null) {
                group = next;
            }
            else if(group.equals("com") || group.equals("org") || group.equals("net")) {
                group += '.' + next;
            }
            else if(module == null) {
                if(next != null
                        && (next.equals("concourse") || (next.equals("plugin")
                                && previous != null && previous
                                    .equals("concourse")))) {
                    continue;
                }
                else {
                    module = next;
                }
            }
            else {
                cls = next;
            }
        }
        return new PluginId(group, module, cls);
    }

    /**
     * Parse a {@link PluginId} from a class name. Fully qualified class names
     * should be in the form of
     * (com|org|net).(company).(concourse?).(module).(...).ClassName to produce
     * a PluginId with the following properties:
     * <ul>
     * <li><strong>group-id:</strong> (com|org|net).company</li>
     * <li><strong>module:</strong> module</li>
     * <li><strong>cls:</strong> ClassName</li>
     * </ul>
     * 
     * @param clazz the {@link Class} object for which the {@link PluginId}
     *            should be generated
     * @return a {@link PluginId} for the {@code clazz}
     */
    public static PluginId forClass(Class<?> clazz) {
        return forName(clazz.getName());
    }

    /**
     * The top-level group for the plugin (i.e. com.cinchapi).
     */
    public final String group;

    /**
     * The module in which the plugin is housed (i.e. nlp)
     */
    public final String module;

    /**
     * The name of the class that contains the plugin methods (i.e.
     * TranslationEngine).
     */
    public final String cls;

    /**
     * Construct a new instance.
     * 
     * @param group
     * @param module
     * @param cls
     */
    private PluginId(String group, String module, String cls) {
        this.group = group;
        this.module = module;
        this.cls = cls;
    }

    @Override
    public String toString() {
        return Strings.join(':', group, module, cls);
    }

}
