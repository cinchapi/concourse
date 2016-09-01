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
package com.cinchapi.concourse.plugin.data;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.Maps;

public class ObjectDataset extends AbstractDataset<Object> {

    private static final long serialVersionUID = -3574802163004190565L;
    
    private Dataset<Long, String, TObject> tdataset;
    
    public ObjectDataset(Dataset<Long, String, TObject> tdataset) {
        tdataset = this.tdataset;
    }
    
    public Set<Object> get(Long entity, String attribute) {
        Set<TObject> tobjects = tdataset.get(entity, attribute);
        return Convert.thriftSetToJava(tobjects);
    }
    
    public Map<String, Set<Object>> get(Object entity) {
        Map<String, Set<TObject>> tmap = tdataset.get(entity);
        Map<String, Set<Object>> omap = Maps.newHashMap();
        for(Entry<String, Set<TObject>> entry : tmap.entrySet()) {
            omap.put(entry.getKey(), Convert.thriftSetToJava(entry.getValue()));
        }
        return omap;
    }

}
