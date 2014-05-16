/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package hazelcastHRU.mapreduce;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import hazelcastHRU.hru.HRU;

import java.util.StringTokenizer;

public class HRUMapper
        implements Mapper<Integer, HRU, String, Double> {

    private static final String LOW = "LOW";
    private static final String MED = "MED";
    private static final String HIGH = "HIGH";

    @Override
    public void map(Integer key, HRU hru, Context<String, Double> context) {
        
        if(hru.slope<30.0){
            context.emit(LOW, hru.slope);
        }
        else if(hru.slope<60.0){
            context.emit(MED, hru.slope);
        }
        else{
            context.emit(HIGH, hru.slope);
        }
    }

}