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

package hazelcastHRU.driver;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcastHRU.hru.HRU;
import hazelcastHRU.mapreduce.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Map;


/**
 * Example modified by Dan Elliott
 * 
 * A basic and simple MapReduce demo application for the Hazelcast MR framework.
 * The example Lorem Ipsum texts were created by this awesome generator: http://www.lipsum.com/
 *
 * For any further questions feel free
 * - to ask at the mailing list: https://groups.google.com/forum/#!forum/hazelcast
 * - read the Javadoc: http://hazelcast.org/docs/latest/javadoc/
 * - read the documentation this demo is for: http://bit.ly/1nQSxhH
 */
public class MapReduceDriver {
    private static final int NUM_HRU = 2; // How many test HRUs we'll generate

    // Commandline argument (optional): <size> to set the minimum number of nodes on this cluster.
    // If no argument is specified, the mapreduce job will run locally 
    public static void main(String[] args) throws Exception {
        Config config = new Config(); 
        config.getGroupConfig().setName("HRU"); //Nodes which identify as "HRU" are in this same cluster
        
        // set a minimum number of nodes in the cluster before the mapreduce job can begin
        if(args.length >= 1){
            try{
                config.setProperty("hazelcast.initial.min.cluster.size", args[0] );
            }catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
        
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        System.out.println("This node's name is:"+hazelcastInstance.getCluster());
        
        try {
            fillMapWithData(hazelcastInstance);

            Map<String, Double[]> slopeAvgs = mapReduce(hazelcastInstance);
            
            for (Map.Entry<String, Double[]> entry : slopeAvgs.entrySet()) {
                System.out.println("\tSlope type'" + entry.getKey() + "' has average " + entry.getValue()[0] + " angle, and max of "+entry.getValue()[1]);
            }
        } finally {
            //destroy shared data we've created here
            //hazelcastInstance.getMap("articles").destroy();
            Hazelcast.shutdownAll();
        }
    }

    private static Map<String, Double[]> mapReduce(HazelcastInstance hazelcastInstance) throws Exception {

        // Retrieving the JobTracker by name
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");

        // Creating the KeyValueSource for a Hazelcast IMap
        IMap<Integer, HRU> map = hazelcastInstance.getMap("articles");
        KeyValueSource<Integer, HRU> source = KeyValueSource.fromMap(map);

        Job<Integer, HRU> job = jobTracker.newJob(source);

        // Creating a new Job
        ICompletableFuture<Map<String, Double[]>> future = job // returned future
                .mapper(new HRUMapper())             // adding a mapper
                .reducer(new HRUReducerFactory())    // adding a reducer through the factory
                .submit();                                 // submit the task

        // Attach a callback listener
        future.andThen(buildCallback());

        // Wait and retrieve the result
        return future.get();
    }

    private static ExecutionCallback<Map<String, Double[]>> buildCallback() {
        return new ExecutionCallback<Map<String, Double[]>>() {
            @Override
            public void onResponse(Map<String, Double[]> stringLongMap) {
                System.out.println("Calculation finished! :)");
            }

            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        };
    }

    private static void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {
        IMap<Integer, HRU> map = hazelcastInstance.getMap("articles");
        for(int i=1;i<=NUM_HRU;i++){
            HRU tmp = new HRU();
            tmp.ID = i;
            tmp.slope = (Math.random() * (90)); // generate in range [0,90]
            map.put(new Integer(i), tmp);
        }
    }

}
