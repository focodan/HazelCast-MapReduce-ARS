/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hazelcastHRU.driver;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiMap;
import hazelcastHRU.executor.HRUavgMax;
import hazelcastHRU.hru.HRU;
/**
 *
 * @author daniel.elliott
 */
public class ExecutorDriver {
    public static final int NUM_HRU = 100;
    
    public static void main(String[] args){
        ExecutorDriver d = new ExecutorDriver();
        try{
            d.executeHRUAvgMax();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
        
     void executeHRUAvgMax() throws Exception {
        
        Config cfg = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IExecutorService es = hz.getExecutorService("default");
        
        fillMapWithData(hz);
        
        es.submit(new HRUavgMax("LOW"),buildCallback());
        es.submit(new HRUavgMax("MED"),buildCallback());
        es.submit(new HRUavgMax("HIGH"),buildCallback());

    }
        
        private void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {
        MultiMap<String, HRU> map = hazelcastInstance.getMultiMap("HRUs");
        for (int i = 1; i <= NUM_HRU; i++) {
            HRU tmp = new HRU();
            tmp.ID = i;
            tmp.slope = (Math.random() * (90)); // generate in range [0,90]
            String level;

            if (tmp.slope <= 30) {
                level = "LOW";
            } else if (tmp.slope <= 60) {
                level = "MED";
            } else {
                level = "HIGH";
            }
            map.put(level, tmp);
        }
    }
        
        private ExecutionCallback<String[]> buildCallback() {
        return new ExecutionCallback<String[]>() {
            @Override
            public void onResponse(String[] res) {
                System.out.println(res[0]+" "+res[1]+" "+res[2]);
            }

            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        };
    }
}
