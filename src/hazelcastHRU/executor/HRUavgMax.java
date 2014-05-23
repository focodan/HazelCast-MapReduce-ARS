package hazelcastHRU.executor;

import com.hazelcast.config.Config;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import hazelcastHRU.hru.HRU;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 * @author daniel.elliott
 */
public class HRUavgMax implements Callable<String[]>, Serializable {

    private final String level;

    private HRUavgMax(){
        level = null;

    }

    public HRUavgMax(String level) {
        this.level = level;

    }

    @Override
    public String[] call() {
        ClientConfig cfg = new ClientConfig();
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(cfg);

        MultiMap<String,HRU> hrusMap  = hz.getMultiMap("HRUs");
        Collection<HRU> hrus = hrusMap.get(level);
        
        
        double max=0;
        double sum=0;
        int count=0;
        for(HRU h : hrus){
            sum += h.slope;
            count++;
            if(max < h.slope) max = h.slope;
        }

        return new String[] {level,(new Double(sum/count)).toString(),new Double(max).toString()};
    }
}
