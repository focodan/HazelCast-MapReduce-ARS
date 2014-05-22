package hazelcastHRU.executor;

import com.hazelcast.core.PartitionAware;
import java.io.Serializable;

/**
 *
 * @author daniel.elliott
 */
public class HRUkey implements Serializable, PartitionAware {
    private final String level; // Whether the HRU angle is LOW, MED, or HIGH
    private final int id; // Unique identifier of HRU
    
    public HRUkey(String level, int id){
        this.level = level;
        this.id = id;
    }
    
    public int getID(){
        return id;
    }
    
    public String getLevel(){
        return level; // String is immutable, so this isn't bad reference escape
    }
    
    @Override
    public Object getPartitionKey(){
        return level;
    }
    
    //TODO add .equals and .toString methods
}
