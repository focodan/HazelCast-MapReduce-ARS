package hazelcastHRU.node;

/**
 * This will create a hazelcast node that will join the cluster "HRU".
 * Once a member of the cluster, this node will implicitly store cluster data
 * as well as help execute mapreduce tasks of the cluster.
 * @author daniel.elliott
 */
import com.hazelcast.core.*;
import com.hazelcast.config.*;

 
public class SimpleNode {
    
    public void run(){
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(new Config());
    }
    
    public void run(String groupID){
        Config config = new Config();
        config.getGroupConfig().setName(groupID);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
    }
    
    public static void main(String[] args) {
        Config config = new Config();
        config.getGroupConfig().setName("HRU");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        System.out.println("This node has ID:"+instance.getCluster());
    }
}