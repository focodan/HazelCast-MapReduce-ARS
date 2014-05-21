/*
 * $Id:$
 * 
 * Copyright 2007-2011, Olaf David, Colorado State University
 *
 * This file is part of the Object Modeling System OMS.
 *
 * OMS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * any later version.
 *
 * OMS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with OMS.  If not, see <http://www.gnu.org/licenses/>.
 */
package hzMapRedHRU;


import hazelcastHRU.driver.MapReduceDriverOMS;
import hazelcastHRU.node.SimpleNode;
import oms3.annotations.*;

/**
 *
 * @author od, daniel.elliott
 */
public class Component {

    @Role(Role.PARAMETER)
    @In public Integer NUM_HRU;
    
    @Role(Role.PARAMETER)
    @In public Integer MIN_C_SIZE;

    @Execute
    public void run() {
        // contains the mapreduce job and 1 node to execute the job
        MapReduceDriverOMS driver = new MapReduceDriverOMS(MIN_C_SIZE, NUM_HRU);
        SimpleNode[] additionalNodes; // contains additional nodes on the cluster
        
        if(MIN_C_SIZE > 1){ // initialize any additional nodes
            additionalNodes = new SimpleNode[MIN_C_SIZE-1];
            for(int i=0; i<MIN_C_SIZE-1;i++){
                additionalNodes[i] = new SimpleNode();
                additionalNodes[i].run("HRU"); // HRU is our cluster ID
            }
        }
        
        // run the mapreduce job
        try{
            driver.run("HRU");
        }
        catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("done!");
    }
}
