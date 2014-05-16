package hazelcastHRU.hru;

import java.io.Serializable;

/**
 * A very simplified representation of an HRU.
 * @author daniel.elliott
 */
public class HRU implements Serializable {
    public int ID; // unique identifier
    public double slope; // angle-measure in degrees. Range of values: [0,90]
}