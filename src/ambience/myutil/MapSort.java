package ambience.myutil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
*Sort the <combinations, PAI-KWII> key-value pairs by PAI-KWII value.
*@author:  Chao Feng
*/

public class MapSort {
	static public LinkedHashMap<String, Double> sortHashMapByValuesD(HashMap<String, Double> passedMap) {
		//get all the keys from PV
		List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
		//get all the values from PV
		List<Double> mapValues = new ArrayList<Double>(passedMap.values());
		//sort key from min to max
		Collections.sort(mapValues);
		//sort values from min to max
		Collections.sort(mapKeys);
		//reverse the order(max to min)
		Collections.reverse(mapValues);
		//reverse the order(max to min)
		Collections.reverse(mapKeys);
		
		LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();

		Iterator<Double> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			Object val = valueIt.next();
			Iterator<String> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				Object key = keyIt.next();
				String comp1 = passedMap.get(key).toString();
				String comp2 = val.toString();
				//if key's PAI equals to the PAI in the mapVlues
				if (comp1.equals(comp2)) {
					//remove the key:value pair with duplication
					passedMap.remove(key);
					//remove the key with duplication
					mapKeys.remove(key);
					//store final result:(com,PAI) and PAI could be duplicated.
					sortedMap.put((String) key, (Double) val);
					break;
				}
			}
		}
		return sortedMap;
	}
}
