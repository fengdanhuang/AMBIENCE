package ambience.order.one;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ambience.myutil.MapSort;

/**
 * Sort <Combinations, PAI-KWII> key-value pairs based on PAI(phenotype-associated information) value
 * @author Chao Feng
 */

public class Ambience1_c_org_PAIandKWII {
	
	public static class Mapper_c extends Mapper<Object, Text, Text, Text> {
		private Text LeftSub = new Text();
//		private Text RightSub = new Text();
		private Text PAI = new Text();
		
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			// receive one line of the input
			String line = value.toString(); 
			String combinations = line.split("\t")[0];
			String P = line.split("\t")[1];
			
			String L = null;
//			String R = null;
			int NthComma = 0;
			for (int i = 0; i < combinations.length(); i++){
				if(combinations.substring(i, i+1).equals(",")){
					NthComma++;
					if (NthComma == 1){
						L = combinations.substring(0,i);
//						R = combinations.substring(i, combinations.length());
						break;
					}
				}
			}
//			System.out.println("L = " + L);
			LeftSub.set(new Text(L));
//			RightSub.set(new Text(R));
			
			PAI.set(P);
//			System.out.println("LeftSub=" + LeftSub + "   PAI=" + PAI);
			context.write(LeftSub, PAI);
		}
	}
	
	public static class Reducer_c extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, Double> PV = new HashMap<String, Double>();//PAI:variables

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for (Text value : values) {
				PV.put(key.toString(), Double.valueOf(value.toString()));
			}
		}

		public void cleanup(Context context) throws IOException,InterruptedException {
			
			LinkedHashMap<String, Double> sortedPV = MapSort.sortHashMapByValuesD(PV);//Sort by PAI value.
			Iterator<Entry<String, Double>> iterator = sortedPV.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String,Double> entry = iterator.next();
				String value = entry.getValue() + "\t" + entry.getValue();
				context.write(new Text((String) entry.getKey()),new Text(value));
			}
		}
	}
}
