package ambience.order.n;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ambience.myutil.CombinationSearcher;
import ambience.myutil.MapSort;

/**
 * Sort <Combinations, PAI-KWII> key-value pairs based on PAI(Phenotype-associated information) value
 * @author Chao Feng
 */

public class AmbienceN_c_org_PAIandKWII {
	
	public static class Mapper_c extends Mapper<Object, Text, Text, Text> {
	
		private int order;
		private Text LeftSub = new Text();
//		private Text RightSub = new Text();
		private Text PAIandKWII = new Text();
		
		protected void setup(Context context) throws IOException,InterruptedException {
			order = Integer.parseInt(context.getConfiguration().get("order"));
		}
		
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			// receive one line of the input
			String line = value.toString(); 
			String[] lineArray = new String [3];
			lineArray = line.split("\t");
			String combinations = lineArray[0];
			String PAI = lineArray[1];
			String KWII = lineArray[2];
			
			String L = null;
//			String R = null;
			int NthComma = 0;
			for (int i = 0; i < combinations.length(); i++){
				if(combinations.substring(i, i+1).equals(",")){
					NthComma++;
					if (NthComma == order){
						L = combinations.substring(0,i);
//						R = combinations.substring(i, combinations.length());
						break;
					}
				}
			}
//			System.out.println("L = " + L);
			LeftSub.set(new Text(L));
			
			StringBuilder PandK = new StringBuilder();
			PandK.append(PAI).append("\t").append(KWII);
			PAIandKWII.set(PandK.toString());
//			System.out.println("LeftSub=" + LeftSub + "   PAIandKWII=" + PAIandKWII.toString());
			context.write(LeftSub, PAIandKWII);
		}
	}
	
	public static class Reducer_c extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, Double> PV = new HashMap<String, Double>();//PAI:variables
		private HashMap<String, Double> KV = new HashMap<String, Double>();//put <key,KWII>

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for (Text value : values) {
				String PAIandKWII = value.toString();
				String [] PAIandKWIIArray = new String[2];
				PAIandKWIIArray = PAIandKWII.split("\t");
				String PAIStr = PAIandKWIIArray[0];
				String KWIIStr = PAIandKWIIArray[1];
//				System.out.println("PAIStr=" + PAIStr + ", KWIIStr=" + KWIIStr);
				PV.put(key.toString(), Double.valueOf(PAIStr.toString()));
				KV.put(key.toString(), Double.valueOf(KWIIStr.toString()));
			}
		}

		public void cleanup(Context context) throws IOException,InterruptedException {
			
			LinkedHashMap<String, Double> sortedPV = MapSort.sortHashMapByValuesD(PV);//Sort by PAI value.
			Iterator<Entry<String, Double>> iterator = sortedPV.entrySet().iterator();
			PV.clear(); //for correcting java heap space error
			
			//Note: cannot clear sortedPV because that would lead to "java.util.concurrentModificationException".
			//iterator is a independent thread with a mutex. It only establishes a linked index to the original object.
//			sortedPV.clear(); //for correcting java space error		
			while (iterator.hasNext()) {
				Entry<String,Double> entry = iterator.next();				
				//let the key(comb) become sorted comb, eg."2,1,3,4"->"1,2,3,4"
				String sortedComb = CombinationSearcher.sortComb((String) entry.getKey());			
				String value_PAI = entry.getValue().toString();
				String value_KWII = KV.get((String)entry.getKey()).toString();
				String value = value_PAI + "\t" + value_KWII;
				context.write(new Text(sortedComb),new Text(value));
			}
		}
	}
}
