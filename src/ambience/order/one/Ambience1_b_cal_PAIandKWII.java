package ambience.order.one;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import ambience.myutil.TextPair;

/**
 * calculate PAI(phenotype-associated information) and 
 * KWII(K-way Interaction Information) of combinations of geno type and pheno type
 * @author Chao Feng
 */

public class Ambience1_b_cal_PAIandKWII {
	
	public static class Mapper_b extends Mapper<Object, Text, TextPair, Text> {	
		private Text LeftSub = new Text();
		private Text RightSub = new Text();
		private TextPair tp = new TextPair();
		private Text entropy = new Text();
		
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			// receive one line of the input
			String line = value.toString();
			String combinations = line.split("\t")[0];
			String en = line.split("\t")[1];
			
			String L = null;
			String R = null;
			int NthComma = 0;
			for (int i = 0; i < combinations.length(); i++){
				if(combinations.substring(i, i+1).equals(",")){
					NthComma++;
					if (NthComma == 1){
						L = combinations.substring(0,i);
						R = combinations.substring(i, combinations.length());
						break;
					}
				}
			}
//			System.out.println("L = " + L);
//			System.out.println("R = " + R);
			LeftSub.set(new Text(L));
			RightSub.set(new Text(R));
			
			tp.set(LeftSub, RightSub);
			entropy.set(en);	
			context.write(tp, entropy);
//			System.out.println("tp=" + tp +"   ---  entropy=" + entropy);
		}
	}
	
	public static class CustomPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair arg0, Text arg1, int numReduceTasks) {
			return (arg0.getFirst().hashCode() & Integer.MAX_VALUE)% numReduceTasks;
		}
	}

	public static class Reducer_b extends Reducer<TextPair, Text, Text, Text> {
		private List<String> V = new LinkedList<String>();//variables
		private HashMap<String, Double> VE = new HashMap<String, Double>();//variables:entropy

		public void reduce(TextPair key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			String keystring = key.getFirst().toString() + key.getSecond().toString();
			for (Text value : values){
				V.add(keystring);
				VE.put(keystring, Double.valueOf(value.toString()));
			}
		}

		public void cleanup(Context context) throws IOException,InterruptedException {
			// calculate and accumulate PAI(KWII,when 1 order) value
			Double enV, enVP;
			Double enP;
		
			Double PAI;			
			for (int i = 0; i < V.size() - 1; i = i + 3) {
				enV = VE.get(V.get(i));
				enVP = VE.get(V.get(i+1));
				enP = VE.get(V.get(i+2));
			
				PAI = enV + enP - enVP;
				context.write(new Text(V.get(i)), new Text(String.valueOf(PAI)));
			}
		}
	}
}
