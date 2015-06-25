package ambience.order.n;

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
 * calculate PAI(Phenotype-associated information) and 
 * KWII(K-way interaction information) of combinations of geno type and pheno type
 * @author Chao Feng
 */

public class AmbienceN_b_cal_PAIandKWII {
	
	public static class Mapper_b extends Mapper<Object, Text, TextPair, Text> {	
		
		private int order;
		private Text LeftSub = new Text();
		private Text RightSub = new Text();
		private TextPair tp = new TextPair();
		private Text entropy = new Text();
		
		protected void setup(Context context) throws IOException,InterruptedException {
			order = Integer.parseInt(context.getConfiguration().get("order"));
		}
		
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
					if (NthComma == order){
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
		private List<String> V = new LinkedList<String>();//put key
		private HashMap<String, Double> VE = new HashMap<String, Double>();//put <key,Entropy>
//		private HashMap<String, Double> PV = new HashMap<String, Double>();//put <key,PAI>
//		private HashMap<String, Double> KV = new HashMap<String, Double>();//put <key,KWII>
		
		private int order;
		
		public void reduce(TextPair key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			String keystring = key.getFirst().toString() + key.getSecond().toString();
			for (Text value : values){
				V.add(keystring);
				VE.put(keystring, Double.valueOf(value.toString()));
			}
		}
		
		
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			
			Double enV, enVP;
			Double enP;	
			//First need to know the value of order.
			order = Integer.parseInt(context.getConfiguration().get("order"));
			//number of sub set.
			int nSubsetTerms = (int) (Math.pow(2, order)-2);
			
			Double PAI;	
			Double KWII;
			
			//calculate PAI value of current order.
//			System.out.println("V.size = " + V.size());
//			System.out.println("V:"+V);
			for (int i = 0; i < V.size()-1; i = i+3+nSubsetTerms*2) {
//				System.out.println("i=" + i);
				enV = VE.get(V.get(i));
				enVP = VE.get(V.get(i+1));
				enP = VE.get(V.get(i+2));
				PAI = enV + enP - enVP;
				
				KWII = enV - enVP + Math.pow((-1),(order-1))*enP;		
				for(int j=i+3;j<i+3+nSubsetTerms*2; j=j+2){
					String preString = V.get(i+2)+",";
					String[] subSetElement=V.get(j).substring(preString.length()).split(",");
					if(subSetElement.length%2 == 0){
						KWII = KWII + Math.pow((-1),order)*VE.get(V.get(j)) + Math.pow((-1),(order-1))*VE.get(V.get(j+1));
					}else{
						KWII = KWII + Math.pow((-1),(order-1))*VE.get(V.get(j)) + Math.pow((-1),order)*VE.get(V.get(j+1));
					}
				}	
				StringBuilder PAIandKWII = new StringBuilder();
				PAIandKWII.append(PAI).append("\t").append(KWII);
				context.write(new Text(V.get(i)), new Text(PAIandKWII.toString()));
//				PV.put(V.get(i), PAI);
//				KV.put(V.get(i), KWII);
			}
			
			V.clear();	//for correcting java heap space error
			VE.clear(); //for correcting java heap space error
		}
	}
}
