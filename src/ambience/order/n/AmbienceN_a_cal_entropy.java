package ambience.order.n;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import ambience.myutil.CombinationSearcher;
import ambience.myutil.TextPair;
import ambience.myutil.Tools;

/**
 * calculate entropies of combinations of geno type and pheno type
 * @author Chao Feng
 */

public class AmbienceN_a_cal_entropy {
	
	public static class Mapper_a extends Mapper<Object, Text, TextPair, IntWritable> {
		
		private int order;
		private int theta;
		private Text star = new Text("*");// used for counting total counts of
		private TextPair tp = new TextPair();
		
		private int zero = 0; //represent null value after variable, convenient for later process. 
		private int large = 9999999; //represent phenotype
		private final static IntWritable one = new IntWritable(1);
		private List<String> s = new LinkedList<String>();//store top theta variables combinations.

		protected void setup(Context context) throws IOException,InterruptedException {

			FSDataInputStream in = null;
			BufferedReader br = null;
			FileSystem fs = FileSystem.get(context.getConfiguration());
			order = Integer.parseInt(context.getConfiguration().get("order"));
			theta = Integer.parseInt(context.getConfiguration().get("theta"));
			
			StringBuilder pathStringBuilder = new StringBuilder();
			pathStringBuilder.append("o").append(order-1).append("/part-r-00000");
			String pathString = pathStringBuilder.toString();
//			System.out.println("Path of last order output=" + pathString);
			
			Path path = new Path(pathString);
			in = fs.open(path);
			br = new BufferedReader(new InputStreamReader(in));

			String resultLine; // one line from the result of last order.
			String topComb;//The combination, E.g, 1,2
			int i = 0;
			while (i < theta ){//i++<=theta--Wrong. theta:[0,theta-1]
				resultLine = br.readLine();
				if (resultLine != null){
					topComb = resultLine.split("\t")[0];
//					System.out.println(topComb);
					s.add(topComb);
				}
				i++;
			}
			System.out.println(s);
			br.close();
			in.close();
		}

		public void map(Object key, Text value, Context context)throws IOException, InterruptedException {

			String line = value.toString(); // receive one line of the input
			String[] stemp = null;
			stemp = line.split("\t");
			int tlen = stemp.length;
			String pval = stemp[tlen - 1];

			int i = 0;
			List<String> preList = new LinkedList<String>();
			for (String row : s) {
				String col_comb = row;
				preList = s.subList(0, i++);
				List<Integer> duplicatedKeys = CombinationSearcher.getDuplicateKey(col_comb, preList);
				for (int k = 1; k < tlen; k++) {
					if (duplicatedKeys.contains(k)) {
						continue;
					}

					List<Integer> column_combination = CombinationSearcher.formCombination(col_comb, k);
					if (column_combination != null) {
						//get values combination for variables combination.
						String val_combination = CombinationSearcher.search2(column_combination, line);
						//if contain -99,then go on with next cycle.
						if (val_combination.length() == 0) {
							continue;
						}
						
						tp.set(new Text(col_comb + "," + k + "," + zero), new Text(val_combination));
						//<(2,5)(1,3),1> --> Revised as <(2,5,0)(1,3),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(new Text(col_comb + "," + k + "," + zero), star);
						//<(2,5)(*),1> --> Revised as <(2,5,0)(*),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(new Text(col_comb + "," + k + "," + large),new Text(val_combination + "," + pval));
						//<(2,5,999999.0)(1,3,1),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(new Text(col_comb + "," + k + ","+ large), star);
						//<(2,5,999999.0)(*),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");
						
						tp.set(new Text(col_comb + "," + k + "," + large + "," + large),new Text(pval));
						//<(2,5,999999.0,999999.0)(1),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(new Text(col_comb + "," + k + ","+ large + "," + large), star);
						//<(2,5,999999.0,999999.0)(*),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");
												
						String column_combination_string = Tools.listToString(column_combination);
//						System.out.println(column_combination_string);
						List <String>colcom_subSet = Tools.getSubset(column_combination_string);
//						System.out.println(colcom_subSet);
						HashMap <String,String>colvalHash = CombinationSearcher.searchAndReturnHash(column_combination, line);
//						System.out.println(colvalHash);
						
						for(String subset_column:colcom_subSet){
							String subset_value = (String)colvalHash.get(subset_column);
//							System.out.print("subset_value:"+subset_value);
							
							tp.set(new Text(col_comb + "," + k + "," + large + "," + large + "," + subset_column), new Text(subset_value));
							context.write(tp,one);
//							System.out.println("Emit: <"+tp.toString()+">, 1");
							
							tp.set(new Text(col_comb + "," + k + "," + large + "," + large + "," + subset_column), star);
							context.write(tp,one);
//							System.out.println("Emit: <"+tp.toString()+">, 1");
							
							tp.set(new Text(col_comb + "," + k + "," + large + "," + large + "," + subset_column + "," + zero + "," + large), new Text(subset_value + "," + pval));
							context.write(tp,one);
//							System.out.println("Emit: <"+tp.toString()+">, 1");
							
							tp.set(new Text(col_comb + "," + k + "," + large + "," + large + "," + subset_column + "," + zero + "," + large), star);
							context.write(tp,one);
//							System.out.println("Emit: <"+tp.toString()+">, 1");	
						}	
					}
				}
			}
		}
	}
	
	public static class CustomCombiner extends Reducer<TextPair, IntWritable, TextPair, IntWritable>{
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
//				System.out.println("("+key.getFirst().tsum1oString()+")("+key.getSecond().toString()+")"+"\tcount="+count);
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static class CustomPartitioner extends Partitioner<TextPair, IntWritable> {
		@Override
		public int getPartition(TextPair arg0, IntWritable arg1,int numReduceTasks) {
			return (arg0.getFirst().hashCode() & Integer.MAX_VALUE)% numReduceTasks;
		}
	}
	
	public static class Reducer_a extends Reducer<TextPair, IntWritable, Text, Text> {
		
		// calculate total number of values for each variable,must in class so as to deal with * first.
		private double sum1 = 0;
		// put all the probabilities of each variable together.
		private List<Double> I = new LinkedList<Double>();		
		private String old_subKey = "";

		public void reduce(TextPair key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			double count = 0;
			if (key.getSecond().toString().equalsIgnoreCase("*")) {
				sum1 = 0;
				for (IntWritable value : values) {
					sum1 += value.get();
				}
			}
			for (IntWritable value : values) {
				count += value.get();
			}
			// get current first sub key.
			String new_subKey = new String();
			new_subKey = key.getFirst().toString();
			// check where is the first subKey
			if (old_subKey == "") old_subKey = new_subKey;

			if (old_subKey.equalsIgnoreCase(new_subKey)) {
				// old_subKey equals new_subKey
				if (!key.getSecond().toString().equals("*")) {
					double p1 = count / sum1; // probability of each value
					I.add(p1);// accumulate the probability
				}
			} else {//entropy
				double entropy = 0;
				for (int i = 0; i < I.size(); i++) {
					double p1 = I.get(i).doubleValue();
					entropy -= p1 * (Math.log(p1)/Math.log(2)); // calculate entropy
				}
				context.write(new Text(old_subKey), new Text(String.valueOf(entropy)));
				I.clear();// clear after get entropy for a variable
				old_subKey = new_subKey;
			}
		}

		public void cleanup(Context context) throws IOException,InterruptedException { 
			// calculate the entropy for last variale: not trait1 this time.
			double entropy = 0;
			for (int i = 0; i < I.size(); i++) {
				double p1 = I.get(i).doubleValue();
				entropy -= p1 * (Math.log(p1)/Math.log(2)); // calculate entropy
			}
			I.clear();
			context.write(new Text(old_subKey), new Text(String.valueOf(entropy)));
		}
	}
}
