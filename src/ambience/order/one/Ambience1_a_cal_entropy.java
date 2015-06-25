package ambience.order.one;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import ambience.myutil.TextPair;

/**
 * calculate entropies of combinations of geno type and pheno type
 * @author Chao Feng
 */

public class Ambience1_a_cal_entropy {
	
	public static class Mapper_a extends Mapper<Object, Text, TextPair, IntWritable> {

		private Text variable = new Text();// variable
		private Text values = new Text();// values of variable
		private Text varpheno = new Text();//variable with phenotype
		private Text valcom = new Text(); //values of variable with phenotype 
		private Text varphenopheno = new Text(); //phenotype (different variable has different phenotype)
		private Text valpheno = new Text(); //values of phenotype
		
		private Text star = new Text("*");// used for counting total counts of each variable
		private TextPair tp = new TextPair();
		
		private int zero = 0; //represent null value after variable, convenient for later process. 
		private int large = 9999999; //represent phenotype
		private int INVALID_VAL = -99;// to denote missing data.
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); // receive one line
			String[] stemp = null;
			stemp = line.split("\t");
			int tlen = stemp.length;
			String pval = stemp[tlen - 1];
			StringTokenizer tokenizer = new StringTokenizer(line); //process each line
			int var_no = 1;

			// the first token is variable
			int temp;
			while (tokenizer.hasMoreTokens()) {
				
				temp = Integer.valueOf(tokenizer.nextToken());
				
				if (var_no <= (tlen-1)){						
					if (temp != INVALID_VAL){	
						//Adding zero is for splitting key, eazing of PAI and KWII.  
						variable.set(new Text(var_no + "," + zero));//all kinds of variables			
						varpheno.set(new Text(var_no + "," + large));	
						varphenopheno.set(new Text(var_no + "," + large + "," + large));
						
						values.set(Integer.toString(temp));	//all kinds of values											
						valcom.set(new Text(temp + ","+ pval));						
						valpheno.set(new Text(pval));												
					
						tp.set(variable, values);
						//<(1)(3),1>  --> Revised as <(1,0)(3),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(variable, star);
						//<(1)(*),1>  --> Revised as <(1,0)(*),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(varpheno, valcom);
						//<(1,999999.0)(3,1),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");

						tp.set(varpheno, star);
						//<(1,999999.0)(*),1>
						context.write(tp, one);	
//						System.out.println("Emit: <"+tp.toString()+">, 1");
						
						tp.set(varphenopheno, valpheno);
						//<(1,9999999.0,9999999.0)(1),1>
						context.write(tp, one);
//						System.out.println("Emit: <"+tp.toString()+">, 1");
						
						tp.set(varphenopheno, star);
						//<(1,9999999.0,9999999.0)(*),1>
						context.write(tp, one);	
//						System.out.println("Emit: <"+tp.toString()+">, 1");				
					}				
				}
				var_no++;			
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
			// Note:* is ok, "total" doesn't work. Since * is before number
			if (key.getSecond().toString().equalsIgnoreCase("*")) {
				sum1 = 0;
				for (IntWritable value : values) {
					sum1 += value.get();
				}
//				System.out.println("("+key.getFirst().toString()+")("+key.getSecond().toString()+")"+"\tsum1="+sum1);
			}
			for (IntWritable value : values) {
				count += value.get();
//				System.out.println("("+key.getFirst().tsum1oString()+")("+key.getSecond().toString()+")"+"\tcount="+count);
			}
			// get current first sub key.
			String new_subKey = new String();
			new_subKey = key.getFirst().toString();
			// check where is the first subKey
			if (old_subKey == "") old_subKey = new_subKey;

			if (old_subKey.equalsIgnoreCase(new_subKey)) {//probability
				if (!key.getSecond().toString().equals("*")) {
					double p1 = count / sum1; // probability of each value
					I.add(p1);// accumulate the probability
//					context.write(new Text(old_subKey+","+key.getSecond().toString()), new Text(String.valueOf(count)));
//					context.write(new Text(old_subKey+","+key.getSecond().toString()), new Text(String.valueOf(sum1)));
//					context.write(new Text(old_subKey), new Text(String.valueOf(p1)));
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
			// calculate the entropy for last variable: trait1.
			double enP = 0;
			for (int i = 0; i < I.size(); i++) {
				double p1 = I.get(i).doubleValue();
				enP -= p1 * (Math.log(p1)/Math.log(2)); // calculate entropy
			}
			I.clear();
			context.write(new Text(old_subKey), new Text(String.valueOf(enP)));
		}
	}

}
