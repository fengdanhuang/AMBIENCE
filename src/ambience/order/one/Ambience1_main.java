package ambience.order.one;

import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import ambience.myutil.TextPair;

/**
 * Configurate hadoop mapreduce jobs for 1st order iteration.
 * Way of Execution: 
 * $AMBIENCE1.jar output_file
 * eg. $AMBIENCE1.jar o1 1
 * @author Chao Feng
 */

public class Ambience1_main {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		System.out.println("\nAMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * \n");
		//Job a: calculate Entropy in a distributed way.
		Job job_a = new Job(conf, "Ambience1_a_cal_entropy");
		job_a.setJarByClass(Ambience1_a_cal_entropy.class);//Set Jar class.
		job_a.setMapperClass(Ambience1_a_cal_entropy.Mapper_a.class);//Set Mapper class.
		job_a.setReducerClass(Ambience1_a_cal_entropy.Reducer_a.class);//Set Reducer class.
		job_a.setCombinerClass(Ambience1_a_cal_entropy.CustomCombiner.class);//Set Combiner class to postprocess Mapper, help Reducer.
		job_a.setPartitionerClass(Ambience1_a_cal_entropy.CustomPartitioner.class);//Set Partitioner class
		job_a.setMapOutputKeyClass(TextPair.class);//Set Mappers's output key class
		job_a.setMapOutputValueClass(IntWritable.class);//Set Mappers's output value class
		
		//Job b: calculate PAI in a distributed way.
		Job job_b = new Job(conf, "Ambience1_b_cal_PAIandKWII");
		job_b.setJarByClass(Ambience1_b_cal_PAIandKWII.class);	
		job_b.setMapperClass(Ambience1_b_cal_PAIandKWII.Mapper_b.class);
		job_b.setReducerClass(Ambience1_b_cal_PAIandKWII.Reducer_b.class);
		job_b.setPartitionerClass(Ambience1_b_cal_PAIandKWII.CustomPartitioner.class);
		job_b.setMapOutputKeyClass(TextPair.class);
		job_b.setMapOutputValueClass(Text.class);
		
		//Job c: sort key by PAI and output to one file.
		Job job_c = new Job(conf, "Ambience1_c_org_PAIandKWII");
		job_c.setJarByClass(Ambience1_c_org_PAIandKWII.class);
		job_c.setMapperClass(Ambience1_c_org_PAIandKWII.Mapper_c.class);
		job_c.setReducerClass(Ambience1_c_org_PAIandKWII.Reducer_c.class);
//		job_c.setPartitionerClass(Ambience1_c_org_PAIandKWII.CustomPartitioner.class);
		job_c.setMapOutputKeyClass(Text.class);
		job_c.setMapOutputValueClass(Text.class);
		
		//Set # of Reducers for each job. Important.
		job_a.setNumReduceTasks(40);//calculate entropy, decide # of output file.
		job_b.setNumReduceTasks(40);//calculate PAI and KWII
		job_c.setNumReduceTasks(1);//organize PAI and KWII
		
		//job_a's input and output parameters
		FileInputFormat.addInputPath(job_a, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job_a, new Path("o1-job_a"));
		FileOutputFormat.setOutputPath(job_a, new Path(otherArgs[1]+ "-job_a"));
		//job_b's input and output parameters
//		FileInputFormat.addInputPath(job_b, new Path("o1-job_a"));
//		FileOutputFormat.setOutputPath(job_b, new Path("o1-job_b"));
		FileInputFormat.addInputPath(job_b, new Path(otherArgs[1] + "-job_a"));
		FileOutputFormat.setOutputPath(job_b, new Path(otherArgs[1] + "-job_b"));
		//job_c's input and output parameters
//		FileInputFormat.addInputPath(job_c, new Path("o1-job_b"));
//		FileOutputFormat.setOutputPath(job_c, new Path("o1"));
		FileInputFormat.addInputPath(job_c, new Path(otherArgs[1] + "-job_b"));
		FileOutputFormat.setOutputPath(job_c, new Path(otherArgs[1]));
		
		double moment1 = new Date().getTime();	
		job_a.waitForCompletion(true);//'True' means print progress to user.
		double moment2 = new Date().getTime();
		job_b.waitForCompletion(true);
		double moment3 = new Date().getTime();
		job_c.waitForCompletion(true);
		double moment4 = new Date().getTime();
		
		System.out.println("Job_a took " + (moment2 - moment1)/1000 + "seconds");
		System.out.println("Job_b took " + (moment3 - moment2)/1000 + "seconds");
		System.out.println("Job_c took " + (moment4 - moment3)/1000 + "seconds");
		System.out.println("Ambience (order=1) took " + (moment4 - moment1)/1000 + "seconds");

		System.out.println("\nAMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * \n");		
	}
}
