package ambience.order.n;

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
 * Configurate hadoop mapreduce jobs for Nth order. (N>=2)
 * Way of Execution: 
 * $AMBIENCEn.jar output_file order theta
 * eg1. $AMBIENCEn.jar o2 2 10
 * eg2. $AMBIENCEn.jar o3 3 10
 * @author Chao Feng
 */

public class AmbienceN_main {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		long milliSecondsForTimeout = 1000*60*60*60; //Set timeoutvalue as 60 hours.
		conf.setLong("mapred.task.timeout", milliSecondsForTimeout);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		System.out.println("\nAMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * \n");
		//Job a: calculate Entropy in a distributed way.
		Job job_a = new Job(conf, "AmbienceN_a_cal_entropy");
		job_a.setJarByClass(AmbienceN_a_cal_entropy.class);//Set Jar class.
		job_a.setMapperClass(AmbienceN_a_cal_entropy.Mapper_a.class);//Set Mapper class.
		job_a.setReducerClass(AmbienceN_a_cal_entropy.Reducer_a.class);//Set Reducer class.
		job_a.setCombinerClass(AmbienceN_a_cal_entropy.CustomCombiner.class);//Set Combiner class to postprocess Mapper, help Reducer.
		job_a.setPartitionerClass(AmbienceN_a_cal_entropy.CustomPartitioner.class);//Set Partitioner class
		job_a.setMapOutputKeyClass(TextPair.class);//Set Mappers's output key class
		job_a.setMapOutputValueClass(IntWritable.class);//Set Mappers's output value class
		
		//Job b: calculate PAI in a distributed way.
		Job job_b = new Job(conf, "AmbienceN_b_cal_PAIandKWII");
		job_b.setJarByClass(AmbienceN_b_cal_PAIandKWII.class);	
		job_b.setMapperClass(AmbienceN_b_cal_PAIandKWII.Mapper_b.class);
		job_b.setReducerClass(AmbienceN_b_cal_PAIandKWII.Reducer_b.class);
		job_b.setPartitionerClass(AmbienceN_b_cal_PAIandKWII.CustomPartitioner.class);
		job_b.setMapOutputKeyClass(TextPair.class);
		job_b.setMapOutputValueClass(Text.class);
		
		//Job c: sort key by PAI and output to one file.
		Job job_c = new Job(conf, "AmbienceN_c_org_PAIandKWII");
		job_c.setJarByClass(AmbienceN_c_org_PAIandKWII.class);
		job_c.setMapperClass(AmbienceN_c_org_PAIandKWII.Mapper_c.class);
		job_c.setReducerClass(AmbienceN_c_org_PAIandKWII.Reducer_c.class);
//		job_c.setPartitionerClass(Ambience1_c_org_PAIandKWII.CustomPartitioner.class);
		job_c.setMapOutputKeyClass(Text.class);
		job_c.setMapOutputValueClass(Text.class);
		
		//Set # of Reducers for each job. Important.
		job_a.setNumReduceTasks(40);//calculate entropy, decide # of output file.
		job_b.setNumReduceTasks(40);//calculate PAI and KWII
		job_c.setNumReduceTasks(1);//organize PAI and KWII
		
		//job_a's input and output parameters
		FileInputFormat.addInputPath(job_a, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job_a, new Path("oN-job_a"));
		FileOutputFormat.setOutputPath(job_a, new Path(otherArgs[1]+ "-job_a"));
		//job_b's input and output parameters
//		FileInputFormat.addInputPath(job_b, new Path("oN-job_a"));
//		FileOutputFormat.setOutputPath(job_b, new Path("oN-job_b"));
		FileInputFormat.addInputPath(job_b, new Path(otherArgs[1] + "-job_a"));
		FileOutputFormat.setOutputPath(job_b, new Path(otherArgs[1] + "-job_b"));
		//job_c's input and output parameters
//		FileInputFormat.addInputPath(job_c, new Path("oN-job_b"));
//		FileOutputFormat.setOutputPath(job_c, new Path("oN"));
		FileInputFormat.addInputPath(job_c, new Path(otherArgs[1] + "-job_b"));
		FileOutputFormat.setOutputPath(job_c, new Path(otherArgs[1]));
		
		//job_a gets order and theta parameters.
		job_a.getConfiguration().set("order", new String(otherArgs[2]));
		job_a.getConfiguration().set("theta", new String(otherArgs[3]));
		//job_b gets order and theta parameters.
		job_b.getConfiguration().set("order", new String(otherArgs[2]));
		job_b.getConfiguration().set("theta", new String(otherArgs[3]));
		//job_c gets order and theta parameters.
		job_c.getConfiguration().set("order", new String(otherArgs[2]));
//		job_c.getConfiguration().set("theta", new String(otherArgs[3]));
		
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
		System.out.println("Ambience (order=" + otherArgs[2] +") took " + (moment4 - moment1)/1000 + "seconds");

		System.out.println("\nAMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * AMBIENCE * \n");		
	}

}
