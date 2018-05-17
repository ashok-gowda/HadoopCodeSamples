package mapre_problems;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;

public class MaxTemperature {
	public static void main(String args[]) throws IOException {
		String inputPath="C:\\Users\\Ashok\\Documents\\GitHub\\HadoopCodeSamples\\data\\ndcg";
		String outputPath="C:\\Users\\Ashok\\Documents\\GitHub\\HadoopCodeSamples\\data\\ndcg\\output";
		JobConf conf=new JobConf(MaxTemperature.class);
		FileInputFormat.addInputPath(conf,new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setReducerClass(MaxTemperatureReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
	}

}
