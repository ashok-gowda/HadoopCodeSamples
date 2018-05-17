package mapre_problems;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureReducer extends Mapper implements Reducer<Text,IntWritable,Text,IntWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(Text textInput,Iterator<IntWritable>values,OutputCollector<Text,IntWritable>output,
			Reporter reporter) throws IOException {
		int max_value=Integer.MIN_VALUE;
		while(values.hasNext()) {
			int value=values.next().get();
			max_value=Math.max(max_value,value);
		}
		System.out.println("REDUCER");
		System.out.println(textInput.toString()+" "+max_value);
		output.collect(textInput,new IntWritable(max_value));
		
	}

}
