package hw1;

import java.io.IOException;

import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Homework1 {
	public static final int MAX_MAPPERS = 48;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Path inputFolder = new Path(args[0]);
		Path outputFolder = new Path(args[1]);
		
		Configuration conf = new Configuration();
		conf.set("outputPath", outputFolder.toString());
		Job job = Job.getInstance(conf, "batch gunzip");
		conf.setInt("mapred.map.tasks", MAX_MAPPERS);
		
		job.setJarByClass(Homework1.class);
		job.setMapperClass(UnzipMapper.class);
		job.setInputFormatClass(GzipFileInputFormat.class);
		// We don't need any reducer (instead we could use a IdentityReducer)
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		FileInputFormat.addInputPath(job, inputFolder);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}