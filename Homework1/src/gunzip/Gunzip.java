package gunzip;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Gunzip {
  
  public static class MyMapper
  extends Mapper<Text, BytesWritable, NullWritable, NullWritable>
{

  public void map( Text key, BytesWritable value, Context context )
      throws IOException, InterruptedException
  {
      String filename = key.toString();
      filename = filename.substring(0, filename.indexOf(".gz"));
      
      Path outputPath = FileOutputFormat.getOutputPath(context);
      outputPath = new Path(outputPath.toString() + "/" + filename);
      
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(outputPath);
      out.write(value.copyBytes());
      out.close();

  }
}
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    
    job.setJarByClass(Gunzip.class);
    
    job.setMapperClass(MyMapper.class);
    
    job.setInputFormatClass(GzipFileInputFormat.class);
    
    job.setOutputFormatClass(NullOutputFormat.class);
    
    job.setNumReduceTasks(0);
    
    conf.setInt("mapred.map.tasks", 50);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}