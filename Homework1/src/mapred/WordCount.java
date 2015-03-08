package mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class MyMapper
       extends Mapper<Text, BytesWritable, Text, IntWritable>{

	  private final static IntWritable one = new IntWritable( 1 );
	  private Text word = new Text();
    
    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
    	String content = new String( value.getBytes(), "UTF-8" );
    	StringTokenizer tokenizer = new StringTokenizer( content );
        while ( tokenizer.hasMoreTokens() )
        {
            word.set( tokenizer.nextToken() );
            context.write( word, one );
        }
    }
  }
  
  public static class MyReducer
  extends Reducer<Text, IntWritable, Text, IntWritable>
{
  public void reduce( Text key, Iterable<IntWritable> values, Context context )
      throws IOException, InterruptedException
  {
      int sum = 0;
      for ( IntWritable val : values )
      {
          sum += val.get();
      }
      context.write(key, new IntWritable(sum));
  }
}

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    
    job.setInputFormatClass(GzipInputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

