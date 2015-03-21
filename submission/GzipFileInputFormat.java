package gunzip;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Extends FileInputFormat class. Makes sure that files are not split (as gzip files are not splitable)
 * It creates a GzipRecordReader instead of the default record reader
 */
public class GzipFileInputFormat
    extends FileInputFormat<Text, BytesWritable>
{
    
    /**
     * GZIP files are not splitable
     */
    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }

    /**
     * Create a GZipFileRecordReader
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
        throws IOException, InterruptedException
    {
        return new GzipFileRecordReader();
    }
}