package mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This RecordReader implementation extracts individual files from a ZIP
 * file and hands them over to the Mapper. The "key" is the decompressed
 * file name, the "value" is the file contents.
 */
public class GzipFileRecordReader
    extends RecordReader<Text, BytesWritable>
{
    /** InputStream used to read the GZIP file from the FileSystem */
    //private FSDataInputStream fsin;

    /** GZIP file parser/decompresser */
    //private GZIPInputStream gzip;

    /** Uncompressed file name */
    //private Text currentKey;

    /** Uncompressed file contents */
    //private BytesWritable currentValue;

    /** Used to indicate progress */
    //private boolean isFinished = false;
    
    //private FileSplit fileSplit;
    
    private CompressionCodecFactory compressionCodecs = null;
    private FileSplit fileSplit;
    private Configuration conf;
    private InputStream in;
    private Text key = new Text("");
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;

    /**
     * Initialise and open the ZIP file from the FileSystem
     */
    /*
    @Override
    public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
        throws IOException, InterruptedException
    {
        this.fileSplit = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        
        Path path = fileSplit.getPath();
        currentKey = new Text(path.getName());
        FileSystem fs = path.getFileSystem( conf );
        
        // Open the stream
        fsin = fs.open( path );
        gzip = new GZIPInputStream( fsin );
    }*/
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();

        final Path file = fileSplit.getPath();
        compressionCodecs = new CompressionCodecFactory(conf);

        final CompressionCodec codec = compressionCodecs.getCodec(file);
        System.out.println(codec);
        FileSystem fs = file.getFileSystem(conf);
        in = fs.open(file);

        if (codec != null) {
            in = codec.createInputStream(in);
        }
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            key.set(file.getName());

            try {
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(in);
            }

            processed = true;
            return true;
        }

        return false;
    }

    /**
     * This is where the magic happens, each ZipEntry is decompressed and
     * readied for the Mapper. The contents of each file is held *in memory*
     * in a BytesWritable object.
     * 
     * If the ZipFileInputFormat has been set to Lenient (not the default),
     * certain exceptions will be gracefully ignored to prevent a larger job
     * from failing.
     */
    /*
    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException
    {
        long length = fileSplit.getLength();
        
        
        Path file = fileSplit.getPath();
        currentKey.set(file.getName());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = 0;
            bytesRead = gzip.read( temp, 0, 8192 );

            if ( bytesRead > 0 )
                bos.write( temp, 0, bytesRead );
            else
                break;
        }
        
        // Uncompressed contents
        currentValue = new BytesWritable( bos.toByteArray() );
        
        return true;
    }
*/
    

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

}