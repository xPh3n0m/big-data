package hw1;

import java.lang.String;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class UnzipMapper extends Mapper<Text, BytesWritable, NullWritable, NullWritable> {

	// The key is a file's path and the value its content
	public void map(Text key, BytesWritable value, Context context) {
		String filename = key.toString();
		Configuration conf = context.getConfiguration();
		String outputFolder = conf.get("outputPath");
		
		
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(filename), conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not get filesystem for " + filename);
			System.exit(1);
		}
		
		// Find correct compression codec based on file extension
		Path inputPath = new Path(filename);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + filename);
			System.exit(2);
		}

		// Write uncompressed data to output file
		String outputUri = new String(outputFolder + "/" +
				CompressionCodecFactory.removeSuffix(filename, codec.getDefaultExtension()));
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(new ByteArrayInputStream(value.getBytes()));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not write " + outputUri);
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}