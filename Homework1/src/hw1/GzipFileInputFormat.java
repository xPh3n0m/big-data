package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class GzipFileInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		WholeFileRecordReader recordReader = new WholeFileRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
	
	// This is important in order for the archive not to get split
	@Override
	public boolean isSplitable(JobContext context, Path path) {
		return false;
	}
	
	public static class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

		private FileSplit fileSplit;
		private Configuration conf;
		private boolean processed = false;
		private Text filePath;
		private BytesWritable value;

		// Find real uncompressed size of gz file at the end of archive
		private int uncompressedLength() {
			int nLastBytes = 4;
			int length = 0;
			File file = new File(fileSplit.getPath().toString());
			RandomAccessFile raf = null;
			try {
				raf = new RandomAccessFile(file, "r");
				raf.seek(raf.length() - nLastBytes);
				
				int offset = 0;
				for (int i=0; i<nLastBytes; ++i) {
					length = length | (raf.read() << offset);
					offset += 8;
				}
				
			} catch (FileNotFoundException e) {
				System.err.println("Could not open split");
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Could not seek random access file");
				e.printStackTrace();
			} finally {
				try {
					raf.close();
				} catch (IOException e) {
					System.err.println("Could not close split");
					e.printStackTrace();
				}
			}
			
			return length;
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!processed) {
				byte[] contents = new byte[(int) fileSplit.getLength()];
				Path file = fileSplit.getPath();
				filePath.set(file.toString());
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try {
					in = fs.open(file);
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
		@Override
		public void close() throws IOException {
			// do nothing
		}
		@Override
		public Text getCurrentKey() throws IOException,
				InterruptedException {
			return filePath;
		}
		@Override
		public BytesWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
			
		}
		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}
	}
}