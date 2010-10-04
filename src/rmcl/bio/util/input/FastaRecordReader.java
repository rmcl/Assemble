package rmcl.bio.util.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


/**
 * Treats keys as offset in file and value as line. 
 */
public class FastaRecordReader extends RecordReader<Text, Text> {
	private static final Log LOG = LogFactory.getLog(FastaRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private FSDataInputStream fileIn;
	private Seekable filePosition;

	private Text key = null;
	private Text nextKey = null;
	private Text value = null;
	//	private Counter inputByteCounter;
	private CompressionCodec codec;
	private Decompressor decompressor;
  
	public void initialize(InputSplit genericSplit,
			TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
//		inputByteCounter = ((MapContext)context).getCounter(
//				FileInputFormat.COUNTER_GROUP, FileInputFormat.BYTES_READ);
		Configuration job = context.getConfiguration();
	
		value = new Text();
		key = new Text();
		nextKey = new Text();
		
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(file);
		if (isCompressedInput()) {
			throw new IOException("Compression not supported.");
		} else {
			fileIn.seek(start);
			in = new LineReader(fileIn, job);
			filePosition = fileIn;
		}

		this.pos = start;
        
		// Find the start of the first record in the split
		int newSize = 0;
		while (getFilePosition() <= end) {
			newSize = in.readLine(nextKey);
			if (newSize == 0) {
				break;
			}
        
			// Increment counters and position in file
			pos += newSize;
//			inputByteCounter.increment(newSize);      
        
			// Loop until we find the start of the next record
			if (nextKey.find(">") == 0) {
				nextKey.set(nextKey.toString().substring(1));
				break;
			}
		}
	}
  
  private boolean isCompressedInput() {
    return (codec != null);
  }

  private int maxBytesToConsume(long pos) {
    return isCompressedInput()
      ? Integer.MAX_VALUE
      : (int) Math.min(Integer.MAX_VALUE, end - pos);
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  public boolean nextKeyValue() throws IOException {  
    int newSize = 0;
    
    key.set(nextKey);
    Text tmp = new Text();
    value.clear();
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end) {
    	   	
      newSize += in.readLine(tmp);

      if (newSize == 0) {
        break;
      }
      
      // Increment counters and position in file
      pos += newSize;
//      inputByteCounter.increment(newSize);      
      

      if (tmp.find("#") == 0) {
    	  // Comment line - ignore
    	  continue;
      } else if (tmp.find(">") != 0) {
          // Loop until we find the start of the next record
    	  value.append(tmp.getBytes(), 0, tmp.getBytes().length);
    	  continue;
      } else {
    	  //Got rid of the ">"
    	  nextKey.set(tmp.toString().substring(1));
    	  return true;
      }
    }
    
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    }
    
    //If this read contains a period discard it and get next one.
    if (value != null && value.find(".") >= 0) {
    	return nextKeyValue();
    }
    
    return true;
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }
}