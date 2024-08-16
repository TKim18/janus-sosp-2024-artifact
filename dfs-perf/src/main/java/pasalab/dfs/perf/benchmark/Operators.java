package pasalab.dfs.perf.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.log4j.Logger;

import pasalab.dfs.perf.PerfConstants;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.fs.PerfFileSystem;

public class Operators {
  private static final Random RAND = new Random(System.currentTimeMillis());
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  /**
   * Close the connect to the file system.
   * 
   * @param fs
   * @throws IOException
   */
  public static void close(PerfFileSystem fs) throws IOException {
    fs.close();
  }

  /**
   * Connect to the file system.
   * 
   * @param fsPath
   * @param taskConf
   * @return
   * @throws IOException
   */
  public static PerfFileSystem connect(String fsPath, TaskConfiguration taskConf)
      throws IOException {
    PerfFileSystem fs = PerfFileSystem.get(fsPath, taskConf);
    fs.connect();
    return fs;
  }

  /**
   * Skip forward then read the file for times.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param skipBytes
   * @param readBytes
   * @param times
   * @return
   * @throws IOException
   */
  public static long forwardSkipRead(PerfFileSystem fs, String filePath, int bufferSize,
      long skipBytes, long readBytes, int times, String workload, int taskId, int threadId) throws IOException {
    byte[] content = new byte[bufferSize];
    long readLen = 0;
    InputStream is = fs.getInputStream(filePath);
    for (int t = 0; t < times; t ++) {
      is.skip(skipBytes);
      readLen += readSpecifiedBytes(is, content, readBytes, workload, taskId, threadId);
    }
    is.close();
    return readLen;
  }

  /**
   * Do metadata operations.
   * 
   * @param fs
   * @param filePath
   * @return
   * @throws IOException
   */
  public static int metadataSample(PerfFileSystem fs, String filePath) throws IOException {
    String emptyFilePath = filePath + "/empty_file";
    if (!fs.mkdir(filePath, true)) {
      return 0;
    }
    if (!fs.create(emptyFilePath)) {
      return 1;
    }
    if (!fs.exists(emptyFilePath)) {
      return 2;
    }
    if (!fs.rename(filePath, filePath + "-__-__-")) {
      return 3;
    }
    if (!fs.delete(filePath + "-__-__-", true)) {
      return 4;
    }
    return 5;
  }

  /**
   * Skip randomly then read the file for times.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param readBytes
   * @param times
   * @return
   * @throws IOException
   */
  public static long randomSkipRead(PerfFileSystem fs, String filePath, int bufferSize,
      long readBytes, int times, String workload, int taskId, int threadId) throws IOException {
    byte[] content = new byte[bufferSize];
    long readLen = 0;
    long fileLen = fs.getLength(filePath);
    for (int t = 0; t < times; t ++) {
      long skipBytes = RAND.nextLong() % fileLen;
      if (skipBytes < 0) {
        skipBytes = -skipBytes;
      }
      InputStream is = fs.getInputStream(filePath);
      is.skip(skipBytes);
      readLen += readSpecifiedBytes(is, content, readBytes, workload, taskId, threadId);
      is.close();
    }
    return readLen;
  }

  /**
   * Read a file from begin to the end.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @return
   * @throws IOException
   */
  public static long readSingleFile(PerfFileSystem fs, String filePath, int bufferSize,
      String workload, int taskId, int threadId)
      throws IOException {
    long readLen = 0;
    byte[] content = new byte[bufferSize];
    DFSInputStream is = null;
      try {
        is = (DFSInputStream) ((HdfsDataInputStream) fs.getInputStream(filePath)).getWrappedStream();
      } catch (Exception e) {
      LOG.error("Uh oh ", e);
      return 0;
    }
    long startTime = System.currentTimeMillis();
      int onceLen = 0;
      try {
        onceLen = is.read(0, content, 0 , bufferSize);
      } catch (Exception e) {
        LOG.error("ruh roh", e);
      }
      //int onceLen = is.read(0, content, 0 , bufferSize);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;	
    while (onceLen > 0) {
      LOG.info("Once len = " + onceLen);
		//LOG.info("HeART-DFS-PERF-Metrics-" + workload + "-READ || " + taskId + "," + threadId + ","
      //    + startTime + "," + endTime + "," + duration + ","
      //    + onceLen + "," + ((onceLen / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
      readLen += (long) onceLen;
      startTime = System.currentTimeMillis();
      onceLen = is.read(readLen, content, 0, bufferSize);
	    endTime = System.currentTimeMillis();
      duration = endTime - startTime;
    }

	  //readLen = onceLen;
    is.close();
    return readLen;
  }

  public static long readFileIntoBuffer(PerfFileSystem fs, String filePath, byte[] buffer) throws IOException {
    InputStream is = fs.getInputStream(filePath);
    // read file into buffer
    long total = 0;
	long n = is.read(buffer);
	total += n;
    while (n > 0) {
      n = is.read(buffer);
	  total += n;
    }
    is.close();
    return total;
  }

  private static long readSpecifiedBytes(InputStream is, byte[] content, long readBytes,
      String workload, int taskId, int threadId)
      throws IOException {
    long remainBytes = readBytes;
    int readLen = 0;
    long startTime, endTime, duration;
    while (remainBytes >= content.length) {
      startTime = System.currentTimeMillis();
      int once = is.read(content);
      endTime = System.currentTimeMillis();
      duration = endTime - startTime;
      if (once == -1) {
        return readLen;
      }
      readLen += once;
      remainBytes -= once;
      LOG.info("HeART-DFS-PERF-Metrics-" + workload + "-READ || " + taskId + "," + threadId + ","
          + startTime + "," + endTime + "," + duration + ","
          + once + "," + ((once / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
    }
    if (remainBytes > 0) {
      startTime = System.currentTimeMillis();
      int once = is.read(content, 0, (int) remainBytes);
      endTime = System.currentTimeMillis();
      duration = endTime - startTime;
      if (once == -1) {
        return readLen;
      }
      readLen += once;
      LOG.info("HeART-DFS-PERF-Metrics-" + workload + "-READ || " + taskId + "," + threadId + ","
          + startTime + "," + endTime + "," + duration + ","
          + once + "," + ((once / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
    }
    return readLen;
  }

  /**
   * Read a file. Skip once and read once.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param skipBytes
   * @param readBytes
   * @return
   * @throws IOException
   */
  public static long skipReadOnce(PerfFileSystem fs, String filePath, int bufferSize,
      long skipBytes, long readBytes, String workload, int taskId, int threadId) throws IOException {
    byte[] content = new byte[bufferSize];
    InputStream is = fs.getInputStream(filePath);
    is.skip(skipBytes);
    long readLen = readSpecifiedBytes(is, content, readBytes, workload, taskId, threadId);
    is.close();
    return readLen;
  }

  /**
   * Read a file. Skip and read until the end of the file.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param skipBytes
   * @param readBytes
   * @return
   * @throws IOException
   */
  public static long skipReadToEnd(PerfFileSystem fs, String filePath, int bufferSize,
      long skipBytes, long readBytes, String workload, int taskId, int threadId) throws IOException {
    byte[] content = new byte[bufferSize];
    long readLen = 0;
    InputStream is = fs.getInputStream(filePath);
    is.skip(skipBytes);
    long once = readSpecifiedBytes(is, content, readBytes, workload, taskId, threadId);
    while (once > 0) {
      readLen += once;
      is.skip(skipBytes);
      once = readSpecifiedBytes(is, content, readBytes, workload, taskId, threadId);
    }
    is.close();
    return readLen;
  }

  private static void writeContentToFile(OutputStream os, long fileSize, int bufferSize,
      String workload, int taskId, int threadId)
      throws IOException {
    byte[] content = new byte[bufferSize];
    long remain = fileSize;
    long startTime, endTime, duration;
    while (remain >= bufferSize) {
      startTime = System.currentTimeMillis();
      os.write(content);
	    endTime = System.currentTimeMillis();
      duration = endTime - startTime;
      remain -= bufferSize;
//      LOG.info("HeART-DFS-PERF-Metrics-" + workload + "-WRITE || " + taskId + "," + threadId + ","
//          + startTime + "," + endTime + "," + duration + ","
//          + bufferSize + "," + ((bufferSize / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
    }
    if (remain > 0) {
      startTime = System.currentTimeMillis();
      os.write(content, 0, (int) remain);
      endTime = System.currentTimeMillis();
      duration = endTime - startTime;
//      LOG.info("HeART-DFS-PERF-Metrics-" + workload + "-WRITE || " + taskId + "," + threadId + ","
//          + startTime + "," + endTime + "," + duration + ","
//          + remain + "," + ((remain / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
    }
  }

  /**
   * Create a file and write to it.
   * 
   * @param fs
   * @param filePath
   * @param fileSize
   * @param bufferSize
   * @throws IOException
   */
  public static void writeSingleFile(PerfFileSystem fs, String filePath, long fileSize,
      int bufferSize, String workload, int taskId, int threadId) throws IOException {
    OutputStream os = fs.getOutputStream(filePath);
    writeContentToFile(os, fileSize, bufferSize, workload, taskId, threadId);
    os.close();
  }
}
