package pasalab.dfs.perf.benchmark.simplewrite;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import pasalab.dfs.perf.basic.FileMetadata;
import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.RedundancyStatus;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.benchmark.ListGenerator;
import pasalab.dfs.perf.benchmark.Operators;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

public class SimpleWriteThread extends PerfThread {
  private PerfFileSystem mFileSystem;
  private int mBufferSize;
  private long mFileLength;
  private int mNumFiles;
  private String mBaseDirectory;
  private boolean isBaseline;
  private List<String> mWriteFiles;
  private List<Long> mLatency;
  private ConcurrentLinkedDeque<FileMetadata> files;
  private AtomicBoolean terminated;

  private boolean mSuccess;
  private double mThroughput; // in MB/s

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getThroughput() {
    return mThroughput;
  }

  public List<Long> getLatency() {
    return mLatency;
  }

  public void clearHistory() {
    mThroughput = 0.0;
    mLatency = new ArrayList<Long>();
  }

  public void run() {
    int numFiles = mNumFiles;
    LOG.info("Starting write thread at task " + mTaskId + " and id " + mId);
    Random generator = new Random(128);
    // removed all latency/throughput tracking
    clearHistory();
    mThroughput = 0;
    mSuccess = true;

    int fileNum = 0;
    int fileSize;
    String filePath, fileName;
    FileMetadata file;
    // keep running until killed
    while (fileNum < numFiles) {
      if (terminated.get()) {
        LOG.info("Got a termination signal, ending write loop");
        break;
      }
      // we'll be writing files constantly
      // determine the file size based on some distribution
      fileSize = generateFileSize(generator);
      fileName = generateFileName(mId, fileNum, fileSize, isBaseline);
      filePath = mBaseDirectory;
      try {
        final long start = System.currentTimeMillis();
        Operators.writeSingleFile(mFileSystem, filePath + "/" + fileName, fileSize,
            mBufferSize, "SimpleWrite", mTaskId, mId);
        LOG.info("Time to write took " + (System.currentTimeMillis() - start) + " ms to write file number " + fileNum + ".");
        file = new FileMetadata(fileName, filePath, fileSize,
            isBaseline ? RedundancyStatus.REPL : RedundancyStatus.HYBRID);
        // upon success, start tracking the file for
        // future conversions in a separate thread
        files.addLast(file);
      } catch (ConnectException e) {
        terminated.set(true);
        LOG.error("Namenode is down, terminating writes");
      } catch (IOException e) {
        // it's okay if write fails, just don't track it
        LOG.info("Failed to write file number " + fileNum + " due to " + e.getMessage());
      }
      fileNum++;
    }
    // notify the transcode thread to stop working
//    while (true) {
//      try {
//        Thread.sleep(10000);
//        if (terminated.get()) {
//          return;
//        }
//      } catch (Exception e) {
//
//      }
//    }
    terminated.set(true);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mFileLength = taskConf.getLongProperty("file.length.bytes");
    try {
      mFileSystem = Operators.connect(PerfConf.get().DFS_ADDRESS, taskConf);
      mBaseDirectory = taskConf.getProperty("write.dir");
      isBaseline = !mBaseDirectory.startsWith("/ec");
      mNumFiles = taskConf.getIntProperty("files.per.thread");
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mThroughput = 0;
    mLatency = new ArrayList<Long>();
    return true;
  }

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf, boolean closeFileSystem) {
    try {
      if (closeFileSystem) Operators.close(mFileSystem);
      terminated.set(true);
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }

  public void setConcurrentFileMap(ConcurrentLinkedDeque<FileMetadata> files) {
    this.files = files;
  }

  public void setTerminationFlag(AtomicBoolean terminated) {
    this.terminated = terminated;
  }

  private int generateFileSize(Random generator) {
    boolean random = false;
    int mb = 1024 * 1024;
    if (random) {
      int n = generator.nextInt(5);
      // 3/5 are 48 MB
      // 1/5 are 96 MB
      // 1/5 are 192 MB
      if (n < 3) {
        return 48 * mb;
      } else if (n < 4) {
        return 96 * mb;
      } else {
        return 192 * mb;
      }
    } else {
      return 160 * mb;
    }
  }

  public static String generateFilePath(String baseDir, int clientId, int threadId, int fileId, int fileSize) {
    return baseDir + "/" + clientId + "/" + threadId + "-" + fileId + "-" + fileSize;
  }

  public static String generateFileName(int threadId, int fileId, int fileSize, boolean isBaseline) {
    String prefix = isBaseline ? "regular" : "hybrid";
    String randomName = UUID.randomUUID().toString().substring(0, 10);
    // return prefix + "-" + threadId + "-" + fileId + "-" + fileSize;
    return prefix + "-" + randomName;
  }
}
