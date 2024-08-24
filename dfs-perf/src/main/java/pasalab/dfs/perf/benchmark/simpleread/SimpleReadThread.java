package pasalab.dfs.perf.benchmark.simpleread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.benchmark.ListGenerator;
import pasalab.dfs.perf.benchmark.Operators;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

public class SimpleReadThread extends PerfThread {
  private int mBufferSize;
  private PerfFileSystem mFileSystem;
  private List<String> mReadFiles;

  private boolean mSuccess;
  private double mThroughput; // in MB/s
  private List<Long> mLatency;

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
    long readBytes = 0;
    mSuccess = true;
    clearHistory();
    long timeMs = System.currentTimeMillis();
    for (String fileName : mReadFiles) {
      long startTime = System.currentTimeMillis();
      long nbytes = 0;
      try {
        nbytes = Operators.readSingleFile(mFileSystem, fileName, mBufferSize, "SimpleRead", mTaskId, mId);
        readBytes += nbytes;
      } catch (IOException e) {
        LOG.error("Failed to read file " + fileName, e);
        mSuccess = false;
      }
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      mLatency.add(duration);
      LOG.info("HeART-DFS-PERF-Metrics-SimpleRead-FILE || " + mTaskId + "," + mId + ","
          + startTime + "," + endTime + "," + duration + ","
          + nbytes + "," + ((nbytes/ 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
    }
    timeMs = System.currentTimeMillis() - timeMs;
    mThroughput = (readBytes / 1024.0 / 1024.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    try {
      mFileSystem = Operators.connect(PerfConf.get().DFS_ADDRESS, taskConf);
      String readDir = taskConf.getProperty("read.dir");
      List<String> candidates = mFileSystem.list(readDir);
      if (candidates == null || candidates.isEmpty()) {
        throw new IOException("No file to read");
      }
      boolean isRandom = "RANDOM".equals(taskConf.getProperty("read.mode"));
      int filesNum = taskConf.getIntProperty("files.per.thread");
      if (isRandom) {
        mReadFiles = ListGenerator.generateRandomReadFiles(filesNum, candidates);
      } else {
        mReadFiles =
            ListGenerator.generateSequenceReadFiles(mId, PerfConf.get().THREADS_NUM, filesNum,
                candidates);
      }
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
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }
}
