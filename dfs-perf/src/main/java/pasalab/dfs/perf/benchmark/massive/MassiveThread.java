package pasalab.dfs.perf.benchmark.massive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.benchmark.ListGenerator;
import pasalab.dfs.perf.benchmark.Operators;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

public class MassiveThread extends PerfThread {
  private Random mRand;

  private int mBasicFilesNum;
  private int mBufferSize;
  private long mFileLength;
  private PerfFileSystem mFileSystem;
  private long mLimitTimeMs;
  private boolean mShuffle;
  private String mWorkDir;

  private double mBasicWriteThroughput; // in MB/s
  private double mReadThroughput; // in MB/s
  private boolean mSuccess;
  private double mWriteThroughput; // in MB/s

  private List<Long> mBasicLatency;
  private List<Long> mReadLatency;
  private List<Long> mWriteLatency;

  public void clearHistory() {
    mBasicWriteThroughput = 0.0;
    mReadThroughput = 0.0;
    mWriteThroughput = 0.0;
    mBasicLatency = new ArrayList<Long>();
    mReadLatency = new ArrayList<Long>();
    mWriteLatency = new ArrayList<Long>();
  }

  public double getBasicWriteThroughput() {
    return mBasicWriteThroughput;
  }

  public double getReadThroughput() {
    return mReadThroughput;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getWriteThroughput() {
    return mWriteThroughput;
  }

  public List<Long> getBasicLatency() { return mBasicLatency; }

  public List<Long> getReadLatency() { return mReadLatency; }

  public List<Long> getWriteLatency() { return mWriteLatency; }

  private void initSyncBarrier() throws IOException {
    mFileSystem.create(mWorkDir + "/sync/" + mTaskId + "-" + mId);
  }

  private void syncBarrier() throws IOException {
    String syncDirPath = mWorkDir + "/sync";
    String syncFileName = mTaskId + "-" + mId;
    mFileSystem.delete(syncDirPath + "/" + syncFileName, false);
    while (!mFileSystem.list(syncDirPath).isEmpty()) {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        LOG.error("Error in Sync Barrier", e);
      }
    }
  }

  public void run() {
    long basicBytes = 0;
    long basicTimeMs = 0;
    long readBytes = 0;
    long readTimeMs = 0;
    long writeBytes = 0;
    long writeTimeMs = 0;
    clearHistory();
    mSuccess = true;

    String tmpDir = mWorkDir + "/tmp";
    String dataDir;
    if (mShuffle) {
      dataDir = mWorkDir + "/data";
    } else {
      dataDir = mWorkDir + "/data/" + mTaskId;
    }

    long tTimeMs = System.currentTimeMillis();
    for (int b = 0; b < mBasicFilesNum; b ++) {
      long startTime = System.currentTimeMillis();
      try {
        String fileName = mTaskId + "-" + mId + "-" + b;
        Operators.writeSingleFile(mFileSystem, dataDir + "/" + fileName, mFileLength, mBufferSize,
            "MassiveBASIC", mTaskId, mId);
        basicBytes += mFileLength;
      } catch (IOException e) {
        LOG.error("Failed to write basic file", e);
        mSuccess = false;
        break;
      }
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      mBasicLatency.add(duration);
      LOG.info("HeART-DFS-PERF-Metrics-MassiveBASIC-FILE || " + mTaskId + "," + mId + ","
          + startTime + "," + endTime + "," + duration + ","
          + mFileLength + "," + ((mFileLength / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
    }
    tTimeMs = System.currentTimeMillis() - tTimeMs;
    basicTimeMs += tTimeMs;

    try {
      syncBarrier();
    } catch (IOException e) {
      LOG.error("Error in Sync Barrier", e);
      mSuccess = false;
    }
    int index = 0;
    long limitTimeMs = System.currentTimeMillis() + mLimitTimeMs;
    long startTime = 0;
    while ((tTimeMs = System.currentTimeMillis()) < limitTimeMs) {
      if (mRand.nextBoolean()) { // read
        long nbytes = 0;
        try {
          List<String> candidates = mFileSystem.list(dataDir);
          String readFilePath = ListGenerator.generateRandomReadFiles(1, candidates).get(0);
          startTime = System.currentTimeMillis();
          nbytes = Operators.readSingleFile(mFileSystem, readFilePath, mBufferSize,
              "MassiveREAD", mTaskId, mId);
          readBytes += nbytes;
        } catch (IOException e) {
          LOG.error("Failed to read file", e);
          mSuccess = false;
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        readTimeMs += duration;
        mReadLatency.add(duration);
        LOG.info("HeART-DFS-PERF-Metrics-MassiveREAD-FILE || " + mTaskId + "," + mId + ","
            + startTime + "," + endTime + "," + duration + ","
            + nbytes + "," + ((nbytes / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
      } else { // write
        try {
          String writeFileName = mTaskId + "-" + mId + "--" + index;
          startTime = System.currentTimeMillis();
          Operators.writeSingleFile(mFileSystem, tmpDir + "/" + writeFileName, mFileLength,
              mBufferSize, "MassiveWrite", mTaskId, mId);
          writeBytes += mFileLength;
          mFileSystem.rename(tmpDir + "/" + writeFileName, dataDir + "/" + writeFileName);
        } catch (IOException e) {
          LOG.error("Failed to write file", e);
          mSuccess = false;
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        writeTimeMs += duration;
        mWriteLatency.add(duration);
        LOG.info("HeART-DFS-PERF-Metrics-MassiveWRITE-FILE || " + mTaskId + "," + mId + ","
            + startTime + "," + endTime + "," + duration + ","
            + mFileLength + "," + ((mFileLength / 1024.0 / 1024.0) / (duration / 1000.0)) + " ||");
      }
      index ++;
    }

    mBasicWriteThroughput =
        (basicBytes == 0) ? 0 : (basicBytes / 1024.0 / 1024.0) / (basicTimeMs / 1000.0);
    mReadThroughput = (readBytes == 0) ? 0 : (readBytes / 1024.0 / 1024.0) / (readTimeMs / 1000.0);
    mWriteThroughput =
        (writeBytes == 0) ? 0 : (writeBytes / 1024.0 / 1024.0) / (writeTimeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mRand = new Random(System.currentTimeMillis() + mTaskId + mId);
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mFileLength = taskConf.getLongProperty("file.length.bytes");
    mLimitTimeMs = taskConf.getLongProperty("time.seconds") * 1000;
    mShuffle = taskConf.getBooleanProperty("shuffle.mode");
    mWorkDir = taskConf.getProperty("work.dir");
    mBasicFilesNum = taskConf.getIntProperty("basic.files.per.thread");
    try {
      mFileSystem = Operators.connect(PerfConf.get().DFS_ADDRESS, taskConf);
      initSyncBarrier();
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mBasicWriteThroughput = 0;
    mReadThroughput = 0;
    mWriteThroughput = 0;
    mBasicLatency = new ArrayList<Long>();
    mReadLatency = new ArrayList<Long>();
    mWriteLatency = new ArrayList<Long>();
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
