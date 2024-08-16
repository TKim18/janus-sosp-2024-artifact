package pasalab.dfs.perf.benchmark.metadata;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.benchmark.Operators;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

public class MetadataThread extends PerfThread {

  private PerfFileSystem[] mClients;
  private int mClientsNum;
  private int mOpTimeMs;
  private String mWorkDir;

  private double mRate; // in ops/sec
  private boolean mSuccess;
  private List<Long> mLatency;

  public void clearHistory () {
    mRate = 0.0;
    mLatency = new ArrayList<Long>();
  }

  public double getRate() {
    return mRate;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public List<Long> getLatency() { return mLatency; }

  public void run() {
    int currentOps = 0;
    int nextClient = 0;
    mSuccess = true;
    clearHistory();
    long timeMs = System.currentTimeMillis();
    while ((System.currentTimeMillis() - timeMs) < mOpTimeMs) {
      long startTime = System.currentTimeMillis();
      try {
        currentOps += Operators.metadataSample(mClients[nextClient], mWorkDir + "/" + mId);
      } catch (IOException e) {
        LOG.error("Failed to do metadata operation", e);
        mSuccess = false;
      }
      long duration = System.currentTimeMillis() - startTime;
      nextClient = (nextClient + 1) % mClientsNum;
      mLatency.add(duration);
    }
    timeMs = System.currentTimeMillis() - timeMs;
    mRate = (currentOps / 1.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mClientsNum = taskConf.getIntProperty("clients.per.thread");
    mOpTimeMs = taskConf.getIntProperty("op.second.per.thread") * 1000;
    mWorkDir = taskConf.getProperty("work.dir");
    try {
      mClients = new PerfFileSystem[mClientsNum];
      String dfsAddress = PerfConf.get().DFS_ADDRESS;
      for (int i = 0; i < mClientsNum; i++) {
        mClients[i] = Operators.connect(dfsAddress, taskConf);
      }
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mRate = 0;
    mSuccess = false;
    mLatency = new ArrayList<Long>();
    return true;
  }

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf, boolean closeFileSystem) {
    if (closeFileSystem) {
      for (int i = 0; i < mClientsNum; i++) {
        try {
          Operators.close(mClients[i]);
        } catch (IOException e) {
          LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
        }
      }
      return true;
    } else {
      return true;
    }
  }
}
