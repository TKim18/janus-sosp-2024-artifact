package pasalab.dfs.perf.basic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import pasalab.dfs.perf.PerfConstants;
import pasalab.dfs.perf.benchmark.simplewrite.SimpleWriteThread;
import pasalab.dfs.perf.benchmark.transcode.TranscodeThread;
import pasalab.dfs.perf.conf.PerfConf;

/**
 * The abstract class for all the test tasks. For new test, you should create a new class which
 * extends this.
 */
public abstract class PerfTask {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected TaskConfiguration mTaskConf;
  protected String mTestCase;

  private PerfThread[] mThreads;

  public void initialSet(int id, String nodeName, TaskConfiguration taskConf, String testCase) {
    mId = id;
    mNodeName = nodeName;
    mTaskConf = taskConf;
    mTestCase = testCase;
  }

  /**
   * If you want to cleanup certain work directory after the whole test finished, return in here.
   * Otherwise return null.
   * 
   * @return the work directory to cleanup, otherwise null;
   */
  public abstract String getCleanupDir();

  /**
   * Setup the task. Do some preparations.
   * 
   * @param taskContext The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean setupTask(PerfTaskContext taskContext);

  /**
   * Cleanup the task. Do some following work.
   * 
   * @param taskContext The statistics of this task
   * @return true if cleanup successfully, false otherwise
   */
  protected abstract boolean cleanupTask(PerfTaskContext taskContext);

  public boolean setup(PerfTaskContext taskContext) {
    taskContext.setStartTimeMs(System.currentTimeMillis());
    boolean ret = setupTask(taskContext);
    // add transcoding treads
    int numTranscodingThreads = 2;
    mThreads = new PerfThread[PerfConf.get().THREADS_NUM + numTranscodingThreads];
    ConcurrentLinkedDeque<FileMetadata> files = new ConcurrentLinkedDeque<FileMetadata>();
    AtomicBoolean terminated = new AtomicBoolean(false);
    try {
      for (int i = 0; i < mThreads.length; i ++) {
        if (i < PerfConf.get().THREADS_NUM) {
          mThreads[i] = TestCase.get().getTaskThreadClass(mTestCase);
          if (mThreads[i] instanceof SimpleWriteThread) {
            // set deque
            ((SimpleWriteThread) mThreads[i]).setConcurrentFileMap(files);
            ((SimpleWriteThread) mThreads[i]).setTerminationFlag(terminated);
            LOG.info("Write thread init at " + i);
          }
        } else {
          mThreads[i] = TestCase.get().getTaskThreadClass("Transcode");
          if (mThreads[i] instanceof TranscodeThread) {
            // set deque
            ((TranscodeThread) mThreads[i]).setConcurrentFileMap(files);
            ((TranscodeThread) mThreads[i]).setTerminationFlag(terminated);
            LOG.info("Transcode thread init at " + i);
          }
        }
        mThreads[i].initialSet(i, mId, mNodeName, mTestCase);
        ret &= mThreads[i].setupThread(mTaskConf);
      }
    } catch (Exception e) {
      LOG.error("Error to create task thread", e);
      return false;
    }
    return ret;
  }

  public boolean run(PerfTaskContext taskContext, int iteration) {
    List<Thread> threadList = new ArrayList<Thread>(mThreads.length);
    taskContext.setIteration(iteration);
    LOG.info("HeART-DFS-PERF-General || Start running || "+ taskContext.getId() + "," + iteration);
    try {
      for (int i = 0; i < mThreads.length; i ++) {
        Thread t = new Thread(mThreads[i]);
        threadList.add(t);
      }
      for (Thread t : threadList) {
        t.start();
      }
      for (Thread t : threadList) {
        t.join();
      }
      LOG.info("HeART-DFS-PERF-General || Stop running || "+ taskContext.getId() + "," + iteration);
    } catch (InterruptedException e) {
      LOG.error("Error when wait all threads", e);
      LOG.info("HeART-DFS-PERF-General || Stop running || "+ taskContext.getId() + "," + iteration);
      return false;
    } catch (Exception e) {
      LOG.error("Error to create task thread", e);
      LOG.info("HeART-DFS-PERF-General || Stop running || "+ taskContext.getId() + "," + iteration);
      return false;
    }
    return true;
  }

  public boolean cleanup(PerfTaskContext taskContext, boolean closeFileSystem) {
    boolean ret = true;
    for (int i = 0; i < mThreads.length; i ++) {
      ret &= mThreads[i].cleanupThread(mTaskConf, closeFileSystem);
    }
    ret &= cleanupTask(taskContext);
    taskContext.setFromThread(mThreads);
    taskContext.setFinishTimeMs(System.currentTimeMillis());
    try {
      String outDirPath = PerfConf.get().OUT_FOLDER;
      File outDir = new File(outDirPath);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      String reportFileName =
          outDirPath + "/" + PerfConstants.PERF_CONTEXT_FILE_NAME_PREFIX + mTestCase + "-" + mId
              + "-" + taskContext.getIteration() + "@" + mNodeName;
      taskContext.writeToFile(new File(reportFileName));
    } catch (IOException e) {
      LOG.error("Error when generate the task report", e);
      ret = false;
    }
    return ret;
  }
}
