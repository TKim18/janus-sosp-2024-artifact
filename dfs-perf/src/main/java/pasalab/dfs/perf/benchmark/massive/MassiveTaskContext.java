package pasalab.dfs.perf.benchmark.massive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.benchmark.SimpleTaskContext;

public class MassiveTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(3);
    mAggregateStatistics = new HashMap<String, List<Long>>(3);
    List<Double> basicThroughputs = new ArrayList<Double>(threads.length);
    List<Double> readThroughputs = new ArrayList<Double>(threads.length);
    List<Double> writeThroughputs = new ArrayList<Double>(threads.length);
    List<Long> basicLatency = new ArrayList<Long>();
    List<Long> readLatency = new ArrayList<Long>();
    List<Long> writeLatency = new ArrayList<Long>();
    for (PerfThread thread : threads) {
      if (!((MassiveThread) thread).getSuccess()) {
        mSuccess = false;
      }
      basicThroughputs.add(((MassiveThread) thread).getBasicWriteThroughput());
      readThroughputs.add(((MassiveThread) thread).getReadThroughput());
      writeThroughputs.add(((MassiveThread) thread).getWriteThroughput());
      basicLatency.addAll(((MassiveThread) thread).getBasicLatency());
      readLatency.addAll(((MassiveThread) thread).getReadLatency());
      writeLatency.addAll(((MassiveThread) thread).getWriteLatency());
    }
    mAdditiveStatistics.put("BasicWriteThroughput(MB/s)", basicThroughputs);
    mAdditiveStatistics.put("ReadThroughput(MB/s)", readThroughputs);
    mAdditiveStatistics.put("WriteThroughput(MB/s)", writeThroughputs);
    mAggregateStatistics.put("BasicWriteLatency(ms)", basicLatency);
    mAggregateStatistics.put("ReadLatency(ms)", readLatency);
    mAggregateStatistics.put("WriteLatency(ms)", writeLatency);
  }
}
