package pasalab.dfs.perf.benchmark.mixture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.benchmark.SimpleTaskContext;

public class MixtureTaskContext extends SimpleTaskContext {
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
      if (!((MixtureThread) thread).getSuccess()) {
        mSuccess = false;
      }
      basicThroughputs.add(((MixtureThread) thread).getBasicWriteThroughput());
      readThroughputs.add(((MixtureThread) thread).getReadThroughput());
      writeThroughputs.add(((MixtureThread) thread).getWriteThroughput());
      basicLatency.addAll(((MixtureThread) thread).getBasicLatency());
      readLatency.addAll(((MixtureThread) thread).getReadLatency());
      writeLatency.addAll(((MixtureThread) thread).getWriteLatency());
    }
    mAdditiveStatistics.put("BasicWriteThroughput(MB/s)", basicThroughputs);
    mAdditiveStatistics.put("ReadThroughput(MB/s)", readThroughputs);
    mAdditiveStatistics.put("WriteThroughput(MB/s)", writeThroughputs);
    mAggregateStatistics.put("BasicWriteLatency(ms)", basicLatency);
    mAggregateStatistics.put("ReadLatency(ms)", readLatency);
    mAggregateStatistics.put("WriteLatency(ms)", writeLatency);
  }
}
