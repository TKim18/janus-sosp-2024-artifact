package pasalab.dfs.perf.benchmark.simpleread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.benchmark.SimpleTaskContext;

public class SimpleReadTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(1);
    mAggregateStatistics = new HashMap<String, List<Long>>(1);
    List<Double> throughputs = new ArrayList<Double>(threads.length);
    List<Long> latencies = new ArrayList<Long>();
    for (PerfThread thread : threads) {
      if (!((SimpleReadThread) thread).getSuccess()) {
        mSuccess = false;
      }
      throughputs.add(((SimpleReadThread) thread).getThroughput());
      latencies.addAll(((SimpleReadThread) thread).getLatency());
    }
    mAdditiveStatistics.put("ReadThroughput(MB/s)", throughputs);
    mAggregateStatistics.put("Latency(ms)", latencies);
  }
}
