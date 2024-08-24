package pasalab.dfs.perf.benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import pasalab.dfs.perf.basic.PerfTaskContext;
import pasalab.dfs.perf.basic.PerfTotalReport;

public class SimpleTotalReport extends PerfTotalReport {
  protected String mFailedSlaves = "";
  protected int mFailedTasks = 0;
  protected long mId = Long.MAX_VALUE;
  protected int mSlavesNum;
  protected List<Map<String, Map<String, List<Double>>>> mStatistics;
  protected List<Map<String, Map<String, List<Long>>>> mLatencies;

  protected HashSet<String> mSlaves;
  protected List<Map<String, String>> mConf;

  @Override
  public void initialFromTaskContexts(PerfTaskContext[] taskContexts, int iterations) throws IOException {
    mSlavesNum = taskContexts.length / iterations;
    mSlaves = new HashSet<String>(mSlavesNum);
    mConf = new ArrayList<Map<String, String>>(iterations);
    mStatistics = new ArrayList<Map<String, Map<String, List<Double>>>>(iterations);
    mLatencies = new ArrayList<Map<String, Map<String, List<Long>>>>(iterations);

    for (int i = 0; i < iterations; i++) {
      mStatistics.add(new HashMap<String, Map<String, List<Double>>>());
      mLatencies.add(new HashMap<String, Map<String, List<Long>>>());
      mConf.add(new HashMap<String, String>());
    }

    for (PerfTaskContext taskContext : taskContexts) {
      SimpleTaskContext context = (SimpleTaskContext) taskContext;
      String slaveName = context.getId() + "@" + context.getNodeName();
      mSlaves.add(slaveName);

      int currentIter = context.getIteration();
      mConf.set(currentIter, context.getConf());
      Map<String, Map<String, List<Double>>> slaveStatistics = mStatistics.get(currentIter);
      slaveStatistics.put(slaveName, context.getAdditiveStatistics());
      Map<String, Map<String, List<Long>>> slaveLatencies = mLatencies.get(currentIter);
      slaveLatencies.put(slaveName, context.getAggregateStatistics());

      if (context.getStartTimeMs() < mId) {
        mId = context.getStartTimeMs();
      }
      if (!context.getSuccess()) {
        mFailedTasks ++;
        mFailedSlaves += context.getId() + "-" + currentIter + "@" + context.getNodeName() + " ";
      }
    }
  }

  private String generateSlaveDetails(int iteration, String slaveName) {
    StringBuffer sbSlaveDetail = new StringBuffer();
    Map<String, List<Double>> statistic = mStatistics.get(iteration).get(slaveName);
    for (Map.Entry<String, List<Double>> entry : statistic.entrySet()) {
      sbSlaveDetail.append(slaveName).append("'s ").append(entry.getKey())
          .append(" for each threads:\n\t");
      for (Double d : entry.getValue()) {
        sbSlaveDetail.append("[ ").append(d).append(" ]");
      }
      sbSlaveDetail.append("\n");
    }
    sbSlaveDetail.append("\n");
    Map<String, List<Long>> latencies = mLatencies.get(iteration).get(slaveName);
    for (Map.Entry<String, List<Long>> entry : latencies.entrySet()) {
      sbSlaveDetail.append(slaveName).append("'s ").append(entry.getKey())
          .append(" for each threads:\n\t");
      for (Long v : entry.getValue()) {
        sbSlaveDetail.append("[ ").append(v).append(" ]");
      }
      sbSlaveDetail.append("\n");
    }
    sbSlaveDetail.append("\n");
    return sbSlaveDetail.toString();
  }

  private String generateTaskConf(int iteration) {
    StringBuffer sbReadConf = new StringBuffer();
    Map<String, String> currentConf = mConf.get(iteration);
    for (Map.Entry<String, String> entry : currentConf.entrySet()) {
      sbReadConf.append(entry.getKey()).append("\t").append(entry.getValue()).append("\n");
    }
    return sbReadConf.toString();
  }

  private String generateStatistics(int iteration) {
    StringBuffer sbStatistics = new StringBuffer("SlaveName");
    Map<String, Map<String, List<Double>>> slaveStatistics = mStatistics.get(iteration);
    Map<String, List<Double>> sample = slaveStatistics.values().iterator().next();
    int size = sample.size();
    List<String> names = new ArrayList<String>(size);
    List<Double> totals = new ArrayList<Double>(size);
    for (String name : sample.keySet()) {
      names.add(name);
      totals.add(0.0);
      sbStatistics.append("\t").append(name);
    }
    sbStatistics.append("\n");
    for (Map.Entry<String, Map<String, List<Double>>> slaveReport : slaveStatistics.entrySet()) {
      sbStatistics.append(slaveReport.getKey());
      Map<String, List<Double>> statistic = slaveReport.getValue();
      for (int t = 0; t < size; t ++) {
        List<Double> threadDetails = statistic.get(names.get(t));
        double sum = 0;
        for (Double d : threadDetails) {
          sum += d;
        }
        sbStatistics.append("\t").append(sum);
        totals.set(t, totals.get(t) + sum);
      }
      sbStatistics.append("\n");
    }
    sbStatistics.append("Total");
    for (Double total : totals) {
      sbStatistics.append("\t").append(total);
    }
    sbStatistics.append("\n");
    return sbStatistics.toString();
  }

  private int getPercentile(double percentile, double totalCount) {
    return (int) Math.ceil(percentile / 100.0 * totalCount) - 1;
  }

  private String generateLatencyReport(int iteration) {
    StringBuffer sbLatencies = new StringBuffer("SlaveName");
    List<Integer> percentiles = Arrays.asList(50, 95, 99);
    Map<String, Map<String, List<Long>>> slaveStatistics = mLatencies.get(iteration);
    Map<String, List<Long>> sample = slaveStatistics.values().iterator().next();
    int size = sample.size();
    List<String> names = new ArrayList<String>(size);
    for (String name : sample.keySet()) {
      names.add(name);
      for (Integer percentile : percentiles) {
        sbLatencies.append("\t").append(percentile).append("th ").append(name);
      }
    }
    sbLatencies.append("\n");
    for (Map.Entry<String, Map<String, List<Long>>> slaveReport : slaveStatistics.entrySet()) {
      sbLatencies.append(slaveReport.getKey());
      Map<String, List<Long>> statistic = slaveReport.getValue();
      for (int t = 0; t < size; t ++) {
        List<Long> threadDetails = statistic.get(names.get(t));
        Collections.sort(threadDetails);
        int count = threadDetails.size();
        for (Integer percentile : percentiles) {
          int index = getPercentile((double)percentile, (double)count);
          sbLatencies.append("\t").append(threadDetails.get(index));
        }
      }
      sbLatencies.append("\n");
    }
    return sbLatencies.toString();
  }

  @Override
  public void writeToFile(File file, int iterations) throws IOException {
    BufferedWriter fout = new BufferedWriter(new FileWriter(file));
    fout.write(mTestCase + " Test - ID : " + mId + "\n");
    if (mFailedTasks == 0) {
      fout.write("Finished Successfully\n");
    } else {
      fout.write("Failed: " + mFailedTasks + " slaves failed ( " + mFailedSlaves + ")\n");
    }
    for (int iter = 0; iter < iterations; iter++) {
      fout.write("Iteration: " + iter);
      fout.write("********** Task Configurations **********\n");
      fout.write(generateTaskConf(iter));
      fout.write("********** Statistics **********\n");
      fout.write(generateStatistics(iter));
      fout.write("********** Latencies **********\n");
      fout.write(generateLatencyReport(iter));
      fout.write("********** Slave Details **********\n");
      for (String slave : mSlaves) {
        fout.write(generateSlaveDetails(iter, slave));
      }
    }
    fout.close();
  }

}
