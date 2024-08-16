package pasalab.dfs.perf.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pasalab.dfs.perf.basic.PerfTaskContext;
import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.conf.PerfConf;

public class SimpleTaskContext extends PerfTaskContext {
  protected Map<String, String> mConf;

  protected Map<String, List<Double>> mAdditiveStatistics = null;

  protected Map<String, List<Long>> mAggregateStatistics = null;

  @Override
  public void initial(int id, String nodeName, String testCase, TaskConfiguration taskConf) {
    super.initial(id, nodeName, testCase, taskConf);
    mConf = taskConf.getAllProperties();
    mConf.put("dfs.perf.dfs.address", PerfConf.get().DFS_ADDRESS);
  }

  public Map<String, String> getConf() {
    return mConf;
  }

  public Map<String, List<Double>> getAdditiveStatistics() {
    return mAdditiveStatistics;
  }

  public Map<String, List<Long>> getAggregateStatistics() {
    return mAggregateStatistics;
  }

  @Override
  public void loadFromFile(File file) throws IOException {
    BufferedReader fin = new BufferedReader(new FileReader(file));
    mTestCase = fin.readLine();
    mId = Integer.parseInt(fin.readLine());
    mIteration = Integer.parseInt(fin.readLine());
    mNodeName = fin.readLine();
    mSuccess = Boolean.parseBoolean(fin.readLine());
    mStartTimeMs = Long.parseLong(fin.readLine());
    mFinishTimeMs = Long.parseLong(fin.readLine());
    int confSize = Integer.parseInt(fin.readLine());
    mConf = new HashMap<String, String>(confSize);
    for (int i = 0; i < confSize; i ++) {
      mConf.put(fin.readLine(), fin.readLine());
    }
    int sSize = Integer.parseInt(fin.readLine());
    mAdditiveStatistics = new HashMap<String, List<Double>>(sSize);
    for (int i = 0; i < sSize; i ++) {
      String key = fin.readLine();
      int lSize = Integer.parseInt(fin.readLine());
      List<Double> value = new ArrayList<Double>(lSize);
      for (int j = 0; j < lSize; j ++) {
        value.add(Double.parseDouble(fin.readLine()));
      }
      mAdditiveStatistics.put(key, value);
    }
    sSize = Integer.parseInt(fin.readLine());
    mAggregateStatistics = new HashMap<String, List<Long>>(sSize);
    for (int i = 0; i < sSize; i ++) {
      String key = fin.readLine();
      int lSize = Integer.parseInt(fin.readLine());
      List<Long> value = new ArrayList<Long>(lSize);
      for (int j = 0; j < lSize; j ++) {
        value.add(Long.parseLong(fin.readLine()));
      }
      mAggregateStatistics.put(key, value);
    }
    fin.close();
    return;
  }

  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>();
    List<Double> statistic = new ArrayList<Double>(threads.length);
    for (int i = 0; i < threads.length; i ++) {
      statistic.add(0.0);
    }
    mAdditiveStatistics.put("Statistic", statistic);
    mAggregateStatistics = new HashMap<String, List<Long>>();
    List<Long> latency = new ArrayList<Long>(threads.length);
    mAggregateStatistics.put("Latency", latency);

  }

  @Override
  public void writeToFile(File file) throws IOException {
    BufferedWriter fout = new BufferedWriter(new FileWriter(file));
    fout.write(mTestCase + "\n");
    fout.write(mId + "\n");
    fout.write(mIteration + "\n");
    fout.write(mNodeName + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mConf.size() + "\n");
    for (Map.Entry<String, String> entry : mConf.entrySet()) {
      fout.write(entry.getKey() + "\n");
      fout.write(entry.getValue() + "\n");
    }

    if (mAdditiveStatistics != null) {
      fout.write(mAdditiveStatistics.size() + "\n");
      for (Map.Entry<String, List<Double>> entry : mAdditiveStatistics.entrySet()) {
        fout.write(entry.getKey() + "\n");
        List<Double> value = entry.getValue();
        fout.write(value.size() + "\n");
        for (Double ele : value) {
          fout.write(ele + "\n");
        }
      }
    }

    if (mAggregateStatistics != null) {
      fout.write(mAggregateStatistics.size() + "\n");
      for (Map.Entry<String, List<Long>> entry : mAggregateStatistics.entrySet()) {
        fout.write(entry.getKey() + "\n");
        List<Long> value = entry.getValue();
        fout.write(value.size() + "\n");
        for (Long ele : value) {
          fout.write(ele + "\n");
        }
      }
    }
    fout.close();
  }
}
