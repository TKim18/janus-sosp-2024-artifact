package pasalab.dfs.perf.benchmark.transcode;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ECSchema;
import pasalab.dfs.perf.basic.FileMetadata;
import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.RedundancyStatus;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.benchmark.Operators;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class TranscodeThread extends PerfThread {
  private PerfFileSystem mFileSystem;
  private String mBaseDirectory;
  private int mBufferSize;
  private boolean isBaseline;
  private AtomicBoolean terminated;
  private ConcurrentLinkedDeque<FileMetadata> files;

  public void run() {
    // this background thread transcodes files, pull file metadata from files
    // and begin transcoding it, decide target scheme based on current scheme and some probability
    // there will be multiple transcode threads, each iteratively transcoding
    FileMetadata file;
    Random generator = new Random(128);
    int transcodingCount = 0;
    int batchSize = 5;
    int intervalInSeconds = 5;
    int initialDelayInSeconds = 5;
    int maxBufferSize = 1024 * 1024 * 40;
    int t,b,success;
    byte[] buffer = new byte[maxBufferSize];
    LOG.info("Starting transcode thread at task " + mTaskId + " and id " + mId);
    LOG.info("Writing to base directory " + mBaseDirectory + " which is baseline=" +isBaseline);

    long lastCleared = 0;
    // we transcode in batches, we can determine number of threads doing transcodings
    List<FileMetadata> targets = new ArrayList<FileMetadata>(); // transcoding targets
    byte[] result = new byte[100];
    try {
      Thread.sleep(1000 * initialDelayInSeconds);
      // keep running until killed
      while (true) {
        if (terminated.get()) {
          LOG.info("Got a termination signal, ending transcode loop");
          break;
        }
        // want to drop all datanode caches at this point
        if (mId % 2 == 0 && (System.currentTimeMillis() - lastCleared) > 30000) {
          lastCleared = System.currentTimeMillis();
          // only one node for every client should execute
          Process p = Runtime.getRuntime().exec("/bin/bash /proj/HeARTy/ceridwen-sosp-2024-artifact/scripts/clear_cache.sh");
          p.waitFor();
          if (p.getErrorStream().available() > 0) {
            p.getErrorStream().read(result);
            LOG.info(new String(result, StandardCharsets.UTF_8 ));
          }
          if (p.getInputStream().available() > 0) {
            LOG.info("Successfully dropped buffer caches");
          }
        }

        // pause for a bit after clearing buffer cache before doing a batch of transcodings
        Thread.sleep(1000 * intervalInSeconds);

        for (b = 0; b < batchSize;) {
          file = files.pollFirst();
          if (file == null) {
            break;
          }
          targets.add(file);
          b++;
        }
        LOG.info("Scheduled " + b + " transcoding operations out of max batch " + batchSize +
            ". There are " + files.size() + " transcoding operations in the queue.");

        success = 0;
        for (t = 0; t < targets.size(); t++) {
          if (doTranscode(targets.get(t), buffer, generator)) {
            success++;
          }
        }
        transcodingCount += success;
        targets.clear();
        LOG.info("Completed " + success + " transcoding operations in this batch of " + b
            + ". " + files.size() + " remaining transcoding operations in the queue");
        LOG.info("This thread has completed a total of " + transcodingCount + " transcoding events.");
      }
    } catch (Exception e) {
      LOG.warn("Some kind of exception on the transcoding thread occurred " + e.getMessage());
    }
    // terminated.set(true);
  }

  private boolean doTranscode(FileMetadata file, byte[] buffer, Random generator) {
    final long start = System.currentTimeMillis();
    RedundancyStatus targetStatus;
    String targetPath;
    FileMetadata targetFile;

    // determine target redundancy scheme and directory
    targetStatus = determineNextStage(generator, file.status);
    if (targetStatus == RedundancyStatus.NONE) {
      LOG.warn("Something terrible happened, how did that get there?");
      return true;
    }
    LOG.info("Initiating transcode for" + file.getFullPath()
        + " with current state " + file.status.name()
        + " to target state " + targetStatus.name());

    try {
      if (isBaseline) {
        // baseline transcode first reads all the data
        final long read = System.currentTimeMillis();
        long n = Operators.readFileIntoBuffer(mFileSystem, file.getFullPath(), buffer);
        final long write = System.currentTimeMillis();
        if (n > 0) {
          // then write what's in buffer back out
          targetPath = determineTargetPath(targetStatus);
          Operators.writeSingleFile(mFileSystem, targetPath + "/" + file.fileName,
              file.fileSize, mBufferSize, "TranscodeWrite", mTaskId, mId);
          // and delete the old file
          mFileSystem.delete(file.getFullPath(), false);
          targetFile = new FileMetadata(file.fileName, targetPath, file.fileSize, targetStatus);
          LOG.info("Baseline transcode metrics | read=" + (write-read)
              + " ms and write=" + (System.currentTimeMillis() - write) + " ms.");
        } else {
          LOG.error("Unable to read all bytes from file, skipping re-write");
          return false;
        }
      } else {
        // janus transcode is a metadata op call to Namenode via rename op
        mFileSystem.rename(file.getFullPath(), "transcode_" + determineEcPolicyName(targetStatus));
        targetFile = new FileMetadata(file.fileName, file.filePath, file.fileSize, targetStatus);
      }
      LOG.info("Completed transcode for " + file.getFullPath() + " from "
          + file.status.name() + " to " + targetFile.status.name() + " and path "
          + targetFile.getFullPath() + " in " + (System.currentTimeMillis() - start) + " ms.");

      // ignore files on their last leg
      if (targetFile.status == RedundancyStatus.EC203) {
        LOG.info(file.getFullPath() + " has reached end of its path. Salute!");
      } else {
        files.addLast(targetFile);
      }
      return true;
    } catch (IOException e) {
      LOG.error("Issue while transcoding " + e.getMessage());
      return false;
    }
  }

  private RedundancyStatus determineNextStage(Random generator, RedundancyStatus currentStatus) {
    // determine next transcoding phase
    // order goes repl/hybrid --> EC53 --> EC103 --> EC203
    if (currentStatus == RedundancyStatus.HYBRID || currentStatus == RedundancyStatus.REPL) {
      return RedundancyStatus.EC53;
    } else if (currentStatus == RedundancyStatus.EC53) {
      return RedundancyStatus.EC103;
    } else if (currentStatus == RedundancyStatus.EC103){
      return RedundancyStatus.EC203;
    } else {
      return RedundancyStatus.NONE;
    }
  }

  private String determineTargetPath(RedundancyStatus newStatus) {
    if (newStatus == RedundancyStatus.EC53) {
      return "/ec53rs/simple-read-write/" + mTaskId;
    } else if (newStatus == RedundancyStatus.EC103) {
      return "/ec103rs/simple-read-write/" + mTaskId;
    } else if (newStatus == RedundancyStatus.EC203) {
      return "/ec203rs/simple-read-write/" + mTaskId;
    } else {
      return null;
    }
  }

  private String determineEcPolicyName(RedundancyStatus newStatus) {
    ECSchema schema;
    if (newStatus == RedundancyStatus.EC53) {
      schema = new ECSchema("RS", 5, 3);
    } else if (newStatus == RedundancyStatus.EC103) {
      schema = new ECSchema("RS", 10, 3);
    } else if (newStatus == RedundancyStatus.EC203) {
      schema = new ECSchema("RS", 20, 3);
    } else {
      LOG.warn("Strange thing happened");
      schema = new ECSchema("RS", 2, 1);
    }
    ErasureCodingPolicy ecPolicy = new ErasureCodingPolicy(schema, 1024*1024*8);
    return ecPolicy.getName();
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    // don't need to do much
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    try {
      mFileSystem = Operators.connect(PerfConf.get().DFS_ADDRESS, taskConf);
      mBaseDirectory = taskConf.getProperty("write.dir");
      isBaseline = !mBaseDirectory.startsWith("/ec");
    } catch (IOException e) {
      LOG.error("Failed to setup transcoding thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
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

  public boolean getSuccess() {
    return true;
  }
}

