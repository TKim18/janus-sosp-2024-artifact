/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;


/**
 * This class supports writing files in striped layout and erasure coded format.
 * Each stripe contains a sequence of cells.
 */
@InterfaceAudience.Private
public class DFSStripedOutputStream extends DFSOutputStream
  implements StreamCapabilities {
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  /**
   * OutputStream level last exception, will be used to indicate the fatal
   * exception of this stream, i.e., being aborted.
   */
  private final ExceptionLastSeen exceptionLastSeen = new ExceptionLastSeen();

  /** Information on a redundancy group's position within a stripe. */
  static class ExtendedBlockWithPosition extends ExtendedBlock {
    private int pos; // the relative position (1st, 2nd, 3rd, etc... block group in the stripe)
    private int startPosition; // the starting index in streamers
    private int endPosition;

    ExtendedBlockWithPosition(ExtendedBlock b) {
      super(b);
    }
  }

  /** Buffers for writing the data and parity cells of a stripe. */
  class CellBuffers {
    private final ByteBuffer[] buffers;
    private final byte[][] checksumArrays;
    private int indexLastSet;
    private int indexFirstSet;

    CellBuffers(int numParityBlocks) {
      if (cellSize % bytesPerChecksum != 0) {
        throw new HadoopIllegalArgumentException("Invalid values: "
          + HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (="
          + bytesPerChecksum + ") must divide cell size (=" + cellSize + ").");
      }

      checksumArrays = new byte[numParityBlocks][];
      final int size = getChecksumSize() * (cellSize / bytesPerChecksum);
      for (int i = 0; i < checksumArrays.length; i++) {
        checksumArrays[i] = new byte[size];
      }

      buffers = new ByteBuffer[numDataBlocksInStripe + numParityBlocksInGroup];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(useDirectBuffer(), cellSize);
        buffers[i].limit(cellSize);
      }
    }

    private ByteBuffer[] getBuffers() {
      return buffers;
    }

    private int getFirstSetIndex() {
      return indexFirstSet;
    }

    byte[] getChecksumArray(int i) {
      return checksumArrays[i - numDataBlocksInStripe];
    }

    private int addTo(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      indexLastSet = i;
      return pos;
    }

    private void clear() {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
      }
      indexFirstSet = (indexLastSet + 1) % numDataBlocksInStripe;
    }

    private void release() {
      for (int i = 0; i < buffers.length; i++) {
        if (buffers[i] != null) {
          BUFFER_POOL.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < numDataBlocksInStripe; i++) {
        buffers[i].flip();
      }
    }
  }

  /** Buffers for partial parities from groups that span more than one stripes */
  class PartialParityBuffers {
    private final ByteBuffer[] buffers;
    private final ByteBuffer[] cellBuffers;
    private final int[] cellsPerRow; // the number of cells used to encode that row's parity

    PartialParityBuffers() {
      // There will be at most one group's worth of partial parities to buffer.
      buffers = new ByteBuffer[numParityBlocksInGroup];
      cellBuffers = new ByteBuffer[numParityBlocksInGroup];
      cellsPerRow = new int[numCellsInBlock];
      int size = (int) blockSize;
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(useDirectBuffer(), size);
        buffers[i].limit(size);
      }
    }

    private ByteBuffer[] getCellBuffers(int atRow) {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i].position(atRow * cellSize);
        buffers[i].limit(buffers[i].position() + cellSize);
        cellBuffers[i] = buffers[i].slice();
      }
      return cellBuffers;
    }

    private void clearCells(int atRow) {
      for (int i = 0; i < cellBuffers.length; i++) {
        cellBuffers[i].clear();
        cellBuffers[i].limit(cellSize);
      }
      cellsPerRow[atRow] = 0;
    }

    private void addCells(int atRow, int length) {
      cellsPerRow[atRow] += length;
    }

    private int numCells(int atRow) {
      return cellsPerRow[atRow];
    }

    private boolean isCleared(int atRow) {
      return cellsPerRow[atRow] == 0;
    }
  }
  private final Coordinator coordinator;
  private final CellBuffers cellBuffer;
  private final PartialParityBuffers partialBuffers;
  private final ErasureCodingPolicy ecPolicy;
  private final RawErasureEncoder encoder;
  private final List<StripedDataStreamer> streamers;
  private final List<StripedDataStreamer> pstreamers;
  private final DFSPacket[] currentPackets; // current Packet of each streamer
  private final DFSPacket[] parityPackets;

  // Size of each striping cell, must be a multiple of bytesPerChecksum.

  private int numDataBlocksInGroup;
  private final int numParityBlocksInGroup;
  private final int numDataBlocksInStripe;
  private final int numStreamers;

  private ExtendedBlock currentDataStripe;
  public static DatanodeInfoWithStorage datanodeToKill;
  public static DatanodeInfoWithStorage datanodeToKill2;
  private ExtendedBlock prevDataStripe4Append;

  // all redundancy groups ending in the current data stripe
  private final List<ExtendedBlockWithPosition> currentBlockGroupsInStripe;
  // current block group info used by streamers to know what to flush
  private ExtendedBlockWithPosition currentBlockGroup;
  // this is used to check which block group is ending
  private Set<Integer> blockBoundaries;

  private String[] favoredNodes;
  private final List<StripedDataStreamer> failedStreamers;
  private final Map<Integer, Integer> corruptBlockCountMap;
  private ExecutorService flushAllExecutor;
  private CompletionService<Void> flushAllExecutorCompletionService;
  private int dataStripeIndex;
  private long datanodeRestartTimeout;
  private final int numCellsInBlock;
  private int rowIndexInBlock;

  // latency metrics to collect
  private boolean updateEncodeEnabled = true;
  private final boolean metricsLoggingEnabled = false;
  private long totalInitTime;
  private long totalBlockAllocTime;
  private long totalWritingTime;
  private long totalParitySendingTime;
  private long totalEncodingTime;
  private long totalCombiningTime;
  private long totalClosingTime;
  private long totalWaitingTime;
  private long totalDataSendingTime;
  private long globalStartTime;

  // per stripe metrics
  private long stripeStartTime;
  private long timeSpentEncodingInStripe;
  private long timeSpentCombiningInStripe;
  private long timeSpentAllocatingInStripe;
  private long timeSpentFlushingParity;
  private long timeSpentInWriteChunk;
  private long timeSpentFlushingData;
  private long timeStartingStripe;

  /** Construct a new output stream for creating a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes)
    throws IOException {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes, false);
    globalStartTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating DFSStripedOutputStream for " + src);
    }

    ecPolicy = stat.getErasureCodingPolicy();
    cellSize = ecPolicy.getCellSize();

    numCellsInBlock = (int) (blockSize / cellSize);

    // TODO: remove after testing
//    numDataBlocksInGroup = 6;
//    numParityBlocksInGroup = 3;
//    numDataBlocksInStripe = 6;

    // TODO: emulating coupled arch
//    numDataBlocksInGroup = ecPolicy.getNumDataUnits();
//    numParityBlocksInGroup = ecPolicy.getNumParityUnits();
//    numDataBlocksInStripe = ecPolicy.getNumDataUnits();

    // this is the number of data blocks that consist a group
    numDataBlocksInGroup = ecPolicy.getNumDataUnits();
    // this is the number of parity blocks in a group
    numParityBlocksInGroup = ecPolicy.getNumParityUnits();
    // this is the number of data blocks that span a stripe
//    numDataBlocksInStripe = dfsClient.getConf().getDefaultReplication();
    numDataBlocksInStripe = ecPolicy.getStripeWidth() != 0 ? ecPolicy.getStripeWidth() : numDataBlocksInGroup;

    LOG.info("Configs: stripe size = {}, block size = {},  ecpolicy name = {}",
      numDataBlocksInStripe, blockSize, ecPolicy.getCodecName());

    if (metricsLoggingEnabled) {
      LOG.info("Group width = {}-{} | Stripe width = {}",
        numDataBlocksInGroup, numParityBlocksInGroup, numDataBlocksInStripe);
    }

    // this is the number of streamers to initialize
    // it's possible there are multiple groups in a stripe, and you need that many sets of parity streamers
    int maxNumberOfGroupsInStripe = (numDataBlocksInStripe + numDataBlocksInGroup - 1) / numDataBlocksInGroup;
    numStreamers = numDataBlocksInStripe + (maxNumberOfGroupsInStripe * numParityBlocksInGroup);

    this.favoredNodes = favoredNodes;
    failedStreamers = new ArrayList<>();
    corruptBlockCountMap = new LinkedHashMap<>();
    flushAllExecutor = Executors.newFixedThreadPool(numStreamers);
    flushAllExecutorCompletionService = new
      ExecutorCompletionService<>(flushAllExecutor);

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
      numDataBlocksInGroup, numParityBlocksInGroup);
    final long st = System.currentTimeMillis();
    encoder = CodecUtil.createRawEncoder(dfsClient.getConfiguration(),
      ecPolicy.getCodecName(), coderOptions);
    final long af = System.currentTimeMillis();
    LOG.debug("Time to init encoder: {}", (af-st));
    currentBlockGroupsInStripe = new ArrayList<>();
    coordinator = new Coordinator(numStreamers);
    cellBuffer = new CellBuffers(numParityBlocksInGroup);

    if (numDataBlocksInStripe == numDataBlocksInGroup || numDataBlocksInStripe % numDataBlocksInGroup == 0) {
      partialBuffers = null; // no need for partial buffers if not using
    } else {
      partialBuffers = new PartialParityBuffers();
    }

    streamers = new ArrayList<>(numDataBlocksInStripe);
    pstreamers = new ArrayList<>(maxNumberOfGroupsInStripe * numParityBlocksInGroup);
    for (short i = 0; i < numStreamers; i++) {
      StripedDataStreamer streamer = new StripedDataStreamer(stat,
              dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
              favoredNodes, i, coordinator, getAddBlockFlags());
      if (i < numDataBlocksInStripe) {
        streamers.add(streamer);
      } else {
        pstreamers.add(streamer);
      }
    }
    currentPackets = new DFSPacket[streamers.size()];
    parityPackets = new DFSPacket[pstreamers.size()];

    datanodeRestartTimeout = dfsClient.getConf().getDatanodeRestartTimeout();
    setCurrentStreamer(0);

    totalInitTime = System.currentTimeMillis() - globalStartTime;
    dfsClient.getNamenode().setStripeWidth(src, numDataBlocksInStripe);
  }

  /** Construct a new output stream for appending to a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src,
                         EnumSet<CreateFlag> flags, Progressable progress, LocatedBlock lastBlock,
                         HdfsFileStatus stat, DataChecksum checksum, String[] favoredNodes)
    throws IOException {
    this(dfsClient, src, stat, flags, progress, checksum, favoredNodes);
    initialFileSize = stat.getLen(); // length of file when opened
    prevDataStripe4Append = lastBlock != null ? lastBlock.getBlock() : null;
  }

  private boolean useDirectBuffer() {
    return encoder.preferDirectBuffer();
  }

  StripedDataStreamer getStripedDataStreamer(int i) {
    return streamers.get(i);
  }

  StripedDataStreamer getStripedParityStreamer(int i) {
    return pstreamers.get(i);
  }

  int getCurrentIndex() {
    return getCurrentStreamer().getIndex();
  }

  int getParityIndex() {
    return getCurrentParityStreamer().getIndex();
  }

  int getCurrentRow() {
    return rowIndexInBlock;
  }

  void setRowIndex(int index) {
    this.rowIndexInBlock = index;
  }

  private synchronized StripedDataStreamer getCurrentStreamer() {
    return (StripedDataStreamer) streamer;
  }

  private synchronized StripedDataStreamer setCurrentStreamer(int newIdx) {
    // backup currentPacket for current streamer
    if (streamer != null) {
      int oldIdx = streamers.indexOf(getCurrentStreamer());
      if (oldIdx >= 0) {
        currentPackets[oldIdx] = currentPacket;
      }
    }

    streamer = getStripedDataStreamer(newIdx);
    currentPacket = currentPackets[newIdx];
    adjustChunkBoundary(streamer);

    return getCurrentStreamer();
  }

  private StripedDataStreamer getCurrentParityStreamer() {
    return (StripedDataStreamer) pstreamer;
  }

  private StripedDataStreamer setParityStreamer(int newIdx) {
    // backup currentPacket for current streamer
    if (pstreamer != null) {
      int oldIdx = pstreamers.indexOf(getCurrentParityStreamer());
      if (oldIdx >= 0) {
        parityPackets[oldIdx] = parityPacket;
      }
    }

    pstreamer = getStripedParityStreamer(newIdx);
    parityPacket = parityPackets[newIdx];
    adjustChunkBoundary(pstreamer);

    return getCurrentParityStreamer();
  }

  /**
   * Encode the buffers, i.e. compute parities.
   *
   * @param buffers input data buffers -> output parity buffers
   */
  private static void encode(RawErasureEncoder encoder,
                             ByteBuffer[] buffers,
                             int stripeOffset,
                             int numData, int numParity) throws IOException {
    final long start = System.currentTimeMillis();
    final ByteBuffer[] dataBuffers = new ByteBuffer[numData];
    final ByteBuffer[] parityBuffers = new ByteBuffer[numParity];
    System.arraycopy(buffers, stripeOffset, dataBuffers, 0, numData);
    System.arraycopy(buffers, buffers.length - numParity, parityBuffers, 0, parityBuffers.length);
    final long encode = System.currentTimeMillis();

    encoder.encode(dataBuffers, parityBuffers);
    LOG.debug("ENCODE TIME = " + (System.currentTimeMillis() - encode) + " ms and time to setup = " + (encode - start));
  }

  /**
   * encodePartial takes the given parameters to protect data in
   * cellBuffer and writing newly encoded data to partialBuffer.
   * This allows for non-full groups to spit out their partially
   * computed parities at the end of a stripe, rather than
   * buffering the entire group in memory until the end.
   *
   * @param encoder just the encoder
   * @param cellBuffer the data cells to be protected
   * @param stripeOffset the starting position in the stripe to read from
   * @param groupOffset the relative position in the group to compute for
   * @param length the number of cells to copy
   * @param partialBuffer the output buffer for partial parities (could be cellBuffer or ppBuffer)
   * @param partialOffset the position within partialBuffer to use as the output
   * @param numData always numDataInGroup so the maximum width to encode
   * @param numParity always numParityInGroup
   */
  private static ByteBuffer[] encodePartial(
      RawErasureEncoder encoder, ByteBuffer[] cellBuffer,
      int stripeOffset, int groupOffset, int length,
      ByteBuffer[] partialBuffer, int partialOffset,
      int numData, int numParity,
      boolean useDirectBuffer, int cellSize) throws IOException {
    final long start = System.currentTimeMillis();
    final ByteBuffer[] inputBuffer = new ByteBuffer[numData];
    final ByteBuffer[] outputBuffer = new ByteBuffer[numParity];

    // pad inputBuffer with zeros
    for (int i = 0; i < numData; i++) {
      if (i < groupOffset || i >= groupOffset + length) {
        inputBuffer[i] = BUFFER_POOL.getBuffer(useDirectBuffer, cellSize);
      }
    }

    System.arraycopy(cellBuffer, stripeOffset, inputBuffer, groupOffset, length);
    System.arraycopy(partialBuffer, partialOffset, outputBuffer, 0, numParity);
    final long encode = System.currentTimeMillis();
    encoder.encode(inputBuffer, outputBuffer);
    LOG.info("Time to encode partial = " + (System.currentTimeMillis() - encode) + " ms and time to setup = " + (encode - start));
    return outputBuffer;
  }

  /**
   * Updates the parities by using singular vector updates to final parities
   * @param encoder just the encoder
   * @param cellBuffer array of source data used to encode parities
   * @param stripeOffset relative position in stripe (within cellBuffer) to start reading
   * @param groupOffset starting index relative to group position (0 when first calling partial update)
   * @param partialBuffer output buffer where partials will be updated to
   * @param partialOffset relative position in partialBuffer to start updating (non-0 for cellBuffer)
   * @param numData number of data units in group
   * @param numDataInStripe number of data units in stripe
   * @param numParity number of parity units
   * @throws IOException
   */
  private static void updatePartial(
      RawErasureEncoder encoder, ByteBuffer[] cellBuffer,
      int stripeOffset, int groupOffset,
      ByteBuffer[] partialBuffer, int partialOffset,
      int numDataInStripe, int numData, int numParity) throws IOException {
    final long start = System.currentTimeMillis();
    // either the full stripe or the remainder of the group
    final int length = Math.min(numData - groupOffset, numDataInStripe - stripeOffset);
    // array of cells
    final ByteBuffer[] inputBuffer = new ByteBuffer[length];
    final ByteBuffer[] outputBuffer = new ByteBuffer[numParity];

    System.arraycopy(cellBuffer, stripeOffset, inputBuffer, 0, length); // inputBuffer is no longer full group
    System.arraycopy(partialBuffer, partialOffset, outputBuffer, 0, numParity);
    final long update = System.currentTimeMillis();
    encoder.update(inputBuffer, outputBuffer, groupOffset, length);
    LOG.info("Time to update partial = " + (System.currentTimeMillis() - update) + " ms and time to setup = " + (update - start));
  }

  /**
   * Combines two partial parities together by XORing all bytes.
   * Does not use an efficient encoder implementation currently
   * but could be used in the future potentially.
   * The rightPartial will hold the newly computed results.
   *
   * @param leftPartial left-hand side
   * @param rightPartial right-hand side
   * @throws IOException
   */
  private static void combine(ByteBuffer[] leftPartial,
                              ByteBuffer[] rightPartial) {
    final long start = System.currentTimeMillis();
    for (int i = 0; i < rightPartial.length; i++) {
      for (int j = rightPartial[i].position(); j < rightPartial[i].capacity(); j+=1) {
        rightPartial[i].put(j, (byte) (leftPartial[i].get(j) ^ rightPartial[i].get(j)));
      }
    }
    LOG.debug("Time to combine partials = " + (System.currentTimeMillis() - start) + " ms.");
  }

  /**
   * check all the existing StripedDataStreamer and find newly failed streamers.
   * @return The newly failed streamers.
   * @throws IOException if less than {@link #numDataBlocksInStripe} streamers are still
   *                     healthy.
   */
  private Set<StripedDataStreamer> checkStreamers() throws IOException {
    Set<StripedDataStreamer> newFailed = new HashSet<>();
    for(StripedDataStreamer s : streamers) {
      if (!s.isHealthy() && !failedStreamers.contains(s)) {
        newFailed.add(s);
      }
    }
    for(StripedDataStreamer s : pstreamers) {
      if (!s.isHealthy() && !failedStreamers.contains(s)) {
        newFailed.add(s);
      }
    }

    final int failCount = failedStreamers.size() + newFailed.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkStreamers: " + streamers);
      LOG.debug("healthy streamer count=" + (numStreamers - failCount));
      LOG.debug("original failed streamers: " + failedStreamers);
      LOG.debug("newly failed streamers: " + newFailed);
    }
    if (failCount > (numStreamers - numDataBlocksInStripe)) {
      closeAllStreamers();
      throw new IOException("Failed: the number of failed blocks = "
        + failCount + " > the number of parity blocks = "
        + (numStreamers - numDataBlocksInStripe));
    }
    return newFailed;
  }

  private void closeAllStreamers() {
    // The write has failed, Close all the streamers.
    for (StripedDataStreamer streamer : streamers) {
      streamer.close(true);
    }
    for (StripedDataStreamer streamer : pstreamers) {
      streamer.close(true);
    }
  }

  private void handleCurrentStreamerFailure(String err, Exception e)
    throws IOException {
    currentPacket = null;
    handleStreamerFailure(err, e, getCurrentStreamer());
  }

  private void handleCurrentParityStreamerFailure(StripedDataStreamer streamer, String err, Exception e)
          throws IOException {
    parityPacket = null;
    handleParityStreamerFailure(err, e, streamer);
  }

  private void handleStreamerFailure(String err, Exception e,
                                     StripedDataStreamer streamer) throws IOException {
    LOG.warn("Failed: " + err + ", " + this, e);
    streamer.getErrorState().setInternalError();
    streamer.close(true);
    checkStreamers();
    currentPackets[streamer.getIndex()] = null;
  }

  private void handleParityStreamerFailure(String err, Exception e,
                                     StripedDataStreamer streamer) throws IOException {
    LOG.warn("Failed: " + err + ", " + this, e);
    streamer.getErrorState().setInternalError();
    streamer.close(true);
    checkStreamers();
    parityPackets[streamer.getIndex()] = null;
  }


  private void replaceFailedStreamers() {
    assert streamers.size() == numDataBlocksInStripe;
    final int currentIndex = getCurrentIndex();
    assert currentIndex == 0;
    for (short i = 0; i < numDataBlocksInStripe; i++) {
      final StripedDataStreamer oldStreamer = getStripedDataStreamer(i);
      if (!oldStreamer.isHealthy()) {
        LOG.info("replacing previously failed streamer " + oldStreamer);
        StripedDataStreamer streamer = new StripedDataStreamer(oldStreamer.stat,
          dfsClient, src, oldStreamer.progress,
          oldStreamer.checksum4WriteBlock, cachingStrategy, byteArrayManager,
          favoredNodes, i, coordinator, getAddBlockFlags());
        streamers.set(i, streamer);
        currentPackets[i] = null;
        if (i == currentIndex) {
          this.streamer = streamer;
          this.currentPacket = null;
        }
        streamer.start();
      }
    }
  }

  private void waitEndBlocks(StripedDataStreamer streamer, int i, ExtendedBlock block, int offset) throws IOException {
    while (streamer.isHealthy()) {
      final ExtendedBlock b = coordinator.endBlocks.takeWithTimeout(i);
      if (b != null) {
        StripedBlockUtil.checkBlocks(block, i - offset, b);
        return;
      }
    }
  }

  private DatanodeInfo[] getExcludedNodes() {
    List<DatanodeInfo> excluded = new ArrayList<>();
    for (StripedDataStreamer streamer : streamers) {
      for (DatanodeInfo e : streamer.getExcludedNodes()) {
        if (e != null) {
          excluded.add(e);
        }
      }
    }
    return excluded.toArray(new DatanodeInfo[excluded.size()]);
  }

  private ExtendedBlockWithPosition toGroupInfo(LocatedBlock lb, int index) {
    ExtendedBlockWithPosition info = new ExtendedBlockWithPosition(lb.getBlock());
    info.pos = index - 1;
    info.startPosition = info.pos * numParityBlocksInGroup;
    info.endPosition = info.startPosition + numParityBlocksInGroup;
    return info;
  }

  private void parseBlockGroupAndSetStreamers(LocatedBlock lb, int index, DatanodeInfo[] excludedNodes) {
    int dataBlkNum = 0;
    int parityBlkNum = 0;
    int offset = 0; // streamer offset since groups don't begin at 0
    boolean forData = index == 0;
    if (forData) {
      dataBlkNum = numDataBlocksInStripe;
    } else {
      parityBlkNum = numParityBlocksInGroup;
      offset = currentBlockGroupsInStripe.get(index - 1).startPosition;
    }
    final LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(
      (LocatedStripedBlock) lb, cellSize, dataBlkNum, parityBlkNum);
    for (int i = 0; i < blocks.length; i++) {
      StripedDataStreamer si = forData ? getStripedDataStreamer(i) : getStripedParityStreamer(offset + i);
      assert si.isHealthy();
      if (blocks[i] == null) {
        // allocBlock() should guarantee that all data blocks are successfully
        // allocated.
        assert i >= numDataBlocksInStripe;
        // Set exception and close streamer as there is no block locations
        // found for the parity block.
        LOG.warn("Cannot allocate parity block(index={}, policy={}). " +
            "Exclude nodes={}. There may not be enough datanodes or " +
            "racks. You can check if the cluster topology supports " +
            "the enabled erasure coding policies by running the command " +
            "'hdfs ec -verifyClusterSetup'.", i,  ecPolicy.getName(),
          excludedNodes);
        si.getLastException().set(
          new IOException("Failed to get parity block, index=" + i));
        si.getErrorState().setInternalError();
        si.close(true);
      } else {
        // each streamer gets its own part of the block
        coordinator.getFollowingBlocks().offer(forData ? i : (numDataBlocksInStripe + offset + i), blocks[i]);
      }
    }
  }

  private void allocateNewDataStripe() throws IOException {
    stripeStartTime = System.currentTimeMillis();

    // replace failed streamers
    failedStreamers.clear();
    DatanodeInfo[] excludedNodes = getExcludedNodes();
    LOG.debug("Excluding DataNodes when allocating new block: "
            + Arrays.asList(excludedNodes));
    replaceFailedStreamers();

    // Ask namenode for new blocks
    ExtendedBlock prevDataStripe = currentDataStripe;
    ExtendedBlock[] prevBlockGroups = null;
    if (prevDataStripe4Append != null) {
      prevDataStripe = prevDataStripe4Append;
      prevDataStripe4Append = null;
    }
    if (!currentBlockGroupsInStripe.isEmpty()) {
      prevBlockGroups = currentBlockGroupsInStripe.toArray(new ExtendedBlock[0]);
      favoredNodes = new String[numParityBlocksInGroup];
      for (int i = 0; i < numParityBlocksInGroup; i++) {
        favoredNodes[i] = getStripedParityStreamer(i).getNode().getXferAddr();
      }
    }
    // Determine new groups needed in stripe
    blockBoundaries = getGroupsInStripe();

    LOG.debug("Allocating new block group. The previous block group: "
            + prevDataStripe);
    final LocatedBlock[] lbs;
    try {
      lbs = addBlocks(excludedNodes, dfsClient, src, prevDataStripe,
              prevBlockGroups, null, fileId, favoredNodes,
              getAddBlockFlags(), 1, blockBoundaries.size());
    } catch (IOException ioe) {
      closeAllStreamers();
      throw ioe;
    }

    // should always be at least one data stripe
    assert lbs.length == blockBoundaries.size() + 1;

    // Wait for all previous blocks to be reported as ack'ed
    if (currentDataStripe != null) {
      for (int i = 0; i < numDataBlocksInStripe; i++) {
        // sync all the healthy streamers before writing to the new block
        waitEndBlocks(getStripedDataStreamer(i), i, currentDataStripe, 0);
      }
    }

    if (!currentBlockGroupsInStripe.isEmpty()) {
      for (int i = 0; i < currentBlockGroupsInStripe.size(); i++) {
        ExtendedBlockWithPosition blockGroup = currentBlockGroupsInStripe.get(i);
        for (int j = blockGroup.startPosition; j < blockGroup.endPosition; j++) {
          waitEndBlocks(getStripedParityStreamer(j), numDataBlocksInStripe + j,
                  blockGroup, numDataBlocksInStripe + blockGroup.startPosition);
        }
      }
      currentBlockGroupsInStripe.clear();
    }

    dataStripeIndex++;
    for (int i = 0; i < lbs.length; i++) {
      if (i == 0) {
        // first unit is always the data stripe
        currentDataStripe = lbs[i].getBlock();
        // use this for killing the first datanode
        datanodeToKill = datanodeToKill != null ? datanodeToKill : lbs[i].getLocations()[0];
      } else {
        // the remainder are blocks which are all groups ending in the stripe
        currentBlockGroupsInStripe.add(toGroupInfo(lbs[i], i));
        datanodeToKill2 = lbs[i].getLocations()[0];
      }
      parseBlockGroupAndSetStreamers(lbs[i], i, excludedNodes);
    }
    currentBlockGroup = null;

    timeSpentEncodingInStripe = 0;
    timeSpentCombiningInStripe = 0;
    timeSpentFlushingParity = 0;
    timeSpentFlushingData = 0;
    timeSpentInWriteChunk = 0;
    timeSpentAllocatingInStripe = System.currentTimeMillis() - stripeStartTime;
  }

  private boolean shouldEndDataStripe() {
    return currentDataStripe != null &&
      currentDataStripe.getNumBytes() == blockSize * numDataBlocksInStripe;
  }

  private boolean shouldEndBlockGroup() {
    return rowIndexInBlock == numCellsInBlock - 1;
  }

  private Set<Integer> getGroupsInStripe() {
    Set<Integer> boundaries = new HashSet<>();
    int lowerBound = dataStripeIndex * numDataBlocksInStripe;
    int upperBound = lowerBound + numDataBlocksInStripe;
    int boundary = (lowerBound / numDataBlocksInGroup) * numDataBlocksInGroup; // starting position
    int pos;
    while (boundary <= upperBound) {
      if (boundary > lowerBound) {
        pos = boundary % numDataBlocksInStripe;
        if (pos == 0) {
          boundaries.add(numDataBlocksInStripe);
        } else {
          boundaries.add(pos);
        }
      }
      boundary += numDataBlocksInGroup;
    }
    return boundaries;
  }

  private void setCurrentBlockGroup() {
    if (currentBlockGroup == null) {
      currentBlockGroup = currentBlockGroupsInStripe.get(0);
    } else {
      // loop back around to the beginning
      int nextPos = currentBlockGroup.pos + 1;
      if (nextPos < currentBlockGroupsInStripe.size()) {
        currentBlockGroup = currentBlockGroupsInStripe.get(nextPos);
      } else {
        currentBlockGroup = currentBlockGroupsInStripe.get(0);
      }
    }
  }

  private void collectStripeLatencyMetrics() {
    final long totalTimeSpentOnStripe = System.currentTimeMillis() - stripeStartTime;
    timeSpentFlushingData = timeSpentFlushingData / (1000 * 1000);
    timeSpentInWriteChunk = timeSpentInWriteChunk / (1000 * 1000);

    totalBlockAllocTime += timeSpentAllocatingInStripe;
    totalWritingTime += timeSpentInWriteChunk;
    totalDataSendingTime += timeSpentFlushingData;
    totalParitySendingTime += timeSpentFlushingParity;
    totalEncodingTime += timeSpentEncodingInStripe;
    totalCombiningTime += timeSpentCombiningInStripe;
    totalWaitingTime += totalTimeSpentOnStripe - timeSpentInWriteChunk; //  this is the time not in writeChunk

    LOG.info("Time to allocate at stripe " + dataStripeIndex + " = " + timeSpentAllocatingInStripe + " ms.");
    LOG.info("Time to write chunks at stripe " + dataStripeIndex + " = " + timeSpentInWriteChunk + " ms.");
    LOG.info("Time to send data at stripe " + dataStripeIndex + " = " + timeSpentFlushingData + " ms.");
    LOG.info("Time to send parity at stripe " + dataStripeIndex + " = " + timeSpentFlushingParity + " ms.");
    LOG.info("Time to encode at stripe " + dataStripeIndex + " = " + timeSpentEncodingInStripe + " ms.");
    LOG.info("Time to combine at stripe " + dataStripeIndex + " = " + timeSpentCombiningInStripe + " ms.");
    LOG.info("Time to wait at stripe " + dataStripeIndex + " = " + (totalTimeSpentOnStripe - timeSpentInWriteChunk) + " ms.");
    LOG.info("Time to time for stripe " + dataStripeIndex + " = " + totalTimeSpentOnStripe + " ms.");
  }

  private void collectTotalLatencyMetrics() {
    LOG.info("Total time to initialize = " + totalInitTime + " ms.");
    LOG.info("Total time to allocate = " + totalBlockAllocTime + " ms.");
    LOG.info("Total time to write chunks = " + totalWritingTime + " ms.");
    LOG.info("Total time to wait = " + totalWaitingTime + " ms.");
    LOG.info("Total time to send data = " + totalDataSendingTime + " ms.");
    LOG.info("Total time to send parity = " + totalParitySendingTime + " ms.");
    LOG.info("Total time to encode = " + totalEncodingTime + " ms.");
    LOG.info("Total time to combine = " + totalCombiningTime + " ms.");
    LOG.info("Total time to close = " + totalClosingTime + " ms.");
    LOG.info("Total time end-to-end = " + (System.currentTimeMillis() - globalStartTime) + " ms.");
    LOG.info("CSV friendly: {}, {}, {} = {},{},{},{},{},{},{},{},{}",
      numDataBlocksInStripe, numDataBlocksInGroup, numParityBlocksInGroup,
      totalInitTime, totalBlockAllocTime, totalWritingTime,
      totalWaitingTime, totalDataSendingTime, totalParitySendingTime,
      totalEncodingTime, totalCombiningTime, totalClosingTime);
  }

  @Override
  protected synchronized void writeChunk(byte[] bytes, int offset, int len,
                                         byte[] checksum, int ckoff, int cklen) throws IOException {
    final long startTime = System.nanoTime();
    final int stripeIndex = getCurrentIndex();
    final int stripeRow = getCurrentRow(); // e.g., with 4MB blocks and 1MB cells, stripeRow=[0,3]
    final CellBuffers cellBuffers = cellBuffer;  // get current cellBuffer
    // can't add to if still being used for parity gen, then need to block until ready
    final int pos = cellBuffers.addTo(stripeIndex, bytes, offset, len);
    final boolean cellFull = pos == cellSize;
    boolean collect = false;

    if (currentDataStripe == null || shouldEndDataStripe()) {
      // the incoming data should belong to a new block. Allocate a new block.
      allocateNewDataStripe();
      timeStartingStripe = System.currentTimeMillis();
    }

    currentDataStripe.setNumBytes(currentDataStripe.getNumBytes() + len);
    // note: the current streamer can be refreshed after allocating a new block
    final StripedDataStreamer current = getCurrentStreamer();
    if (current.isHealthy()) {
      try {
        final long chunkTime = System.nanoTime();
        super.writeChunk(bytes, offset, len, checksum, ckoff, cklen);
        timeSpentFlushingData += System.nanoTime() - chunkTime;
      } catch(Exception e) {
        handleCurrentStreamerFailure("offset=" + offset + ", length=" + len, e);
      }
    }

    // Two extra steps are needed when a striping cell is full:
    // 1. Forward the current index pointer
    // 2. Generate parity packets if a full stripe of data cells are present
    if (cellFull) {
      int nextStripePos = stripeIndex + 1;
      int nextStripeRow = stripeRow;

      //At a block group boundary, means a full group of cells is ready for a parity
      boolean atGroupBoundary = blockBoundaries.contains(nextStripePos);
      if (atGroupBoundary) {
        //When all data cells in a grouplet are ready, we need to encode
        //them and generate completed parity cells using the partial parities
        //and previous data cells in the current stripe, if any.
        setCurrentBlockGroup();
        boolean endBlockGroup = shouldEndBlockGroup();
        writeCompleteParities(cellBuffers, stripeRow, stripeIndex, endBlockGroup, currentBlockGroup);
      }

      //At a stripe boundary, means we might have to generate partial parities if not at group boundary
      boolean atStripeBoundary = nextStripePos == numDataBlocksInStripe;
      if (atStripeBoundary) {
        //When all data cells in a stripe are ready, we need to encode
        //them and generate partial parity cells. These cells will be
        //converted to packets and put to their DataStreamer's queue.
        nextStripePos = 0;
        if (!atGroupBoundary) {
          computePartialParities(cellBuffers, stripeRow);
        }

        // if this is the end of the data stripe, end each internal block
        if (shouldEndDataStripe()) {
          flushDataInternals();
          checkStreamerFailures(false);
          final long flushTime = System.nanoTime();
          for (int i = 0; i < numDataBlocksInStripe; i++) {
            final StripedDataStreamer s = setCurrentStreamer(i);
            if (s.isHealthy()) {
              try {
                endBlock();
              } catch (IOException ignored) {}
            }
          }
          timeSpentFlushingData += (System.nanoTime() - flushTime);
          collect = true;
          nextStripeRow = 0; // reset back to the first row in the new stripe
        } else {
          // check failure state for all the streamers. Bump GS if necessary
          checkStreamerFailures(true);
          nextStripeRow += 1;
        }
        timeStartingStripe = System.currentTimeMillis();
      }

      setCurrentStreamer(nextStripePos);
      setRowIndex(nextStripeRow);
    }
    timeSpentInWriteChunk += (System.nanoTime() - startTime);
    if (collect && metricsLoggingEnabled) {
      collectStripeLatencyMetrics();
    }
  }

  private void writeCompleteParities(CellBuffers cellBuffers, int stripeRow,
                                     int stripeIndex, boolean completeBlockGroup,
                                     ExtendedBlockWithPosition currentBlockGroup) {
    try {
      cellBuffers.flipDataBuffers();
      computeParities(cellBuffers, stripeRow, stripeIndex, currentBlockGroup);

      //Complete end to block group, close out all the parity blocks
      if (completeBlockGroup) {
        flushParityInternals();
        checkStreamerFailures(false);
        final long flushTime = System.currentTimeMillis();
        for (int i = currentBlockGroup.startPosition; i < currentBlockGroup.endPosition; i++) {
          final StripedDataStreamer s = setParityStreamer(i);
          if (s.isHealthy()) {
            try {
              endParityBlock();
            } catch (IOException ignored) {}
          }
        }
        timeSpentFlushingParity += (System.currentTimeMillis() - flushTime);
      } else {
        checkStreamerFailures(true);
      }
    } catch (IOException ioe) {
      LOG.error("error while writing parities", ioe);
    }
  }

  @Override
  synchronized void enqueueCurrentPacketFull() throws IOException {
    LOG.debug("enqueue full {}, src={}, bytesCurBlock={}, blockSize={},"
        + " appendChunk={}, {}", currentPacket, src, getStreamer()
        .getBytesCurBlock(), blockSize, getStreamer().getAppendChunk(),
      getStreamer());
    enqueueCurrentPacket();
    adjustChunkBoundary(getStreamer());
    // no need to end block here
  }

  /**
   * @return whether the data streamer with the given index is streaming data.
   * Note the streamer may not be in STREAMING stage if the block length is less
   * than a stripe.
   */
  private boolean isStreamerWriting(int streamerIndex) {
    final long length = currentDataStripe == null ?
      0 : currentDataStripe.getNumBytes();
    if (length == 0) {
      return false;
    }
    if (streamerIndex >= numDataBlocksInStripe) {
      return true;
    }
    final int numCells = (int) ((length - 1) / cellSize + 1);
    return streamerIndex < numCells;
  }

  private Set<StripedDataStreamer> markExternalErrorOnStreamers() {
    Set<StripedDataStreamer> healthySet = new HashSet<>();
    for (int i = 0; i < numStreamers; i++) {
      final StripedDataStreamer streamer = i < numDataBlocksInStripe
              ? getStripedDataStreamer(i)
              : getStripedParityStreamer(i - numDataBlocksInStripe);
      if (streamer.isHealthy() && isStreamerWriting(i)) {
        Preconditions.checkState(
          streamer.getStage() == BlockConstructionStage.DATA_STREAMING,
          "streamer: " + streamer);
        streamer.setExternalError();
        healthySet.add(streamer);
      } else if (!streamer.streamerClosed()
        && streamer.getErrorState().hasDatanodeError()
        && streamer.getErrorState().doWaitForRestart()) {
        healthySet.add(streamer);
        failedStreamers.remove(streamer);
      }
    }
    return healthySet;
  }

  /**
   * Check and handle data streamer failures. This is called only when we have
   * written a full stripe (i.e., enqueue all packets for a full stripe), or
   * when we're closing the outputstream.
   */
  private void checkStreamerFailures(boolean isNeedFlushAllPackets)
    throws IOException {
    Set<StripedDataStreamer> newFailed = checkStreamers();
    if (newFailed.size() == 0) {
      return;
    }

    if (isNeedFlushAllPackets) {
      // for healthy streamers, wait till all of them have fetched the new block
      // and flushed out all the enqueued packets.
      flushDataInternals();

      // recheck failed streamers again after the flush
      newFailed = checkStreamers();
    }
    while (newFailed.size() > 0) {
      failedStreamers.addAll(newFailed);
      coordinator.clearFailureStates();
      corruptBlockCountMap.put(dataStripeIndex, failedStreamers.size());

      // mark all the healthy streamers as external error
      Set<StripedDataStreamer> healthySet = markExternalErrorOnStreamers();

      // we have newly failed streamers, update block for pipeline
      final ExtendedBlock newBG = updateBlockForPipeline(healthySet);

      // wait till all the healthy streamers to
      // 1) get the updated block info
      // 2) create new block outputstream
      newFailed = waitCreatingStreamers(healthySet);
      if (newFailed.size() + failedStreamers.size() >
        numStreamers - numDataBlocksInStripe) {
        // The write has failed, Close all the streamers.
        closeAllStreamers();
        throw new IOException(
          "Data streamers failed while creating new block streams: "
            + newFailed + ". There are not enough healthy streamers.");
      }
      for (StripedDataStreamer failedStreamer : newFailed) {
        assert !failedStreamer.isHealthy();
      }

      // TODO we can also succeed if all the failed streamers have not taken
      // the updated block
      if (newFailed.size() == 0) {
        // reset external error state of all the streamers
        for (StripedDataStreamer streamer : healthySet) {
          assert streamer.isHealthy();
          streamer.getErrorState().reset();
        }
        updatePipeline(newBG);
      }
      for (int i = 0; i < numStreamers; i++) {
        coordinator.offerStreamerUpdateResult(i, newFailed.size() == 0);
      }
      //wait for get notify to failed stream
      if (newFailed.size() != 0) {
        try {
          Thread.sleep(datanodeRestartTimeout);
        } catch (InterruptedException e) {
          // Do nothing
        }
      }
    }
  }

  /**
   * Check if the streamers were successfully updated, adding failed streamers
   * in the <i>failed</i> return parameter.
   * @param failed Return parameter containing failed streamers from
   *               <i>streamers</i>.
   * @param streamers Set of streamers that are being updated
   * @return total number of successful updates and failures
   */
  private int checkStreamerUpdates(Set<StripedDataStreamer> failed,
                                   Set<StripedDataStreamer> streamers) {
    for (StripedDataStreamer streamer : streamers) {
      if (!coordinator.updateStreamerMap.containsKey(streamer)) {
        if (!streamer.isHealthy() &&
          coordinator.getNewBlocks().peek(streamer.getIndex()) != null) {
          // this streamer had internal error before getting updated block
          failed.add(streamer);
        }
      }
    }
    return coordinator.updateStreamerMap.size() + failed.size();
  }

  /**
   * Waits for streamers to be created.
   *
   * @param healthyStreamers Set of healthy streamers
   * @return Set of streamers that failed.
   *
   * @throws IOException
   */
  private Set<StripedDataStreamer> waitCreatingStreamers(
    Set<StripedDataStreamer> healthyStreamers) throws IOException {
    Set<StripedDataStreamer> failed = new HashSet<>();
    final int expectedNum = healthyStreamers.size();
    final long socketTimeout = dfsClient.getConf().getSocketTimeout();
    // the total wait time should be less than the socket timeout, otherwise
    // a slow streamer may cause other streamers to timeout. here we wait for
    // half of the socket timeout
    long remaingTime = socketTimeout > 0 ? socketTimeout/2 : Long.MAX_VALUE;
    final long waitInterval = 1000;
    synchronized (coordinator) {
      while (checkStreamerUpdates(failed, healthyStreamers) < expectedNum
        && remaingTime > 0) {
        try {
          long start = Time.monotonicNow();
          coordinator.wait(waitInterval);
          remaingTime -= Time.monotonicNow() - start;
        } catch (InterruptedException e) {
          throw DFSUtilClient.toInterruptedIOException("Interrupted when waiting" +
            " for results of updating striped streamers", e);
        }
      }
    }
    synchronized (coordinator) {
      for (StripedDataStreamer streamer : healthyStreamers) {
        if (!coordinator.updateStreamerMap.containsKey(streamer)) {
          // close the streamer if it is too slow to create new connection
          LOG.info("close the slow stream " + streamer);
          streamer.setStreamerAsClosed();
          failed.add(streamer);
        }
      }
    }
    for (Map.Entry<DataStreamer, Boolean> entry :
      coordinator.updateStreamerMap.entrySet()) {
      if (!entry.getValue()) {
        failed.add((StripedDataStreamer) entry.getKey());
      }
    }
    for (StripedDataStreamer failedStreamer : failed) {
      healthyStreamers.remove(failedStreamer);
    }
    return failed;
  }

  /**
   * Call {@link ClientProtocol#updateBlockForPipeline} and assign updated block
   * to healthy streamers.
   * @param healthyStreamers The healthy data streamers. These streamers join
   *                         the failure handling.
   */
  private ExtendedBlock updateBlockForPipeline(
    Set<StripedDataStreamer> healthyStreamers) throws IOException {
    // TODO: think this through
    final LocatedBlock updated = dfsClient.namenode.updateBlockForPipeline(
      currentDataStripe, dfsClient.clientName);
    final long newGS = updated.getBlock().getGenerationStamp();
    ExtendedBlock newBlock = new ExtendedBlock(currentDataStripe);
    newBlock.setGenerationStamp(newGS);
    final LocatedBlock[] updatedBlks = StripedBlockUtil.parseStripedBlockGroup(
      (LocatedStripedBlock) updated, cellSize, numDataBlocksInStripe, 0);

    for (int i = 0; i < numDataBlocksInStripe; i++) {
      StripedDataStreamer si = getStripedDataStreamer(i);
      if (healthyStreamers.contains(si)) {
        final LocatedBlock lb = new LocatedBlock(new ExtendedBlock(newBlock),
          null, null, null, -1, updated.isCorrupt(), null);
        lb.setBlockToken(updatedBlks[i].getBlockToken());
        coordinator.getNewBlocks().offer(i, lb);
      }
    }
    return newBlock;
  }

  private void updatePipeline(ExtendedBlock newBG) throws IOException {
    final DatanodeInfo[] newNodes = new DatanodeInfo[numDataBlocksInStripe];
    final String[] newStorageIDs = new String[numDataBlocksInStripe];
    for (int i = 0; i < numDataBlocksInStripe; i++) {
      final StripedDataStreamer streamer = getStripedDataStreamer(i);
      final DatanodeInfo[] nodes = streamer.getNodes();
      final String[] storageIDs = streamer.getStorageIDs();
      if (streamer.isHealthy() && nodes != null && storageIDs != null) {
        newNodes[i] = nodes[0];
        newStorageIDs[i] = storageIDs[0];
      } else {
        newNodes[i] = new DatanodeInfoBuilder()
          .setNodeID(DatanodeID.EMPTY_DATANODE_ID).build();
        newStorageIDs[i] = "";
      }
    }

    // Update the NameNode with the acked length of the block group
    // Save and restore the unacked length
    final long sentBytes = currentDataStripe.getNumBytes();
    final long ackedBytes = getAckedLength();
    Preconditions.checkState(ackedBytes <= sentBytes,
      "Acked:" + ackedBytes + ", Sent:" + sentBytes);
    currentDataStripe.setNumBytes(ackedBytes);
    newBG.setNumBytes(ackedBytes);
    dfsClient.namenode.updatePipeline(dfsClient.clientName, currentDataStripe,
      newBG, newNodes, newStorageIDs);
    currentDataStripe = newBG;
    currentDataStripe.setNumBytes(sentBytes);
  }

  /**
   * Return the length of each block in the block group.
   * Unhealthy blocks have a length of -1.
   *
   * @return List of block lengths.
   */
  private List<Long> getBlockLengths() {
    List<Long> blockLengths = new ArrayList<>(numDataBlocksInStripe);
    for (int i = 0; i < numDataBlocksInStripe; i++) {
      final StripedDataStreamer streamer = getStripedDataStreamer(i);
      long numBytes = -1;
      if (streamer.isHealthy()) {
        if (streamer.getBlock() != null) {
          numBytes = streamer.getBlock().getNumBytes();
        }
      }
      blockLengths.add(numBytes);
    }
    return blockLengths;
  }

  /**
   * Get the length of acked bytes in the block group.
   *
   * <p>
   *   A full stripe is acked when at least numDataBlocks streamers have
   *   the corresponding cells of the stripe, and all previous full stripes are
   *   also acked. This enforces the constraint that there is at most one
   *   partial stripe.
   * </p>
   * <p>
   *   Partial stripes write all parity cells. Empty data cells are not written.
   *   Parity cells are the length of the longest data cell(s). For example,
   *   with RS(3,2), if we have data cells with lengths [1MB, 64KB, 0], the
   *   parity blocks will be length [1MB, 1MB].
   * </p>
   * <p>
   *   To be considered acked, a partial stripe needs at least numDataBlocks
   *   empty or written cells.
   * </p>
   * <p>
   *   Currently, partial stripes can only happen when closing the file at a
   *   non-stripe boundary, but this could also happen during (currently
   *   unimplemented) hflush/hsync support.
   * </p>
   */
  private long getAckedLength() {
    // Determine the number of full stripes that are sufficiently durable
    final long sentBytes = currentDataStripe.getNumBytes();
    final long numFullStripes = sentBytes / numDataBlocksInStripe / cellSize;
    final long fullStripeLength = numFullStripes * numDataBlocksInStripe * cellSize;
    assert fullStripeLength <= sentBytes : "Full stripe length can't be " +
      "greater than the block group length";

    long ackedLength = 0;

    // Determine the length contained by at least `numDataBlocks` blocks.
    // Since it's sorted, all the blocks after `offset` are at least as long,
    // and there are at least `numDataBlocks` at or after `offset`.
    List<Long> blockLengths = Collections.unmodifiableList(getBlockLengths());
    List<Long> sortedBlockLengths = new ArrayList<>(blockLengths);
    Collections.sort(sortedBlockLengths);
    if (numFullStripes > 0) {
      final int offset = sortedBlockLengths.size() - numDataBlocksInStripe;
      ackedLength = sortedBlockLengths.get(offset) * numDataBlocksInStripe;
    }

    // If the acked length is less than the expected full stripe length, then
    // we're missing a full stripe. Return the acked length.
    if (ackedLength < fullStripeLength) {
      return ackedLength;
    }
    // If the expected length is exactly a stripe boundary, then we're also done
    if (ackedLength == sentBytes) {
      return ackedLength;
    }

    /*
    Otherwise, we're potentially dealing with a partial stripe.
    The partial stripe is laid out as follows:

      0 or more full data cells, `cellSize` in length.
      0 or 1 partial data cells.
      0 or more empty data cells.
      `numParityBlocks` parity cells, the length of the longest data cell.

    If the partial stripe is sufficiently acked, we'll update the ackedLength.
    */

    // How many full and empty data cells do we expect?
    final int numFullDataCells = (int)
      ((sentBytes - fullStripeLength) / cellSize);
    final int partialLength = (int) (sentBytes - fullStripeLength) % cellSize;
    final int numPartialDataCells = partialLength == 0 ? 0 : 1;
    final int numEmptyDataCells = numDataBlocksInStripe - numFullDataCells -
      numPartialDataCells;
    // Calculate the expected length of the parity blocks.
    final int parityLength = numFullDataCells > 0 ? cellSize : partialLength;

    final long fullStripeBlockOffset = fullStripeLength / numDataBlocksInStripe;

    // Iterate through each type of streamers, checking the expected length.
    long[] expectedBlockLengths = new long[numStreamers];
    int idx = 0;
    // Full cells
    for (; idx < numFullDataCells; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + cellSize;
    }
    // Partial cell
    for (; idx < numFullDataCells + numPartialDataCells; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + partialLength;
    }
    // Empty cells
    for (; idx < numFullDataCells + numPartialDataCells + numEmptyDataCells;
         idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset;
    }
    // Parity cells
    for (; idx < numStreamers; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + parityLength;
    }

    // Check expected lengths against actual streamer lengths.
    // Update if we have sufficient durability.
    int numBlocksWithCorrectLength = 0;
    for (int i = 0; i < numStreamers; i++) {
      if (blockLengths.get(i) == expectedBlockLengths[i]) {
        numBlocksWithCorrectLength++;
      }
    }
    if (numBlocksWithCorrectLength >= numDataBlocksInStripe) {
      ackedLength = sentBytes;
    }

    return ackedLength;
  }

  private int stripeDataSize() {
    return numDataBlocksInStripe * cellSize;
  }

  @Override
  public boolean hasCapability(String capability) {
    // StreamCapabilities like hsync / hflush are not supported yet.
    return false;
  }

  @Override
  public void hflush() {
    // not supported yet
    LOG.debug("DFSStripedOutputStream does not support hflush. "
      + "Caller should check StreamCapabilities before calling.");
  }

  @Override
  public void hsync() {
    // not supported yet
    LOG.debug("DFSStripedOutputStream does not support hsync. "
      + "Caller should check StreamCapabilities before calling.");
  }

  @Override
  public void hsync(EnumSet<SyncFlag> syncFlags) {
    // not supported yet
    LOG.debug("DFSStripedOutputStream does not support hsync {}. "
      + "Caller should check StreamCapabilities before calling.", syncFlags);
  }

  @Override
  protected synchronized void start() {
    for (StripedDataStreamer streamer : streamers) {
      streamer.start();
    }
    for (StripedDataStreamer pstreamer : pstreamers) {
      pstreamer.start();
    }
  }

  @Override
  void abort() throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    synchronized (this) {
      if (isClosed()) {
        return;
      }
      exceptionLastSeen.set(new IOException("Lease timeout of "
        + (dfsClient.getConf().getHdfsTimeout() / 1000)
        + " seconds expired."));

      try {
        closeThreads(true);
      } catch (IOException e) {
        b.add(e);
      }
    }

    dfsClient.endFileLease(fileId);
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  @Override
  boolean isClosed() {
    if (closed) {
      return true;
    }
    for(StripedDataStreamer s : streamers) {
      if (!s.streamerClosed()) {
        return false;
      }
    }
    for(StripedDataStreamer s : pstreamers) {
      if (!s.streamerClosed()) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void closeThreads(boolean force) throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    try {
      for (StripedDataStreamer streamer : streamers) {
        try {
          streamer.close(force);
          streamer.join();
          streamer.closeSocket();
        } catch (Exception e) {
          try {
            handleStreamerFailure("force=" + force, e, streamer);
          } catch (IOException ioe) {
            b.add(ioe);
          }
        } finally {
          streamer.setSocketToNull();
        }
      }
      for (StripedDataStreamer streamer : pstreamers) {
        try {
          streamer.close(force);
          streamer.join();
          streamer.closeSocket();
        } catch (Exception e) {
          try {
            handleStreamerFailure("force=" + force, e, streamer);
          } catch (IOException ioe) {
            b.add(ioe);
          }
        } finally {
          streamer.setSocketToNull();
        }
      }
    } finally {
      setClosed();
    }
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  private boolean generateParityCellsForLastStripe() {
    // there are a few cases here:
    // it's possible there's something in the partial buffer
    // which means we may need to allocate a new striped block group

    // TODO: can't do the same math here, need to calculate based on group, not stripe
    final long currentBlockGroupBytes = currentDataStripe == null ?
      0 : currentDataStripe.getNumBytes();
    final long lastStripeSize = currentBlockGroupBytes % stripeDataSize();
    if (lastStripeSize == 0) {
      return false;
    }

    // for the last group, you might be truncated
    // numDataBlocksInGroup = (int) Math.min(numDataBlocksInStripe, (lastStripeSize - 1) / this.cellSize + 1);

    final long parityCellSize = lastStripeSize < cellSize?
      lastStripeSize : cellSize;
    final ByteBuffer[] buffers = cellBuffer.getBuffers();

    for (int i = 0; i < numDataBlocksInStripe + numParityBlocksInGroup; i++) {
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      final int position = buffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller" +
        " than parity block, then its last cell should be small than last" +
        " parity cell";

//      if (position == 0) {
//        Arrays.fill(buffers[i].array(), (byte)0);
//        buffers[i].position((int) parityCellSize);
//      }
      for (int j = 0; j < parityCellSize - position; j++) {
        buffers[i].put((byte) 0);
      }
      buffers[i].flip();
    }
    return true;
  }

  /**
   * Computes the initial set of partial parities for a group.
   */
  void computePartialParities(CellBuffers cellBuffers, int atRow) {
    cellBuffers.flipDataBuffers(); // cell Buffer may have been flipped already from ending a block group.

    final long timeStart = System.currentTimeMillis();
    int stripeOffset = cellBuffers.getFirstSetIndex();
    int groupOffset = 0;
    int length = numDataBlocksInStripe - stripeOffset;

    // encode the data cells and write into partialBuffers output
    try {
      if (updateEncodeEnabled) {
        updatePartial(encoder, cellBuffers.getBuffers(),
          stripeOffset, 0,
          partialBuffers.getCellBuffers(atRow), 0,
          numDataBlocksInStripe, numDataBlocksInGroup, numParityBlocksInGroup);
      } else {
        encodePartial(encoder, cellBuffers.getBuffers(),
          stripeOffset, groupOffset, length,
          partialBuffers.getCellBuffers(atRow), 0,
          numDataBlocksInGroup, numParityBlocksInGroup,
          useDirectBuffer(), cellSize);
      }
      // hold in memory, track the number of cells it currently protects
      partialBuffers.addCells(atRow, length);
    } catch (IOException ioe) {
      LOG.error("Issue while encoding partial", ioe);
    }

    // clear cell buffers used to compute partials
    cellBuffers.clear();
    this.timeSpentEncodingInStripe += System.currentTimeMillis() - timeStart;
  }

  void computeParities(CellBuffers cellBuffers, int atRow, int atCol,
                       ExtendedBlockWithPosition currentBlockGroup) throws IOException {
    final ByteBuffer[] buffers = cellBuffers.getBuffers();

    // assume state is correctly set for the current block group
    int start = currentBlockGroup.startPosition;
    int end = currentBlockGroup.endPosition;
    // add a row of parity cells to block group size
    currentBlockGroup.setNumBytes(currentBlockGroup.getNumBytes() + (long) (end - start) * cellSize);

    // Skips encoding and writing parity cells if there
    // are no healthy parity data streamers for that group
    if (!checkAnyParityStreamerIsHealthy(start, end)) {
      return;
    }

    final long encodeTime = System.currentTimeMillis();
    if (partialBuffers == null || partialBuffers.isCleared(atRow)) {
      // no partials, just use what's in cell buffer
      encode(encoder, buffers, cellBuffers.getFirstSetIndex(),
        numDataBlocksInGroup, numParityBlocksInGroup);
      this.timeSpentEncodingInStripe += System.currentTimeMillis() - encodeTime;

      final long startTime = System.currentTimeMillis();
      for (int i = 0; i < numParityBlocksInGroup; i++) {
        writeParity(start + i,
          buffers[numDataBlocksInStripe + i],
          cellBuffers.getChecksumArray(numDataBlocksInStripe + i));
      }
      this.timeSpentFlushingParity += (System.currentTimeMillis() - startTime);
    } else {
      // partials, use combination of cell buffer and partial parities;
      if (updateEncodeEnabled) {
        // encode the data cells and write into cellBuffers output
        ByteBuffer[] parityBuffers = partialBuffers.getCellBuffers(atRow);
        updatePartial(encoder, cellBuffers.getBuffers(),
          0, partialBuffers.numCells(atRow),
          parityBuffers, 0,
          numDataBlocksInStripe, numDataBlocksInGroup, numParityBlocksInGroup);
        // result is in partial parity buffer
        this.timeSpentEncodingInStripe += System.currentTimeMillis() - encodeTime;

        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < numParityBlocksInGroup; i++) {
          writeParity(start + i,
            parityBuffers[i], // results are directly in partial buffers
            cellBuffers.getChecksumArray(numDataBlocksInStripe + i));
        }
        partialBuffers.clearCells(atRow); // result is in cellBuffer
        this.timeSpentFlushingParity += (System.currentTimeMillis() - startTime);
      } else {
        ByteBuffer[] b = encodePartial(encoder, cellBuffers.getBuffers(),
          0, partialBuffers.numCells(atRow), atCol,
          cellBuffers.getBuffers(), numDataBlocksInStripe,
          numDataBlocksInGroup, numParityBlocksInGroup,
          useDirectBuffer(), cellSize);
        final long combineTime = System.currentTimeMillis();
        combine(partialBuffers.getCellBuffers(atRow), b);
        this.timeSpentEncodingInStripe += combineTime - encodeTime;
        this.timeSpentCombiningInStripe += System.currentTimeMillis() - combineTime;

        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < numParityBlocksInGroup; i++) {
          writeParity(start + i,
            buffers[numDataBlocksInStripe + i],
            cellBuffers.getChecksumArray(numDataBlocksInStripe + i));
        }
        partialBuffers.clearCells(atRow); // result is in cellBuffer
        this.timeSpentFlushingParity += (System.currentTimeMillis() - startTime);
      }
    }

    cellBuffers.clear();
  }

  private boolean checkAnyParityStreamerIsHealthy(int start, int end) {
    for (int i = start; i < end; i++) {
      if (pstreamers.get(i).isHealthy()) {
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Skips encoding and writing parity cells as there are "
        + "no healthy parity data streamers: " + pstreamers);
    }
    return false;
  }

  void writeParity(int index, ByteBuffer buffer, byte[] checksumBuf)
    throws IOException {
    final StripedDataStreamer current = setParityStreamer(index);
    final int len = buffer.limit();

    final long oldBytes = current.getBytesCurBlock();
    if (current.isHealthy()) {
      try {
        DataChecksum sum = getDataChecksum();
        if (buffer.isDirect()) {
          ByteBuffer directCheckSumBuf =
            BUFFER_POOL.getBuffer(true, checksumBuf.length);
          sum.calculateChunkedSums(buffer, directCheckSumBuf);
          directCheckSumBuf.get(checksumBuf);
          BUFFER_POOL.putBuffer(directCheckSumBuf);
        } else {
          sum.calculateChunkedSums(buffer.array(), 0, len, checksumBuf, 0);
        }
        for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
          int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
          int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize();
          super.writeParityChunk(buffer, chunkLen, checksumBuf, ckOffset,
            getChecksumSize());
        }
      } catch(Exception e) {
        handleCurrentParityStreamerFailure(current, "oldBytes=" + oldBytes + ", len=" + len,
          e);
      }
    }
  }

  @Override
  void setClosed() {
    super.setClosed();
    for (int i = 0; i < numStreamers; i++) {
      if (i < numDataBlocksInStripe) {
        getStripedDataStreamer(i).release();
      } else {
        getStripedParityStreamer(i - numDataBlocksInStripe).release();
      }
    }
    cellBuffer.release();
  }

  @Override
  protected void closeImpl() throws IOException {
    final long start = System.currentTimeMillis();
    try {
      if (isClosed()) {
        exceptionLastSeen.check(true);

        // Writing to at least {dataUnits} replicas can be considered as
        //  success, and the rest of data can be recovered.
        final int minReplication = ecPolicy.getNumDataUnits();
        int goodStreamers = 0;
        final MultipleIOException.Builder b = new MultipleIOException.Builder();
        for (final StripedDataStreamer si : streamers) {
          try {
            si.getLastException().check(true);
            goodStreamers++;
          } catch (IOException e) {
            b.add(e);
          }
        }
        if (goodStreamers < minReplication) {
          final IOException ioe = b.build();
          if (ioe != null) {
            throw ioe;
          }
        }
        return;
      }

      try {
        // flush from all upper layers
        flushBuffer();

        // if the last stripe is incomplete, generate and write parity cells
        if (generateParityCellsForLastStripe()) {
          setCurrentBlockGroup();
          computeParities(cellBuffer, getCurrentRow(), getCurrentIndex(), currentBlockGroup);
        }
        enqueueAllCurrentPackets();

        // flush all the data packets
        flushDataInternals();
        flushParityInternals();
        // check failures
        checkStreamerFailures(false);

        for (int i = 0; i < numStreamers; i++) {
          final boolean forData = i < numDataBlocksInStripe;
          final StripedDataStreamer s = forData
                  ? setCurrentStreamer(i)
                  : setParityStreamer(i - numDataBlocksInStripe);
          if (s.isHealthy()) {
            try {
              if (forData) {
                if (s.getBytesCurBlock() > 0) {
                  setCurrentPacketToEmpty();
                }
                // flush the last "close" packet to Datanode
                flushData();
              } else {
                if (s.getBytesCurBlock() > 0) {
                  setParityPacketToEmpty();
                }
                flushParity();
              }
            } catch (Exception e) {
              // TODO for both close and endBlock, we currently do not handle
              // failures when sending the last packet. We actually do not need to
              // bump GS for this kind of failure. Thus counting the total number
              // of failures may be good enough.
            }
          }
        }
      } finally {
        // Failures may happen when flushing data/parity data out. Exceptions
        // may be thrown if the number of failed streamers is more than the
        // number of parity blocks, or updatePipeline RPC fails. Streamers may
        // keep waiting for the new block/GS information. Thus need to force
        // closing these threads.
        closeThreads(true);
      }

      try (TraceScope ignored =
             dfsClient.getTracer().newScope("completeFile")) {
        completeFile(currentDataStripe, currentBlockGroupsInStripe.toArray(new ExtendedBlock[0]));
      }
      logCorruptBlocks();
    } catch (ClosedChannelException ignored) {
    } finally {
      setClosed();
      // shutdown executor of flushAll tasks
      flushAllExecutor.shutdownNow();
      encoder.release();
      totalClosingTime = System.currentTimeMillis() - start;
      if (totalClosingTime > 20 && metricsLoggingEnabled) {
        collectTotalLatencyMetrics();
      }
    }
  }

  @VisibleForTesting
  void enqueueAllCurrentPackets() throws IOException {
    int idx = streamers.indexOf(getCurrentStreamer());
    for(int i = 0; i < streamers.size(); i++) {
      final StripedDataStreamer si = setCurrentStreamer(i);
      if (si.isHealthy() && currentPacket != null) {
        try {
          enqueueCurrentPacket();
        } catch (IOException e) {
          handleCurrentStreamerFailure("enqueueAllCurrentPackets, i=" + i, e);
        }
      }
    }
    setCurrentStreamer(idx);
  }

  void flushDataInternals(int start, int end) throws IOException {
    Map<Future<Void>, Integer> flushAllFuturesMap = new HashMap<>();
    Future<Void> future = null;
    int current = getCurrentIndex();

    for (int i = start; i < end; i++) {
      final StripedDataStreamer s = setCurrentStreamer(i);
      if (s.isHealthy()) {
        try {
          // flush all data to Datanode
          final long toWaitFor = flushInternalWithoutWaitingAck();
          future = flushAllExecutorCompletionService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                s.waitForAckedSeqno(toWaitFor);
                return null;
              }
            });
          flushAllFuturesMap.put(future, i);
        } catch (Exception e) {
          handleCurrentStreamerFailure("flushInternal " + s, e);
        }
      }
    }
    setCurrentStreamer(current);
    for (int i = 0; i < flushAllFuturesMap.size(); i++) {
      try {
        future = flushAllExecutorCompletionService.take();
        future.get();
      } catch (InterruptedException ie) {
        throw DFSUtilClient.toInterruptedIOException(
          "Interrupted during waiting all streamer flush, ", ie);
      } catch (ExecutionException ee) {
        LOG.warn(
          "Caught ExecutionException while waiting all streamer flush, ", ee);
        StripedDataStreamer s = streamers.get(flushAllFuturesMap.get(future));
        handleStreamerFailure("flushInternal " + s,
          (Exception) ee.getCause(), s);
      }
    }
  }

  void flushParityInternals(int start, int end) throws IOException {
    Map<Future<Void>, Integer> flushAllFuturesMap = new HashMap<>();
    Future<Void> future = null;
    int current = getParityIndex() - numDataBlocksInStripe; // subtract to get physical position in list

    for (int i = start; i < end; i++) {
      final StripedDataStreamer s = setParityStreamer(i);
      if (s.isHealthy()) {
        try {
          // flush all data to Datanode
          final long toWaitFor = flushInternalParityWithoutWaitingAck();
          future = flushAllExecutorCompletionService.submit(
                  () -> {
                    s.waitForAckedSeqno(toWaitFor);
                    return null;
                  });
          flushAllFuturesMap.put(future, i);
        } catch (Exception e) {
          handleCurrentStreamerFailure("flushInternal " + s, e);
        }
      }
    }
    setParityStreamer(current);
    for (int i = 0; i < flushAllFuturesMap.size(); i++) {
      try {
        future = flushAllExecutorCompletionService.take();
        future.get();
      } catch (InterruptedException ie) {
        throw DFSUtilClient.toInterruptedIOException(
                "Interrupted during waiting all streamer flush, ", ie);
      } catch (ExecutionException ee) {
        LOG.warn(
                "Caught ExecutionException while waiting all streamer flush, ", ee);
        StripedDataStreamer s = pstreamers.get(flushAllFuturesMap.get(future));
        handleStreamerFailure("flushInternal " + s,
                (Exception) ee.getCause(), s);
      }
    }
  }

  void flushDataInternals() throws IOException {
    final long flushTime = System.nanoTime();
    flushDataInternals(0, numDataBlocksInStripe);
    timeSpentFlushingData += (System.nanoTime() - flushTime);
  }

  void flushParityInternals() throws IOException {
    final long flushTime = System.currentTimeMillis();
    flushParityInternals(currentBlockGroup.startPosition, currentBlockGroup.endPosition);
    timeSpentFlushingParity += (System.currentTimeMillis() - flushTime);
  }

  static void sleep(long ms, String op) throws InterruptedIOException {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException ie) {
      throw DFSUtilClient.toInterruptedIOException(
        "Sleep interrupted during " + op, ie);
    }
  }

  private void logCorruptBlocks() {
    for (Map.Entry<Integer, Integer> entry : corruptBlockCountMap.entrySet()) {
      int bgIndex = entry.getKey();
      int corruptBlockCount = entry.getValue();
      StringBuilder sb = new StringBuilder();
      sb.append("Block group <").append(bgIndex).append("> failed to write ")
        .append(corruptBlockCount).append(" blocks.");
      if (corruptBlockCount == numParityBlocksInGroup) {
        sb.append(" It's at high risk of losing data.");
      }
      LOG.warn(sb.toString());
    }
  }

  @Override
  ExtendedBlock getBlock() {
    return currentDataStripe;
  }
}
