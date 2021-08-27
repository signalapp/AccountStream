/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream.stream;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.signal.accountstream.proto.ContinuationTokenInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;

/** ShardHorizon stores and uses information about our already-processed kinesis
 *  stream data, using that information to request new, novel records from the stream
 *  and mark our place for future restarts.
 *
 * A kinesis stream consists of a set of Shards, where each Shard contains a
 * set of Records within a sequence range.  A Shard may either be complete, in which
 * case its sequence range is [a,b] for some `b`, or incomplete, in which case it
 * has a sequence range of [a,...] with no set end sequence number.  Sequence numbers
 * are not guaranteed contiguous (a+1 does not come after a), but they do increase.
 * The set of incomplete Shards (no end to their sequence range) is the set of Shards
 * currently being written to by kinesis.
 *
 * Shards form a parent/child DAG, where each shard will have zero or
 * more children.  When a shard is completed, the following
 * happens atomically:
 *
 *   - its range is updated with an end sequence number
 *   - zero or more shards are created to take on future records
 *     - If kinesis merges 2 partitions, one partition's shard will have zero children,
 *       it no longer exists.  The remaining, merged partition's shard will have a
 *       single new child shard.
 *     - If kinesis splits a partition, the original shard will be given N children,
 *       one for each created partition
 *
 * We want to keep track of a specific location within this set of multiple shards,
 * such that we can restart streaming from a particular point and be guaranteed
 * (1) not to see any records we've already processed and (2) to see all the records
 * we haven't processed yet.  This class wraps the implementation of that checkpoint,
 * a "shard horizon".  Considering the following shard DAG, where parents are to
 * the left of children, and a * denotes an un-closed shard.
 *
 *   [A] - [B] - [C]
 *            `- [D] - [E*]
 *   [F] - [G] - [H]
 *   [I] - [J] - [K] - [L*]
 *
 * It forms a forest, with roots A,F,I and leaves C,E,H,L.  We can store the position
 * of the latest shard(s) we're currently processing within each tree, and know exactly
 * where we are and how to restart.  For example, we start at [A,F,I] (we keep track of
 * sequence numbers in each).  When we finish A, we now keep track of [B,F,I].  If we
 * finish B, we keep track of [C,D,F,I].  Note that C is closed (it has an end sequence)
 * and has no children.  So, when we finish C, we can change to [D,F,I].  If we finish
 * D, F, G, and H, we keep track only of [E,I].
 *
 * At this point we crash, and we need to restart.  We load our horizon [E,I] and the
 * current stream description, which now looks like this, with A,F,G,H having fallen
 * off historically.
 *
 *   [B] - [C]
 *      `- [D] - [E] - [M*]
 *   [I] - [J] - [K] - [L*]
 *
 * If we can find all of the shards in our horizon [E,I], we know that we have a complete
 * set of data and can restart from that point.  By only moving forward our horizon when
 * the complete path back from that horizon point to the root has completed, and by never
 * processing a child until a parent has completed, we provably have an exact point from
 * which we can start.
 *
 * Let's say, though, that with our [E,I], we restart again and pull in this stream
 * description:
 *
 *   [B] - [C]
 *      `- [D] - [E] - [M*]
 *   [J] - [K] - [L*]
 *
 * We look for all shards within our horizon, but can't find [I].  Because we know that
 * we haven't finished processing [I], we've lost data.  At this point, our shard
 * horizon is no longer relevant, and we can only restart from the beginning.
 */
public class ShardHorizon {
  private static final Logger logger = LoggerFactory.getLogger(ShardHorizon.class);

  public static final String SHARD_NOT_STARTED = "";

  private final Map<String, String> shardIDsToCompletedSequenceNumbers;

  ShardHorizon(Map<String, String> shardIDsToCompletedSequenceNumbers) {
    this.shardIDsToCompletedSequenceNumbers = shardIDsToCompletedSequenceNumbers;
  }

  public Map<String, String> getShardIDsToCompletedSequenceNumbers() {
    return Collections.unmodifiableMap(shardIDsToCompletedSequenceNumbers);
  }

  /** Generate a continuation token from this horizon. */
  public ContinuationTokenInternal toContinuationToken() {
    return ContinuationTokenInternal.newBuilder()
        .putAllShardHorizonSequenceNumbers(shardIDsToCompletedSequenceNumbers)
        .build();
  }

  /** Generate a shard horizon from the given token. */
  public static ShardHorizon fromContinuationToken(ContinuationTokenInternal token) {
    // Copy the map into a new hash map, so it's mutable.
    return new ShardHorizon(new HashMap<>(token.getShardHorizonSequenceNumbersMap()));
  }

  public void markShardSequenceComplete(String shardID, String toSeqInclusive, ShardSet set) {
    logger.trace("Marking shard {} sequence {} complete", shardID, toSeqInclusive);
    shardIDsToCompletedSequenceNumbers.put(shardID, toSeqInclusive);
  }

  public void markShardSequenceClosed(String shardID, ShardSet set) {
    logger.trace("Marking shard {} closed", shardID);
    Shard shard = Preconditions.checkNotNull(set.getShard(shardID));
    String end = shard.sequenceNumberRange().endingSequenceNumber();
    if (end != null && !end.isEmpty()) {
      shardIDsToCompletedSequenceNumbers.remove(shardID);
      logger.trace("Shard {} ended", shardID);
      for (Shard child : set.getShardChildren(shardID)) {
        logger.trace("Shard {} started", child.shardId());
        shardIDsToCompletedSequenceNumbers.put(child.shardId(), SHARD_NOT_STARTED);
      }
    } else {
      logger.trace("Shard {} closed, but set old", shardID);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : shardIDsToCompletedSequenceNumbers.entrySet()) {
      sb.append(last8(e.getKey()));
      sb.append('=');
      sb.append(last8(e.getValue()));
      sb.append(',');
    }
    sb.deleteCharAt(sb.length()-1);
    return sb.toString();
  }

  private static CharSequence last8(String e) {
    return e.subSequence(Math.max(0, e.length()-8), e.length());
  }
}
