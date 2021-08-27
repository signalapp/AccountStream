/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.accountstream.stream;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;

/** ShardSet is a simple in-memory representation of the Shard DAG that makes up a DynamoDB stream.
 *
 * We keep track of shards by ID, and a Shard object contains its own parent.  We also keep track
 * of a precomputed mapping of parent->child.
 *
 * See ShardHorizon for more details.
 */
public class ShardSet {
  private static final Logger logger = LoggerFactory.getLogger(ShardSet.class);
  private Map<String, Shard> shards = new HashMap<>();
  private Map<String, Set<String>> shardsToChildren = new HashMap<>();

  public ShardSet(List<Shard> shards) {
    for (Shard s : shards) {
      logger.trace("Shard {} has parent {}", s.shardId(), s.parentShardId());
      this.shards.put(s.shardId(), s);
      if (s.parentShardId() != null) {
        this.shardsToChildren.computeIfAbsent(s.parentShardId(), shardID -> new HashSet<>()).add(s.shardId());
      }
    }
  }

  Collection<Shard> getShardChildren(String shardID) {
    return shardsToChildren.getOrDefault(shardID, Collections.emptySet()).stream()
        .map(x -> shards.get(x)).collect(Collectors.toList());
  }

  Shard getShard(String shardID){
    return shards.get(shardID);
  }

  public ShardHorizon fromTheBeginning() {
    Map<String, String> shardIDsToCompletedSequenceNumbers = new HashMap<>();
    for (Map.Entry<String, Shard> entry : shards.entrySet()) {
      if (entry.getValue().parentShardId() == null || !shards.containsKey(entry.getValue().parentShardId())) {
        shardIDsToCompletedSequenceNumbers.put(entry.getKey(), ShardHorizon.SHARD_NOT_STARTED);
      }
    }
    return new ShardHorizon(shardIDsToCompletedSequenceNumbers);
  }

  public boolean validHorizon(ShardHorizon horizon) {
    Map<String, String> shardSequences = horizon.getShardIDsToCompletedSequenceNumbers();
    if (shardSequences.isEmpty()) {
      return false;
    }
    for (String key : shardSequences.keySet()) {
      if (getShard(key) == null) {
        return false;
      }
    }
    return true;
  }
}
