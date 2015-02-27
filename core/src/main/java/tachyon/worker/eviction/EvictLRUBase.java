/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.eviction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import tachyon.Pair;
import tachyon.master.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Base class for evicting blocks by LRU strategy.
 */
public abstract class EvictLRUBase implements EvictStrategy {
  private final boolean mLastTier;

  protected EvictLRUBase(boolean lastTier) {
    mLastTier = lastTier;
  }

  /**
   * Check if current block can be evicted
   * 
   * @param blockId the Id of the block
   * @param pinList the list of the pinned files
   * @return true if the block can be evicted, false otherwise
   */
  protected boolean blockEvictable(long blockId, Set<Integer> pinList) {
    return !mLastTier || !pinList.contains(BlockInfo.computeInodeId(blockId));
  }

  /**
   * Get the oldest access information of certain StorageDir
   * @param curDir current StorageDir
   * @param toEvictBlockIds the Ids of blocks that have been selected to be evicted
   * @param pinList list of pinned files
   * @return the oldest access information of current StorageDir
   */
  protected Pair<Long, Long> getLRUBlock(StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList) {
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Set<Entry<Long, Long>> accessTimes = curDir.getLastBlockAccessTimeMs();

    for (Entry<Long, Long> accessTime : accessTimes) {
      if (toEvictBlockIds.contains(accessTime.getKey())) {
        continue;
      }
      if (accessTime.getValue() < oldestTime && !curDir.isBlockLocked(accessTime.getKey())) {
        if (blockEvictable(accessTime.getKey(), pinList)) {
          oldestTime = accessTime.getValue();
          blockId = accessTime.getKey();
        }
      }
    }

    return new Pair<Long, Long>(blockId, oldestTime);
  }
  
  /**
   * Get the oldest access information of certain StorageDir
   * We will use the multi-user strategy--
   * start with the user with most space, adjust space in the meanwhile
   * we do this until we can find a block that is actually evcitable
   * 
   * we now assume that space requesting will eventually succeed,
   * so we evict along return blocks
   * 
   * now implementing the no sharing scenario
   * @param curDir current StorageDir
   * @param toEvictBlockIds the Ids of blocks that have been selected to be evicted
   * @param pinList list of pinned files
   * @return the oldest access information of current StorageDir
   */
  protected Pair<Long, Long> getLRUBlock(long userId, StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList) {
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Set<Entry<Long, Long>> accessTimes = curDir.getUserLastBlockAccessTimeMs(userId);

    for (Entry<Long, Long> accessTime : accessTimes) {
      if (toEvictBlockIds.contains(accessTime.getKey())) {
        continue;
      }
      if (accessTime.getValue() < oldestTime && !curDir.isBlockLocked(accessTime.getKey())) {
          oldestTime = accessTime.getValue();
          blockId = accessTime.getKey();
      }
    }

    return new Pair<Long, Long>(blockId, oldestTime);
  }
  
  protected Pair<Long, Long> getLRUBlockUser(long userId, StorageDir curDir, 
        HashMap<Long, HashSet<Long>> toEvictRecords,
      Set<Integer> pinList) {
    return curDir.findToEvictBlock(userId, toEvictRecords);
  }
}
