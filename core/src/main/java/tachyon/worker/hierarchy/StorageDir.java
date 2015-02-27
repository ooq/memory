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

package tachyon.worker.hierarchy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.eclipse.jetty.security.PropertyUserStore.UserListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.Users;
import tachyon.util.CommonUtils;
import tachyon.worker.BlockHandler;
import tachyon.worker.SpaceCounter;

/**
 * Stores and manages block files in storage's directory in different storage systems.
 */
public final class StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Mapping from blockId to blockSize in bytes */
  private final ConcurrentMap<Long, Long> mBlockSizes = new ConcurrentHashMap<Long, Long>();
  /** Mapping from blockId to its last access time in milliseconds */
  private final ConcurrentMap<Long, Long> mLastBlockAccessTimeMs =
      new ConcurrentHashMap<Long, Long>();
  /** Mapping from block to a list of accessible users */
  private final ConcurrentMap<Long, HashSet<Long>> mBlockToAllowedUsers =
      new ConcurrentHashMap<Long, HashSet<Long>>();
  /** Mapping from user to a map from blockId to last access time */
  private final ConcurrentMap<Long, ConcurrentMap<Long, Long>> mUserToBlockLastAccessTimeMs =
      new ConcurrentHashMap<Long, ConcurrentMap<Long, Long>>();
  /** List of added block Ids to be reported */
  private final BlockingQueue<Long> mAddedBlockIdList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  /** List of to be removed block Ids */
  private final Set<Long> mToRemoveBlockIdSet = Collections.synchronizedSet(new HashSet<Long>());
  /** Space counter of the StorageDir */
  private final SpaceCounter mSpaceCounter;
  /** Id of StorageDir */
  private final long mStorageDirId;
  /** Root path of the StorageDir */
  private final TachyonURI mDirPath;
  /** Path of the data in the StorageDir */
  private final TachyonURI mDataPath;
  /** Path of user temporary directory in the StorageDir */
  private final TachyonURI mUserTempPath;
  /** Under file system of current StorageDir */
  private final UnderFileSystem mFs;
  /** Configuration of under file system */
  private final Object mConf;
  /** Mapping from user Id to space size bytes owned by the user */
  private final ConcurrentMap<Long, Long> mOwnBytesPerUser = new ConcurrentHashMap<Long, Long>();
  /** Mapping from temporary block id to the size bytes allocated to it */
  private final ConcurrentMap<Pair<Long, Long>, Long> mTempBlockAllocatedBytes =
      new ConcurrentHashMap<Pair<Long, Long>, Long>();
  /** Mapping from user Id to list of blocks locked by the user */
  private final Multimap<Long, Long> mLockedBlocksPerUser = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());
  /** Mapping from block Id to list of users that lock the block */
  private final Multimap<Long, Long> mUserPerLockedBlock = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());

  /**
   * Create a new StorageDir.
   * 
   * @param storageDirId the id of the StorageDir
   * @param dirPath the root path of the StorageDir
   * @param capacityBytes the capacity of the StorageDir in bytes
   * @param dataFolder the data folder in the StorageDir
   * @param userTempFolder the temporary folder for users in the StorageDir
   * @param conf the configuration of the under file system
   */
  StorageDir(long storageDirId, String dirPath, long capacityBytes, String dataFolder,
      String userTempFolder, Object conf) {
    mStorageDirId = storageDirId;
    mDirPath = new TachyonURI(dirPath);
    mSpaceCounter = new SpaceCounter(capacityBytes);
    mDataPath = mDirPath.join(dataFolder);
    mUserTempPath = mDirPath.join(userTempFolder);
    mConf = conf;
    mFs = UnderFileSystem.get(dirPath, conf);
  }

  /**
   * update last access time of a block for a particular user
   */
  public void updateUserAccessBlock(long blockId, long userId) {

    LOG.info("block {} is accessed by user {}", blockId, userId);
    synchronized (mUserToBlockLastAccessTimeMs) {
      ConcurrentMap<Long, Long> accesses = mUserToBlockLastAccessTimeMs.get(userId);
      if (accesses == null) {
        accesses = new ConcurrentHashMap<Long, Long>();
      }
      accesses.put(blockId, System.currentTimeMillis());
    }
    HashSet<Long> userList = mBlockToAllowedUsers.get(blockId);
    if (userList == null) {
      userList = new HashSet<Long>();
    }
    userList.add(userId);
    mBlockToAllowedUsers.put(blockId, userList);
  }

  /**
   * update last access time of a block for a particular user
   */
  public void removeUserAccessBlock(long blockId) {
    LOG.info("block {} is deleted for all users", blockId);
    synchronized (mUserToBlockLastAccessTimeMs) {
      for (long userId : mUserToBlockLastAccessTimeMs.keySet()) {
        ConcurrentMap<Long, Long> accesses = mUserToBlockLastAccessTimeMs.get(userId);
        accesses.remove(blockId);
      }
    }
  }

  /**
   * Update the last access time of the block
   * 
   * @param blockId Id of the block
   */
  public void accessBlock(long blockId, long userId) {

    synchronized (mLastBlockAccessTimeMs) {
      if (containsBlock(blockId)) {
        mLastBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
      }
    }
    updateUserAccessBlock(blockId, userId);
  }

  /**
   * Update the last access time of the block
   * 
   * @param blockId Id of the block
   */
  public void accessBlock(long blockId) {
    synchronized (mLastBlockAccessTimeMs) {
      if (containsBlock(blockId)) {
        mLastBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
      }
    }
  }

  /**
   * Adds a block into the dir.
   * 
   * @param blockId the Id of the block
   * @param sizeBytes the size of the block in bytes
   * @param report need to be reported during heartbeat with master
   */
  private void addBlockId(long userId, long blockId, long sizeBytes, boolean report) {
    addBlockId(userId, blockId, sizeBytes, System.currentTimeMillis(), report);
  }

  /**
   * Adds a block into the dir.
   * 
   * @param blockId Id of the block
   * @param sizeBytes size of the block in bytes
   * @param accessTimeMs access time of the block in millisecond.
   * @param report whether need to be reported During heart beat with master
   */
  private void addBlockId(long userId, long blockId, long sizeBytes, long accessTimeMs,
      boolean report) {
    synchronized (mLastBlockAccessTimeMs) {
      mLastBlockAccessTimeMs.put(blockId, accessTimeMs);
      if (mBlockSizes.containsKey(blockId)) {
        mSpaceCounter.returnUsedBytes(mBlockSizes.remove(blockId));
      }
      mBlockSizes.put(blockId, sizeBytes);
      if (report) {
        mAddedBlockIdList.add(blockId);
      }
    }
    updateUserAccessBlock(blockId, userId);
  }

  /**
   * Move the cached block file from user temporary folder to data folder
   * 
   * @param userId the id of the user
   * @param blockId the id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean cacheBlock(long userId, long blockId) throws IOException {
    String srcPath = getUserTempFilePath(userId, blockId);
    String dstPath = getBlockFilePath(blockId);
    Pair<Long, Long> blockInfo = new Pair<Long, Long>(userId, blockId);

    if (!(mFs.exists(srcPath) && mTempBlockAllocatedBytes.containsKey(blockInfo))) {
      cancelBlock(userId, blockId);
      throw new IOException("Block file doesn't exist! blockId:" + blockId + " " + srcPath);
    }
    long blockSize = mFs.getFileSize(srcPath);
    if (blockSize < 0) {
      cancelBlock(userId, blockId);
      throw new IOException("Negative block size! blockId:" + blockId);
    }
    Long allocatedBytes = mTempBlockAllocatedBytes.remove(blockInfo);
    returnSpace(userId, allocatedBytes - blockSize);
    if (mFs.rename(srcPath, dstPath)) {
      addBlockId(userId, blockId, blockSize, false);
      updateUserOwnBytes(userId, -blockSize);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Cancel a block which is being written
   * 
   * @param userId the id of the user
   * @param blockId the id of the block to be cancelled
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean cancelBlock(long userId, long blockId) throws IOException {
    String filePath = getUserTempFilePath(userId, blockId);
    Long allocatedBytes = mTempBlockAllocatedBytes.remove(new Pair<Long, Long>(userId, blockId));
    if (allocatedBytes == null) {
      allocatedBytes = 0L;
    }
    returnSpace(userId, allocatedBytes);
    if (!mFs.exists(filePath)) {
      return true;
    } else {
      return mFs.delete(filePath, false);
    }
  }

  /**
   * Clean resources related to the removed user
   * 
   * @param userId id of the removed user
   * @param tempBlockIdList list of block ids that are being written by the user
   */
  public void cleanUserResources(long userId, Collection<Long> tempBlockIdList) {
    Collection<Long> blockIds = mLockedBlocksPerUser.removeAll(userId);
    for (long blockId : blockIds) {
      mUserPerLockedBlock.remove(blockId, userId);
    }
    for (Long tempBlockId : tempBlockIdList) {
      mTempBlockAllocatedBytes.remove(new Pair<Long, Long>(userId, tempBlockId));
    }
    try {
      mFs.delete(getUserTempPath(userId), true);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
    returnSpace(userId);
  }

  /**
   * Check whether the StorageDir contains certain block
   * 
   * @param blockId Id of the block
   * @return true if StorageDir contains the block, false otherwise
   */
  public boolean containsBlock(long blockId) {
    return mLastBlockAccessTimeMs.containsKey(blockId);
  }

  /**
   * Copy block file from this StorageDir to another StorageDir, the caller needs to make sure the
   * block is locked during copying
   * 
   * @param blockId Id of the block
   * @param dstDir destination StorageDir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean copyBlock(long blockId, StorageDir dstDir) throws IOException {
    long size = getBlockSize(blockId);
    if (size == -1) {
      LOG.error("Block file doesn't exist! blockId:{}", blockId);
      return false;
    }
    boolean copySuccess = false;
    Closer closer = Closer.create();
    ByteBuffer buffer = null;
    try {
      BlockHandler bhSrc = closer.register(getBlockHandler(blockId));
      BlockHandler bhDst = closer.register(dstDir.getBlockHandler(blockId));
      buffer = bhSrc.read(0, (int) size);
      copySuccess = (bhDst.append(0, buffer) == size);
    } finally {
      closer.close();
      CommonUtils.cleanDirectBuffer(buffer);
    }
    if (copySuccess) {
      dstDir.addBlockId(blockId, size, mLastBlockAccessTimeMs.get(blockId), true);
    }
    return copySuccess;
  }

  /**
   * Remove a block from current StorageDir, once calling this method, the block will not be
   * available any longer
   * 
   * @param blockId Id of the block to be removed.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public boolean deleteBlock(long blockId) throws IOException {
    Long accessTimeMs = mLastBlockAccessTimeMs.remove(blockId);
    removeUserAccessBlock(blockId);
    if (accessTimeMs == null) {
      LOG.warn("Block does not exist in current StorageDir! blockId:{}", blockId);
      return false;
    }
    String blockfile = getBlockFilePath(blockId);
    // Should check lock status here
    if (!isBlockLocked(blockId)) {
      if (!mFs.delete(blockfile, false)) {
        LOG.error("Failed to delete block file! filename:{}", blockfile);
        return false;
      }
      deleteBlockId(blockId);
    } else {
      mToRemoveBlockIdSet.add(blockId);
      LOG.debug("Add block file {} to remove list!", blockfile);
    }
    return true;
  }

  /**
   * Delete information of a block from current StorageDir
   * 
   * @param blockId Id of the block
   */
  private void deleteBlockId(long blockId) {
    synchronized (mLastBlockAccessTimeMs) {
      mLastBlockAccessTimeMs.remove(blockId);
      mSpaceCounter.returnUsedBytes(mBlockSizes.remove(blockId));
      if (mAddedBlockIdList.contains(blockId)) {
        mAddedBlockIdList.remove(blockId);
      }
    }
  }

  /**
   * Get Ids of newly added blocks
   * 
   * @return list of added block Ids
   */
  public List<Long> getAddedBlockIdList() {
    List<Long> addedBlockIdList = new ArrayList<Long>();
    mAddedBlockIdList.drainTo(addedBlockIdList);
    return addedBlockIdList;
  }

  /**
   * Get available space size in bytes in current StorageDir
   * 
   * @return available space size in current StorageDir
   */
  public long getAvailableBytes() {
    return mSpaceCounter.getAvailableBytes();
  }

  /**
   * Read data into ByteBuffer from some block file, caller needs to make sure the block is locked
   * during reading
   * 
   * @param blockId Id of the block
   * @param offset starting position of the block file
   * @param length length of data to read
   * @return ByteBuffer which contains data of the block
   * @throws IOException
   */
  public ByteBuffer getBlockData(long blockId, long offset, int length) throws IOException {
    BlockHandler bh = getBlockHandler(blockId);
    try {
      return bh.read(offset, length);
    } finally {
      bh.close();
      accessBlock(blockId);
    }
  }

  /**
   * Get file path of the block file
   * 
   * @param blockId Id of the block
   * @return file path of the block
   */
  public String getBlockFilePath(long blockId) {
    return mDataPath.join("" + blockId).toString();
  }

  /**
   * Get block handler used to access the block file
   * 
   * @param blockId Id of the block
   * @return block handler of the block file
   * @throws IOException
   */
  public BlockHandler getBlockHandler(long blockId) throws IOException {
    String filePath = getBlockFilePath(blockId);
    try {
      return BlockHandler.get(filePath);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Get Ids of the blocks in current StorageDir
   * 
   * @return Ids of the blocks in current StorageDir
   */
  public Set<Long> getBlockIds() {
    return mLastBlockAccessTimeMs.keySet();
  }

  /**
   * Get size of the block in bytes
   * 
   * @param blockId Id of the block
   * @return size of the block, -1 if block doesn't exist
   */
  public long getBlockSize(long blockId) {
    Long size = mBlockSizes.get(blockId);
    if (size == null) {
      return -1;
    } else {
      return size;
    }
  }

  /**
   * Get sizes of the blocks in bytes in current StorageDir
   * 
   * @return set of map entry mapping from block Id to the block size in current StorageDir
   */
  public Set<Entry<Long, Long>> getBlockSizes() {
    return mBlockSizes.entrySet();
  }

  /**
   * Get capacity of current StorageDir in bytes
   * 
   * @return capacity of current StorageDir in bytes
   */
  public long getCapacityBytes() {
    return mSpaceCounter.getCapacityBytes();
  }

  /**
   * Get data path of current StorageDir
   * 
   * @return data path of current StorageDir
   */
  public TachyonURI getDirDataPath() {
    return mDataPath;
  }

  /**
   * Get root path of current StorageDir
   * 
   * @return root path of StorageDir
   */
  public TachyonURI getDirPath() {
    return mDirPath;
  }

  /**
   * Get last access time of blocks in current StorageDir
   * 
   * @return set of map entry mapping from block Id to its last access time in current StorageDir
   */
  public Set<Entry<Long, Long>> getLastBlockAccessTimeMs() {
    return mLastBlockAccessTimeMs.entrySet();
  }

  /**
   * Get last access time of blocks in current StorageDir for a given user
   * 
   * @return set of map entry mapping from block Id to its last access time in current StorageDir
   */
  public Set<Entry<Long, Long>> getUserLastBlockAccessTimeMs(long userId) {
    return mUserToBlockLastAccessTimeMs.get(userId).entrySet();
  }
  
  /**
   * Get size of locked blocks in bytes in current StorageDir
   * 
   * @return size of locked blocks in bytes in current StorageDir
   */
  public long getLockedSizeBytes() {
    long lockedBytes = 0;
    for (long blockId : mUserPerLockedBlock.keySet()) {
      Long blockSize = mBlockSizes.get(blockId);
      if (blockSize != null) {
        lockedBytes += blockSize;
      }
    }
    return lockedBytes;
  }

  /**
   * Get Id of current StorageDir
   * 
   * @return Id of current StorageDir
   */
  public long getStorageDirId() {
    return mStorageDirId;
  }

  /**
   * Get current StorageDir's under file system
   * 
   * @return StorageDir's under file system
   */
  public UnderFileSystem getUfs() {
    return mFs;
  }

  /**
   * Get configuration of current StorageDir's under file system
   * 
   * @return configuration of the under file system
   */
  public Object getUfsConf() {
    return mConf;
  }

  /**
   * Get used space in bytes in current StorageDir
   * 
   * @return used space in bytes in current StorageDir
   */
  public long getUsedBytes() {
    return mSpaceCounter.getUsedBytes();
  }

  /**
   * Get temporary space owned by the user in current StorageDir
   * 
   * @return temporary space in bytes owned by the user in current StorageDir
   */
  public long getUserOwnBytes(long userId) {
    Long ownBytes = mOwnBytesPerUser.get(userId);
    if (ownBytes == null) {
      ownBytes = 0L;
    }
    return ownBytes;
  }

  /**
   * Get temporary file path of block written by some user
   * 
   * @param userId Id of the user
   * @param blockId Id of the block
   * @return temporary file path of the block
   */
  public String getUserTempFilePath(long userId, long blockId) {
    return mUserTempPath.join("" + userId).join("" + blockId).toString();
  }

  /**
   * Get root temporary path of users
   * 
   * @return TachyonURI of users' temporary path
   */
  public TachyonURI getUserTempPath() {
    return mUserTempPath;
  }

  /**
   * Get temporary path of some user
   * 
   * @param userId Id of the user
   * @return temporary path of the user
   */
  public String getUserTempPath(long userId) {
    return mUserTempPath.join("" + userId).toString();
  }

  /**
   * Initialize current StorageDir
   * 
   * @throws IOException
   */
  public void initailize() throws IOException {
    String dataPath = mDataPath.toString();
    if (!mFs.exists(dataPath)) {
      LOG.info("Data folder {} does not exist. Creating a new one.", mDataPath);
      mFs.mkdirs(dataPath, true);
      mFs.setPermission(dataPath, "775");
    } else if (mFs.isFile(dataPath)) {
      String msg = "Data folder " + mDataPath + " is not a folder!";
      throw new IllegalArgumentException(msg);
    }

    String userTempPath = mUserTempPath.toString();
    if (!mFs.exists(userTempPath)) {
      LOG.info("User temp folder {} does not exist. Creating a new one.", mUserTempPath);
      mFs.mkdirs(userTempPath, true);
      mFs.setPermission(userTempPath, "775");
    } else if (mFs.isFile(userTempPath)) {
      String msg = "User temp folder " + mUserTempPath + " is not a folder!";
      throw new IllegalArgumentException(msg);
    }

    int cnt = 0;
    for (String name : mFs.list(dataPath)) {
      String path = mDataPath.join(name).toString();
      if (mFs.isFile(path)) {
        cnt ++;
        long fileSize = mFs.getFileSize(path);
        LOG.debug("File {}: {} with size {} Bs.", cnt, path, fileSize);
        long blockId = CommonUtils.getBlockIdFromFileName(name);
        boolean success = mSpaceCounter.requestSpaceBytes(fileSize);
        if (success) {
          addBlockId(999, blockId, fileSize, true);
        } else {
          mFs.delete(path, true);
          LOG.warn("Pre-existing files exceed storage capacity. deleting file:{}", path);
        }
      }
    }
    return;
  }

  /**
   * Check whether certain block is locked
   * 
   * @param blockId Id of the block
   * @return true if block is locked, false otherwise
   */
  public boolean isBlockLocked(long blockId) {
    return mUserPerLockedBlock.containsKey(blockId);
  }

  /**
   * Lock block by some user
   * 
   * @param blockId Id of the block
   * @param userId Id of the user
   * @return true if success, false otherwise
   */
  public boolean lockBlock(long blockId, long userId) {
    synchronized (mLastBlockAccessTimeMs) {
      if (!containsBlock(blockId)) {
        return false;
      }
      mUserPerLockedBlock.put(blockId, userId);
      mLockedBlocksPerUser.put(userId, blockId);
      return true;
    }
  }

  /**
   * Move a block from its current StorageDir to another StorageDir
   * 
   * @param blockId the id of the block
   * @param dstDir the destination StorageDir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean moveBlock(long blockId, StorageDir dstDir) throws IOException {
    if (copyBlock(blockId, dstDir)) {
      return deleteBlock(blockId);
    }
    return false;
  }

  /**
   * Request space from current StorageDir by some user
   * 
   * @param userId Id of the user
   * @param size request size in bytes
   * @return true if success, false otherwise
   */
  public boolean requestSpace(long userId, long size) {
    boolean result = mSpaceCounter.requestSpaceBytes(size);
    if (result && userId != Users.MIGRATE_DATA_USER_ID) {
      updateUserOwnBytes(userId, size);
    }
    return result;
  }

  /**
   * Return space owned by the user to current StorageDir
   * 
   * @param userId Id of the user
   */
  private void returnSpace(long userId) {
    Long ownBytes = mOwnBytesPerUser.remove(userId);
    if (ownBytes != null) {
      mSpaceCounter.returnUsedBytes(ownBytes);
    }
  }

  /**
   * Return space to current StorageDir by some user
   * 
   * @param userId Id of the user
   * @param size size to return in bytes
   */
  public void returnSpace(long userId, long size) {
    mSpaceCounter.returnUsedBytes(size);
    updateUserOwnBytes(userId, -size);
  }

  /**
   * Unlock block by some user
   * 
   * @param blockId Id of the block
   * @param userId Id of the user
   * @return true if success, false otherwise
   */
  public boolean unlockBlock(long blockId, long userId) {
    if (mUserPerLockedBlock.remove(blockId, userId)) {
      mLockedBlocksPerUser.remove(userId, blockId);
      if (!isBlockLocked(blockId) && mToRemoveBlockIdSet.contains(blockId)) {
        try {
          if (!mFs.delete(getBlockFilePath(blockId), false)) {
            return false;
          }
          mToRemoveBlockIdSet.remove(blockId);
          deleteBlockId(blockId);
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Update allocated space bytes of a temporary block in current StorageDir
   * 
   * @param userId Id of the user
   * @param blockId Id of the block
   * @param sizeBytes updated space size in bytes
   */
  public void updateTempBlockAllocatedBytes(long userId, long blockId, long sizeBytes) {
    Pair<Long, Long> blockInfo = new Pair<Long, Long>(userId, blockId);
    Long oldSize = mTempBlockAllocatedBytes.putIfAbsent(blockInfo, sizeBytes);
    if (oldSize != null) {
      while (!mTempBlockAllocatedBytes.replace(blockInfo, oldSize, oldSize + sizeBytes)) {
        oldSize = mTempBlockAllocatedBytes.get(blockInfo);
        if (oldSize == null) {
          LOG.error("Temporary block doesn't exist! blockId:{}", blockId);
          break;
        }
      }
    }
  }

  /**
   * Update user owned space bytes
   * 
   * @param userId Id of the user
   * @param sizeBytes updated space size in bytes
   */
  private void updateUserOwnBytes(long userId, long sizeBytes) {
    Long used = mOwnBytesPerUser.putIfAbsent(userId, sizeBytes);
    if (used != null) {
      while (!mOwnBytesPerUser.replace(userId, used, used + sizeBytes)) {
        used = mOwnBytesPerUser.get(userId);
        if (used == null) {
          LOG.error("Unknown user! userId:{}", userId);
          break;
        }
      }
    }
  }

  public Pair<Long, Long> findToEvictBlock(long toBenefitUser, 
      HashMap<Long, HashSet<Long>> toEvictRecords) {
    // first: update own bytes by apply records
    // toEvictRecords is a block to a collection of users
    HashMap<Long, Long> userBytesHashMap = new HashMap<Long, Long>();
    for (Long uId : mOwnBytesPerUser.keySet()) {
      userBytesHashMap.put(uId, mOwnBytesPerUser.get(uId));
    }
    for (Long bId: toEvictRecords.keySet()) {
      long bSize = mBlockSizes.get(bId);
      HashSet<Long> userSet = toEvictRecords.get(bId);
      long nOldUsers = mBlockToAllowedUsers.get(bId).size();
      long oldCost = bSize / nOldUsers;
      long nNewUsers = nOldUsers - userSet.size();
      long newCost =  (nNewUsers == 0) ? bSize : (bSize/nNewUsers);
      for (Long userInSet: userSet) {
        long updated = userBytesHashMap.get(userInSet) - oldCost + newCost;
        userBytesHashMap.put(userInSet, updated);
      }
    }
    // now,  pick the user with maximum value
    long maxValue = -1;
    long toEvictUser = -1;
    for (Long uId : userBytesHashMap.keySet()) {
      long bytes = userBytesHashMap.get(uId);
      if (bytes > maxValue || (bytes == maxValue && uId == toBenefitUser)) {
        toEvictUser = uId;
        maxValue = bytes;
      }
    }
    // now, find the lru block, which is not already evicted, from the user 
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Set<Entry<Long, Long>> accessTimes = getUserLastBlockAccessTimeMs(toEvictUser);

    for (Entry<Long, Long> accessTime : accessTimes) {
      if (toEvictRecords.containsKey(accessTime.getKey()) &&
          toEvictRecords.get(accessTime.getKey()).contains(toEvictUser)) {
        continue;
      }
      if (accessTime.getValue() < oldestTime && !isBlockLocked(accessTime.getKey())) {
          oldestTime = accessTime.getValue();
          blockId = accessTime.getKey();
      }
    }
    // update the evict records
    HashSet<Long> userSet = toEvictRecords.get(blockId);
    if (userSet == null) {
      userSet = new HashSet<Long>();
    }
    userSet.add(toEvictUser);
    toEvictRecords.put(blockId, userSet);
    // 
    long nOldUsers = mBlockToAllowedUsers.get(blockId).size();
    long nNewUsers = nOldUsers - userSet.size();
    if (nNewUsers == 0) {
      return new Pair<Long, Long>(blockId, oldestTime);
    } else {
      return findToEvictBlock(toBenefitUser, toEvictRecords);
    }
  }
}
