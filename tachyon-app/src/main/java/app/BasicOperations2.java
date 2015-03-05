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

package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

import com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.client.ReadType;
import tachyon.util.CommonUtils;

public class BasicOperations2 implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private  TachyonURI mMasterLocation;
  private  TachyonURI mFilePath;
  private  String mPathUrl;
  private  WriteType mWriteType;
  private  int mNumbers = 20;
  private String[] args;
  private long mUserId;

  public BasicOperations2(String[] args) {
    mUserId = Long.parseLong(args[0]);
    this.args = Arrays.copyOfRange(args, 1,args.length);
  }

  @Override
  public Boolean call() throws Exception {
    //createFile(tachyonClient);
    if (args[0].equals("copyFromLocal")){
      copyFromLocal(args);
    } else if (args[0].equals("read")) {
      readFile(args);
    } else {
      System.out.println("do nothing for " + args[0]);
    }
      
    return true;
    //return readFile(tachyonClient);
  }

  private void createFile(TachyonFS tachyonClient) throws IOException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = tachyonClient.createFile(mFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  private void writeFile(TachyonFS tachyonClient) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    OutStream os = file.getOutStream(mWriteType);
    os.write(buf.array());
    os.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "writeFile to file " + mFilePath);
  }

  public int copyFromLocal(String[] argv) throws IOException {
    if (argv.length != 5) {
      System.out.println("Usage: tfs copyFromLocal <src> <remoteDst> <master> <type>");
      return -1;
    }

    String srcPath = argv[1];
    TachyonURI dstPath = new TachyonURI(argv[2]);
    File src = new File(srcPath);
    if (!src.exists()) {
      System.out.println("Local path " + srcPath + " does not exist.");
      return -1;
    }
    TachyonFS tachyonClient = TachyonFS.get(argv[3]);
    tachyonClient.setUserId(mUserId);
    long startTimeMs = CommonUtils.getCurrentMs();
    int ret = copyPath(src, tachyonClient, dstPath, argv[4]);
    if (ret == 0) {
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Copied " + srcPath + " to " + dstPath);
    }
    return ret;
  }

  private int copyPath(File src, TachyonFS tachyonClient, TachyonURI dstPath, String typeString) throws IOException {
    if (!src.isDirectory()) {
      TachyonFile tFile = tachyonClient.getFile(dstPath);
      if (tFile != null && tFile.isDirectory()) {
        dstPath = dstPath.join(src.getName());
      }
      int fileId = tachyonClient.createFile(dstPath);
      if (fileId == -1) {
        return -1;
      }
      tFile = tachyonClient.getFile(fileId);
      Closer closer = Closer.create();
      try {
        OutStream os = closer.register(tFile.getOutStream(WriteType.valueOf(typeString)));
        FileInputStream in = closer.register(new FileInputStream(src));
        FileChannel channel = closer.register(in.getChannel());
        ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
        while (channel.read(buf) != -1) {
          buf.flip();
          os.write(buf.array(), 0, buf.limit());
        }
      } finally {
        closer.close();
      }
      return 0;
    } else {
      tachyonClient.mkdir(dstPath);
      for (String file : src.list()) {
        TachyonURI newPath = new TachyonURI(dstPath, new TachyonURI(file));
        File srcFile = new File(src, file);
        if (copyPath(srcFile, tachyonClient, newPath, typeString) == -1) {
          return -1;
        }
      }
    }
    return 0;
  }

  public int readFile(String[] argv) throws IOException {
    if (argv.length != 4) {
      System.out.println("Usage: tfs copyToLocal <src> <master> <type>");
      return -1;
    }

    TachyonURI srcPath = new TachyonURI(argv[1]);
    //String dstPath = argv[2];
    //File dst = new File(dstPath);
    TachyonFS tachyonClient = TachyonFS.get(argv[2]);
    tachyonClient.setUserId(mUserId);
    TachyonFile tFile = tachyonClient.getFile(srcPath);

    // tachyonClient.getFile() catches FileDoesNotExist exceptions and returns null
    if (tFile == null) {
      throw new IOException(srcPath.toString());
    }

    Closer closer = Closer.create();
    try {
      long startTimeMs = CommonUtils.getCurrentMs();
      InStream is = closer.register(tFile.getInStream(ReadType.valueOf(argv[3])));
      //FileOutputStream out = closer.register(new FileOutputStream(dst));
      byte[] buf = new byte[64 * Constants.MB];
      int t = is.read(buf);
      while (t != -1) {
        //out.write(buf, 0, t);
        t = is.read(buf);
      }
      CommonUtils.printTimeTakenMs(startTimeMs, LOG, "Read " + srcPath);
      return 0;
    } finally {
      closer.close();
    }
  }

  private boolean readFileOriginal(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;
    LOG.debug("Reading data...");

    final long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    TachyonByteBuffer buf = file.readByteBuffer(0);
    if (buf == null) {
      file.recache();
      buf = file.readByteBuffer(0);
    }
    buf.mData.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      pass = pass && (buf.mData.getInt() == k);
    }
    buf.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "readFile file " + mFilePath);
    return pass;
  }

  public static void main(String[] args) throws IllegalArgumentException {
    Utils.runExample(new BasicOperations2(args));
  }
}
