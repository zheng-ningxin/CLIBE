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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import com.google.common.annotations.VisibleForTesting;
//*************************************
import org.apache.hadoop.hdfs.server.protocol.AppRegisterTable;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import static  org.apache.hadoop.util.Time.monotonicNow;
import java.util.*;
import java.net.InetSocketAddress;

import org.slf4j.Logger;

import java.net.Socket;             //added for test
import java.io.DataInputStream;     //added for test
import java.io.DataOutputStream;    //added for test
import java.io.InputStreamReader;   //added for test

/**
 * This Class is a daemon thread which used to send the statistic
 * information to Namenode regularly. The time Unit is 2s
 */
class FeedBackManagerNamenodeSide implements Runnable {
  public static final Logger LOG = NameNode.LOG;
  private final NameNode namenode;
  private long  lastupdate;
  private boolean closed = false;
  private final long LOOP_TIME_UNIT;           
  FeedBackManagerNamenodeSide(NameNode namenode){
      this.namenode=namenode;
      lastupdate=monotonicNow();
      LOOP_TIME_UNIT=namenode.FeedBackPeriodDuration;
  }
  @Override
  public synchronized void run() {
    while (namenode.isActiveState() && !this.closed) {
        try {
            namenode.filterInvalid();   
            wait(LOOP_TIME_UNIT);
        }catch(Exception exception){
            LOG.warn("FeedBackManagerNamenodeSide:Runing exception",exception);
        }
    }

  }
  void kill(){
    assert (namenode.isActiveState() == false ) :
      "shoudRun should be set to false or restarting should be true"
      + " before killing";
    try {
      this.closed = true;
    } catch (Exception ie) {
      LOG.warn("IOBandwidthManagerNamenodeSide.kill(): ", ie);
    }
  }



}
