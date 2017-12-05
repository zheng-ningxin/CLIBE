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
package org.apache.hadoop.hdfs.server.datanode;

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

import java.net.Socket;         //added for test
import java.io.DataInputStream; //added for test
import java.io.DataOutputStream; //added for test
import java.io.InputStreamReader; //added for test

/**
 * This Class is a daemon thread which used to send the statistic
 * information to Namenode regularly. The time Unit is 2s
 */
class FeedBackManagerDatanodeSide implements Runnable {
  public static final Logger LOG = DataNode.LOG;
  private final DataNode datanode;
  private final DataXceiverServer dataxceiverserver;
  private long  lastupdate;
  private boolean updaterequired=false;                                             //If Datanode doesn't have the information, set this flag to true,and a suitable bandwidth will be fetched from Namenode
  private boolean closed = false;
  private final long DEFAULT_IOBANDWIDTH=10240000;                                  //10M If the datanode does not have the information about this request ,it will get a default bandwidth
  private final long AUTO_UPDATE_INTERVAL=2;
  private final long LOOP_TIME_UNIT=2000;                                            //ervery half second check once
  private DatanodeProtocolClientSideTranslatorPB bpNamenode;                        //Rpc Handler to communicate with the Namenode
  FeedBackManagerDatanodeSide(DataNode datanode,DataXceiverServer dataxceiverserver){
      this.datanode=datanode;
      this.dataxceiverserver=dataxceiverserver;
      lastupdate=monotonicNow();
  }
  @Override
  public synchronized void run() {
    while (datanode.shouldRun && !datanode.shutdownForUpgrade && !this.closed) {
        try {
            //LOG.info("Test for IOBandwidthManagerDatanodeSide if alive!\n");
            dataxceiverserver.sendStatisticReport();
            wait(LOOP_TIME_UNIT);

        }catch(Exception exception){
            LOG.warn(datanode.getDisplayName()+
                ":FeedBackManagerDatanodeSide:Runing exception",exception);
        }
    }

  }
  void kill(){
    assert (datanode.shouldRun == false || datanode.shutdownForUpgrade) :
      "shoudRun should be set to false or restarting should be true"
      + " before killing";
    try {
      this.closed = true;
    } catch (Exception ie) {
      LOG.warn(datanode.getDisplayName() + ":IOBandwidthManagerDatanodeSide.kill(): ", ie);
    }
  }



}
