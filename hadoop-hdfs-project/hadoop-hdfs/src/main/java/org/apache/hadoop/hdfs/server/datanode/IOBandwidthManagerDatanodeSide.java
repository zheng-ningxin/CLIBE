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
 * This class is a daemon thread used to manage the IO bandwidth
 * at the Datanode side. Its task is to update the DataXceiver's
 * IOBandwidth regularly(For example every two seconds). In addition
 * to that, if a R/W request reach to the Datanode before the update
 * time point, it will also trigger a update, and the next update action
 * will be at the next 2s
 */
class IOBandwidthManagerDatanodeSide implements Runnable {
  public static final Logger LOG = DataNode.LOG;
  private final DataNode datanode;
  private final DataXceiverServer dataxceiverserver;
  private long  lastupdate;
  private final HashMap<String,Long> bandwidthtable = new HashMap<String,Long>();   //tabel stores the client name and the correspinding IO bandwidth
  private final HashMap<String,Integer> showtimestable = new HashMap<String, Integer>();    //table stores the client name and the correspinding show times(one dfsclient may have multi stream)
  private boolean updaterequired=false;                                             //If Datanode doesn't have the information, set this flag to true,and a suitable bandwidth will be fetched from Namenode
  private boolean closed = false;
  private final long DEFAULT_IOBANDWIDTH=10240000;                                  //10M If the datanode does not have the information about this request ,it will get a default bandwidth
  private final long AUTO_UPDATE_INTERVAL=2;
  private final long LOOP_TIME_UNIT=500;                                            //ervery half second check once
  private final HashMap<String,List<DataTransferThrottler>> ThrottlerHandlers=new HashMap<String,List<DataTransferThrottler>>();
  private DatanodeProtocolClientSideTranslatorPB bpNamenode;                        //Rpc Handler to communicate with the Namenode
  private final InetSocketAddress nnAddr;
  IOBandwidthManagerDatanodeSide(DataNode datanode,DataXceiverServer dataxceiverserver,Map<String,Map<String,InetSocketAddress>  > nnAddrs){
      this.datanode=datanode;
      this.dataxceiverserver=dataxceiverserver;
      lastupdate=monotonicNow();
      List<InetSocketAddress> AddrList=new ArrayList<InetSocketAddress>();
      //Situation with multiple namenodes still to be done
      if(nnAddrs.size()>0){
        Set<String> nameservices=nnAddrs.keySet();
        for(String nameserviceid:nameservices ){
            Map<String,InetSocketAddress>nnIdToAddr=nnAddrs.get(nameserviceid);
            for(String nnId: nnIdToAddr.keySet()){
                AddrList.add(nnIdToAddr.get(nnId));
            }
        }
        nnAddr=AddrList.get(0);
      }else
        nnAddr=null;
  }
  void SendToLocalhost(String content) throws IOException{
    Socket socket=null;
    try{

        socket = new Socket("127.0.0.1",10000);
        DataOutputStream out= new DataOutputStream(socket.getOutputStream());
        out.writeUTF(content+"\n");
        out.writeUTF("!!!!!!!!!!!!!!!!!!!!!\n\n");
        out.close();
    }catch(Exception e){
      LOG.info("Send information to localhost:10000 and failed with err:"+e);
    }
    finally{
        if(socket != null){
            try{
                socket.close();
            }catch(IOException e){
                socket=null;
            }
        }
    }
  }
 
  private void  connectToNN() throws IOException{
    if(nnAddr==null)
        throw new IOException("There is no Namenode for IOBandwidthManagerDatanodeSide to communicate with");
    bpNamenode = datanode.connectToNN(nnAddr);
    if(bpNamenode==null)
        throw new IOException("IOBandwidthManagerDatanodeSide: bpNamenode == null");
  }
  //Test function 
  private void testForRPC(){
    try{
      AppRegisterTable tmp= bpNamenode.fetchAppRegisterTable("");
      String[] tt=tmp.getTable();
      LOG.info("***********************************Test  RPC Mechanism**************************\n"); 
      LOG.info("The Number of Application :"+String.valueOf(tt.length));
      for(String cur: tt){
        
        LOG.info(cur+"\n");
      }
      LOG.info("***********************************End   RPC Mechanism**************************\n\n\n");
    }catch (IOException ie){
        LOG.warn("testForRPC in IOBandwidthManagerDatanodeSide:"+ie);
    } 
  }
  @Override
  public synchronized void run() {
    try{
        connectToNN();
    }catch(IOException ioe){
        LOG.warn("IOBandwidthManagerDatanodeSide connectToNN failed!!");
        this.closed=true;
    }
    while (datanode.shouldRun && !datanode.shutdownForUpgrade && !this.closed) {
        try {
            //LOG.info("Test for IOBandwidthManagerDatanodeSide if alive!\n");
            //testForRPC();
            long cur=monotonicNow();
            SendToLocalhost(String.valueOf(cur));
            if(updaterequired==true){
                updaterequired=false;
                updateAllBandwidth();
                updateAllThrottler();
                lastupdate=cur;
            }
            if(cur>=lastupdate+AUTO_UPDATE_INTERVAL){
                updateAllBandwidth();
                updateAllThrottler();
            }
            wait(LOOP_TIME_UNIT);
        }catch(Exception exception){
            LOG.warn(datanode.getDisplayName()+
                ":IOBandwidthManagerDatanodeSide:Runing exception",exception);
        }
    }
    // Close the IOBandwidthManagerDatanodeSide
    try {
        showtimestable.clear();
        bandwidthtable.clear();
        closed = true;
    } catch (Exception ie) {
            LOG.warn(datanode.getDisplayName()
            + " :DataXceiverServer: close exception", ie);
    }

  }
  private void updateAllThrottler()
  {
    ;
  }
  private void updateAllBandwidth(){
    ;
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

  public synchronized void  addThrottlerHandler(String clientname,DataTransferThrottler throttler)
    throws Exception{
    try{
        if(ThrottlerHandlers.containsKey(clientname)){
            List<DataTransferThrottler> throttlerlist=ThrottlerHandlers.get(clientname);
            throttlerlist.add(throttler);
        }else{
            List<DataTransferThrottler> throttlerlist=new ArrayList<DataTransferThrottler>();
            throttlerlist.add(throttler);
            ThrottlerHandlers.put(clientname,throttlerlist);
        }
        addone(clientname);
    }catch(Exception ex){
        LOG.warn(datanode.getDisplayName()+ ": IOBandwidthManagerDatanodeSide.addThrottlerHandler() ",ex);
    }
  }
  public synchronized void removeThrottlerHandler(String clientname,DataTransferThrottler throttler)
      throws Exception{
      try{
        if(ThrottlerHandlers.containsKey(clientname)){
            List<DataTransferThrottler> throttlerlist=ThrottlerHandlers.get(clientname);
            throttlerlist.remove(throttler);
            if(throttlerlist.size()==0){
                ThrottlerHandlers.remove(clientname);
            }
            subtractone(clientname);
        }else{
            LOG.warn(datanode.getDisplayName()+":IOBandwidthManagerDatanodeSide.removeThrottlerHandler :"+"Try to remove a non-existent throttler for"+clientname);
        }
      }catch(Exception ex){
        LOG.warn(datanode.getDisplayName()+ ": IOBandwidthManagerDatanodeSide.removeThrottlerHandler() ",ex);
      }
  }

  public synchronized void addIOBandwidthHandler(String clientname,long bandwidth)
      throws IOException {
    if (closed) {
      throw new IOException("IOBandwidthManagerDatanodeSide closed.");
    }
    bandwidthtable.put(clientname,bandwidth);
  }

  public synchronized void removeIOBandwidthHandler(String clientname)
      throws IOException{
    if(closed){
        throw new IOException("IOBandwidthManagerDatanodeSide closed");
    }
    bandwidthtable.remove(clientname);

  }

  public synchronized void addone(String clientname)
      throws IOException{
    if(closed){
        throw new IOException("IOBandwidthManagerDatanodeSide closed");
    }
    int value;
    if(showtimestable.containsKey(clientname)){
        value=showtimestable.get(clientname);
    }else
        value=0;
    showtimestable.put(clientname,value);
  }

  public synchronized void subtractone(String clientname)
      throws IOException{
    if(closed){
        throw new IOException("IOBandwidthManagerDatanodeSide closed");
    }
    int value;
    if(showtimestable.containsKey(clientname)){
        value=showtimestable.get(clientname);
        if(value<=1){
            showtimestable.remove(clientname);
        }else{
            showtimestable.put(clientname,value-1);
        }
    }else{
        return ;
    }
  }

  public synchronized void setUpdateRequired(){
    updaterequired=true;
  }

}
