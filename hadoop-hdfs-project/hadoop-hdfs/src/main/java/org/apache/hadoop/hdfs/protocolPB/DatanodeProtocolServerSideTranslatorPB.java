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

package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageBlockReportProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.AppRegisterTableProto;           //added for application register table 
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.AppRegisterTableRequestProto;    //added for application register table 
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.IOBandwidthQuotaRequestProto;    //added for application IOBandwidthControl
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.IOBandwidthQuotaResponseProto;   //added for application IOBandwidthControl
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.IOBandwidthQuota;   //added for application IOBandwidthControl
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StatisticInfoReportProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DfsClientIOInfo;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StatisticInfoResponseProto;


import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RollingUpgradeStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.server.protocol.AppRegisterTable;
import org.apache.hadoop.hdfs.server.protocol.DfsClientProcessInfo;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DatanodeProtocolServerSideTranslatorPB implements
    DatanodeProtocolPB {

  private final DatanodeProtocol impl;
  private final int maxDataLength;

  private static final ErrorReportResponseProto
      VOID_ERROR_REPORT_RESPONSE_PROTO = 
          ErrorReportResponseProto.newBuilder().build();
  private static final BlockReceivedAndDeletedResponseProto 
      VOID_BLOCK_RECEIVED_AND_DELETE_RESPONSE = 
          BlockReceivedAndDeletedResponseProto.newBuilder().build();
  private static final ReportBadBlocksResponseProto
      VOID_REPORT_BAD_BLOCK_RESPONSE = 
          ReportBadBlocksResponseProto.newBuilder().build();
  private static final CommitBlockSynchronizationResponseProto 
      VOID_COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO =
          CommitBlockSynchronizationResponseProto.newBuilder().build();

  public DatanodeProtocolServerSideTranslatorPB(DatanodeProtocol impl,
      int maxDataLength) {
    this.impl = impl;
    this.maxDataLength = maxDataLength;
  }

  @Override
  public RegisterDatanodeResponseProto registerDatanode(
      RpcController controller, RegisterDatanodeRequestProto request)
      throws ServiceException {
    DatanodeRegistration registration = PBHelper.convert(request
        .getRegistration());
    DatanodeRegistration registrationResp;
    try {
      registrationResp = impl.registerDatanode(registration);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RegisterDatanodeResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(registrationResp)).build();
  }
  @Override
  public StatisticInfoResponseProto sendStatisticInfo(
          RpcController controller,StatisticInfoReportProto report)
      throws ServiceException{
    String DataXceiverServerID=report.getDataxceiverserverid();
    List<DfsClientIOInfo> tmplist=report.getDfsclientsioinfoList();
    List<DfsClientProcessInfo> dfsclients=new ArrayList();
    if(tmplist!=null)
    for(DfsClientIOInfo info : tmplist){
        DfsClientProcessInfo tmp=new DfsClientProcessInfo(info.getClientname(),
                info.getIoquota(),
                info.getIospeed(),
                info.getDatasize());
        dfsclients.add(tmp);
    }
    try{
        impl.statisticReport(DataXceiverServerID,dfsclients);
    } catch(IOException e){
        throw new ServiceException(e);
    }
    StatisticInfoResponseProto.Builder builder=StatisticInfoResponseProto.newBuilder();
    return builder.build();
  }
  @Override
  public IOBandwidthQuotaResponseProto fetchIOBandwidthQuotaList(
      RpcController controller, IOBandwidthQuotaRequestProto request)
      throws ServiceException{
    String DataXceiverServerID=request.hasDataxceiverserverid() ? request.getDataxceiverserverid() : "";

    List<String> dfsclients= request.getClientnameList();
    long[] quotalist;       //=new long[1];
    try{
        quotalist=impl.ComputeQuota(DataXceiverServerID,dfsclients);
    } catch (IOException ie){
      throw new ServiceException(ie);  
    }
    IOBandwidthQuotaResponseProto.Builder builder=IOBandwidthQuotaResponseProto.newBuilder();
    Iterator<String> i1 = dfsclients.iterator();
    int pos=0;
    while(i1.hasNext() && pos<quotalist.length){
        IOBandwidthQuota.Builder tmpbuilder=IOBandwidthQuota.newBuilder(); 
        tmpbuilder.setClientname(i1.next());
        tmpbuilder.setIobandwidth(quotalist[pos]);pos++;
        builder.addQuotalist(tmpbuilder.build());
    }
    return builder.build();
  }

  @Override
  public AppRegisterTableProto fetchAppRegisterTable(
      RpcController controller, AppRegisterTableRequestProto request)
      throws ServiceException{
    
    //DatanodeRegistration registration = PBHelper.convert(request.getRegistration());
    String requestfromdatanode = request.getRequest();
    AppRegisterTable registertable;
    try{
      registertable=impl.fetchAppRegisterTable(requestfromdatanode);
    } catch (IOException e){
      throw new ServiceException(e);  
    }
    AppRegisterTableProto.Builder builder=AppRegisterTableProto.newBuilder();
    String [] table=registertable.getTable();
    for(int i=0; i<table.length ;i++){
        builder.addTable(table[i]);
    }
    return builder.build();
  }
 
  @Override
  public HeartbeatResponseProto sendHeartbeat(RpcController controller,
      HeartbeatRequestProto request) throws ServiceException {
    HeartbeatResponse response;
    try {
      final StorageReport[] report = PBHelperClient.convertStorageReports(
          request.getReportsList());
      VolumeFailureSummary volumeFailureSummary =
          request.hasVolumeFailureSummary() ? PBHelper.convertVolumeFailureSummary(
              request.getVolumeFailureSummary()) : null;
      response = impl.sendHeartbeat(PBHelper.convert(request.getRegistration()),
          report, request.getCacheCapacity(), request.getCacheUsed(),
          request.getXmitsInProgress(),
          request.getXceiverCount(), request.getFailedVolumes(),
          volumeFailureSummary, request.getRequestFullBlockReportLease());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    HeartbeatResponseProto.Builder builder = HeartbeatResponseProto
        .newBuilder();
    DatanodeCommand[] cmds = response.getCommands();
    if (cmds != null) {
      for (int i = 0; i < cmds.length; i++) {
        if (cmds[i] != null) {
          builder.addCmds(PBHelper.convert(cmds[i]));
        }
      }
    }
    builder.setHaStatus(PBHelper.convert(response.getNameNodeHaState()));
    RollingUpgradeStatus rollingUpdateStatus = response
        .getRollingUpdateStatus();
    if (rollingUpdateStatus != null) {
      // V2 is always set for newer datanodes.
      // To be compatible with older datanodes, V1 is set to null
      //  if the RU was finalized.
      RollingUpgradeStatusProto rus = PBHelperClient.
          convertRollingUpgradeStatus(rollingUpdateStatus);
      builder.setRollingUpgradeStatusV2(rus);
      if (!rollingUpdateStatus.isFinalized()) {
        builder.setRollingUpgradeStatus(rus);
      }
    }

    builder.setFullBlockReportLeaseId(response.getFullBlockReportLeaseId());
    return builder.build();
  }

  @Override
  public BlockReportResponseProto blockReport(RpcController controller,
      BlockReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    StorageBlockReport[] report = 
        new StorageBlockReport[request.getReportsCount()];
    
    int index = 0;
    for (StorageBlockReportProto s : request.getReportsList()) {
      final BlockListAsLongs blocks;
      if (s.hasNumberOfBlocks()) { // new style buffer based reports
        int num = (int)s.getNumberOfBlocks();
        Preconditions.checkState(s.getBlocksCount() == 0,
            "cannot send both blocks list and buffers");
        blocks = BlockListAsLongs.decodeBuffers(num, s.getBlocksBuffersList(),
            maxDataLength);
      } else {
        blocks = BlockListAsLongs.decodeLongs(s.getBlocksList(), maxDataLength);
      }
      report[index++] = new StorageBlockReport(PBHelperClient.convert(s.getStorage()),
          blocks);
    }
    try {
      cmd = impl.blockReport(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), report,
          request.hasContext() ?
              PBHelper.convert(request.getContext()) : null);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    BlockReportResponseProto.Builder builder = 
        BlockReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }

  @Override
  public CacheReportResponseProto cacheReport(RpcController controller,
      CacheReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    try {
      cmd = impl.cacheReport(
          PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(),
          request.getBlocksList());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    CacheReportResponseProto.Builder builder =
        CacheReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }


  @Override
  public BlockReceivedAndDeletedResponseProto blockReceivedAndDeleted(
      RpcController controller, BlockReceivedAndDeletedRequestProto request)
      throws ServiceException {
    List<StorageReceivedDeletedBlocksProto> sBlocks = request.getBlocksList();
    StorageReceivedDeletedBlocks[] info = 
        new StorageReceivedDeletedBlocks[sBlocks.size()];
    for (int i = 0; i < sBlocks.size(); i++) {
      StorageReceivedDeletedBlocksProto sBlock = sBlocks.get(i);
      List<ReceivedDeletedBlockInfoProto> list = sBlock.getBlocksList();
      ReceivedDeletedBlockInfo[] rdBlocks = 
          new ReceivedDeletedBlockInfo[list.size()];
      for (int j = 0; j < list.size(); j++) {
        rdBlocks[j] = PBHelper.convert(list.get(j));
      }
      if (sBlock.hasStorage()) {
        info[i] = new StorageReceivedDeletedBlocks(
            PBHelperClient.convert(sBlock.getStorage()), rdBlocks);
      } else {
        info[i] = new StorageReceivedDeletedBlocks(
            new DatanodeStorage(sBlock.getStorageUuid()), rdBlocks);
      }
    }
    try {
      impl.blockReceivedAndDeleted(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), info);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_BLOCK_RECEIVED_AND_DELETE_RESPONSE;
  }

  @Override
  public ErrorReportResponseProto errorReport(RpcController controller,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      impl.errorReport(PBHelper.convert(request.getRegistartion()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ERROR_REPORT_RESPONSE_PROTO;
  }

  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build();
  }

  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto request) throws ServiceException {
    List<LocatedBlockProto> lbps = request.getBlocksList();
    LocatedBlock [] blocks = new LocatedBlock [lbps.size()];
    for(int i=0; i<lbps.size(); i++) {
      blocks[i] = PBHelperClient.convert(lbps.get(i));
    }
    try {
      impl.reportBadBlocks(blocks);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REPORT_BAD_BLOCK_RESPONSE;
  }

  @Override
  public CommitBlockSynchronizationResponseProto commitBlockSynchronization(
      RpcController controller, CommitBlockSynchronizationRequestProto request)
      throws ServiceException {
    List<DatanodeIDProto> dnprotos = request.getNewTaragetsList();
    DatanodeID[] dns = new DatanodeID[dnprotos.size()];
    for (int i = 0; i < dnprotos.size(); i++) {
      dns[i] = PBHelperClient.convert(dnprotos.get(i));
    }
    final List<String> sidprotos = request.getNewTargetStoragesList();
    final String[] storageIDs = sidprotos.toArray(new String[sidprotos.size()]);
    try {
      impl.commitBlockSynchronization(PBHelperClient.convert(request.getBlock()),
          request.getNewGenStamp(), request.getNewLength(),
          request.getCloseFile(), request.getDeleteBlock(), dns, storageIDs);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO;
  }
}
