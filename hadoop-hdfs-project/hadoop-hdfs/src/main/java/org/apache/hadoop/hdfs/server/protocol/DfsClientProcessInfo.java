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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;

@InterfaceAudience.Private
@InterfaceStability.Evolving

public class DfsClientProcessInfo{
    private String clientname;
    //Average value of the IO Quota in the last feedback time slice
    private double IOQuotaAverage;
    private double IOSpeedAverage;
    //The size of data processed during the last feedback time slice
    private double DataSize;

    public DfsClientProcessInfo(String clientname,double ioquota,double iospeed,double datasize){
        if(ioquota<1e-3) ioquota=iospeed;   //we take the quota lower than 0 as no limit
        this.clientname=clientname;
        this.IOQuotaAverage=ioquota;
        this.IOSpeedAverage=iospeed;
        this.DataSize=datasize;

    }
    public synchronized void update(double quota,double iospeed,double datasize){
        double time=datasize*1.0/iospeed+DataSize/IOSpeedAverage;
        IOSpeedAverage=(datasize+DataSize)/time;
        DataSize+=datasize;
        if(quota< 1e-3){
            quota=iospeed;
        }
        //Quota maybe zero which means that infinite IO Bandwidth quota
        IOQuotaAverage=(DataSize)/((DataSize-datasize)/IOQuotaAverage+datasize/quota);
        //IOQuotaAverage=((DataSize-datasize)*IOQuotaAverage+datasize*quota)/(DataSize);

        
    }
    public String getClientname(){
        return clientname;
    }
    public double getIOQuota(){
        return IOQuotaAverage;
    }
    public double getIOSpeed(){
        return IOSpeedAverage;
    }
    public double getDataSize(){
        return DataSize;
    }
    public synchronized void setClientname(String name){
        this.clientname=name;
    }
    public synchronized void setIOQuota(double quota){
        this.IOQuotaAverage=quota;
    }
    public synchronized void setIOSpeed(double speed){
        this.IOSpeedAverage=speed;
    }
    public synchronized void setDataSize(double datasize){
        this.DataSize=datasize;
    }

}
