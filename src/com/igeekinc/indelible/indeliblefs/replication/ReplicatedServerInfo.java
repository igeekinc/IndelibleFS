/*
 * Copyright 2002-2014 iGeek, Inc.
 * All Rights Reserved
 * @Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.@
 */
 
package com.igeekinc.indelible.indeliblefs.replication;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import com.igeekinc.indelible.indeliblefs.IndelibleFSServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;


class ReplicatedServerInfo
{
	private EntityID serverID;
    private IndelibleFSServer server;
    private CASServerConnectionIF connection;
    private ArrayList<CASCollectionID>replicationCollectionIDs;
    
    public ReplicatedServerInfo(IndelibleFSServer server, CASServerConnectionIF connection) throws IOException
    {
        this.server = server;
        this.connection = connection;
        serverID = server.getServerID();
        replicationCollectionIDs = new ArrayList<CASCollectionID>();
    }
    
    
    public EntityID getServerID()
	{
		return serverID;
	}


	public IndelibleFSServer getServer()
    {
        return server;
    }


    public CASServerConnectionIF getConnection()
    {
        return connection;
    }


    public synchronized void addReplicatedVolume(CASCollectionID replicatedVolumeID)
    {
        if (!replicationCollectionIDs.contains(replicatedVolumeID))
            replicationCollectionIDs.add(replicatedVolumeID);
    }
    
    public synchronized void removeReplicatedVolume(IndelibleFSObjectID removeVolumeID)
    {
        if (replicationCollectionIDs.contains(removeVolumeID))
            replicationCollectionIDs.remove(removeVolumeID);
    }
    
    public synchronized IndelibleFSObjectID [] listReplicatedVolumes()
    {
        IndelibleFSObjectID [] returnList = new IndelibleFSObjectID[replicationCollectionIDs.size()];
        returnList = replicationCollectionIDs.toArray(returnList);
        return returnList;
    }
}
