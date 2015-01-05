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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectIDFactory;

public class ReplicationVolumeInfo
{
    private CASCollectionID volumeID;
    private CASCollectionConnection volume;
    private Map<String, Serializable> properties;
    
    public ReplicationVolumeInfo(CASCollectionID volumeID, CASCollectionConnection volume, Map<String, Serializable> properties)
    {
        this.volumeID = volumeID;
        this.volume = volume;
        this.properties = properties;
    }
    
    public ReplicationVolumeInfo(CASCollectionID volumeID, CASCollectionConnection volume)
    {
        this.volumeID = volumeID;
        this.volume = volume;
        this.properties = new HashMap<String, Serializable>();
    }

	public CASCollectionID getVolumeID()
	{
		return volumeID;
	}

	public CASCollectionConnection getVolume()
	{
		return volume;
	}
    
    public Object getProperty(String key)
    {
    	return properties.get(key);
    }
    
    public EntityID [] getReplicationServers()
    {
    	EntityID [] returnIDs = new EntityID[0];
    	Object replicationServerObject = getProperty(ReplicationManager.kVolumeReplicationServersPropertyName);
    	if (replicationServerObject != null && replicationServerObject instanceof String)
    	{
    		String replicationServerStr = (String)replicationServerObject;
    		StringTokenizer tokenizer = new StringTokenizer(replicationServerStr, ",");
    		ArrayList<EntityID>returnIDList = new ArrayList<EntityID>();
    		while (tokenizer.hasMoreElements())
    		{
    			String curServerIDStr = tokenizer.nextToken();
    			EntityID curServerID = (EntityID) ObjectIDFactory.reconstituteFromString(curServerIDStr);
    			returnIDList.add(curServerID);
    		}
    		returnIDs = returnIDList.toArray(returnIDs);
    	}
    	return returnIDs;
    }
}
