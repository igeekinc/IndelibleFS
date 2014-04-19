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

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventListener;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.proxies.IndelibleFSServerProxy;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.uniblock.CollectionCreatedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.util.logging.ErrorLogMessage;

class ServerListener implements IndelibleEventListener
{
	IndelibleFSServerProxy addedServer;
	ReplicationManager replicationManager;
	CASServerConnectionIF serverConn;
	Logger logger;
	
	public ServerListener(IndelibleFSServerProxy addedServer, ReplicationManager replicationManager, CASServerConnectionIF sourceConnection)
	{
		this.addedServer = addedServer;
		this.replicationManager = replicationManager;
		this.serverConn = sourceConnection;
		logger = Logger.getLogger(getClass());
	}
	
	@Override
	public void indelibleEvent(IndelibleEvent event)
	{
		if (event instanceof CollectionCreatedEvent)
		{
			CollectionCreatedEvent collectionCreatedEvent = (CollectionCreatedEvent)event;
			try
			{
				ReplicatedServerInfo serverInfo;
				serverInfo = replicationManager.replicatedServers.get(serverConn.getServerID());
				if (serverInfo == null)
					serverInfo = new ReplicatedServerInfo(addedServer, serverConn);
				if (replicationManager.addCollection(serverConn.getServerID(), serverConn, collectionCreatedEvent.getCollectionID(), serverInfo))
				{
					replicationManager.replicatedServers.put(serverConn.getServerID(), serverInfo);
				}
			} catch (CollectionNotFoundException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (PermissionDeniedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (AuthenticationFailureException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
	}
}