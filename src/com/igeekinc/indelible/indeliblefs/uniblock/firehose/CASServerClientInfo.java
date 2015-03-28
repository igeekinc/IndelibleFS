/*
 * Copyright 2002-2014 iGeek, Inc.
 * All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.igeekinc.indelible.indeliblefs.uniblock.firehose;

import java.util.HashMap;

import com.igeekinc.firehose.SSLFirehoseChannel;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.oid.EntityID;

public class CASServerClientInfo implements CASServerClientInfoIF
{
	private SSLFirehoseChannel firehoseChannel;
	private EntityAuthentication clientEntityAuthentication;
	private HashMap<CASServerConnectionHandle, CASServerClientConnection> serverHandles = new HashMap<CASServerConnectionHandle, CASServerClientConnection>();
	private long nextConnectionHandle = 0x0300000000000000L | System.currentTimeMillis();

	public CASServerClientInfo(SSLFirehoseChannel firehoseChannel) throws AuthenticationFailureException
	{
		this.firehoseChannel =  firehoseChannel;
		clientEntityAuthentication = EntityAuthenticationClient.getEntityAuthenticationClient().getClientEntityAuthenticationForFirehoseChannel(firehoseChannel);
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#getChannel()
	 */
	public SSLFirehoseChannel getChannel()
	{
		return firehoseChannel;
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#createConnectionHandle(com.igeekinc.indelible.indeliblefs.core.IndelibleFSManagerConnection)
	 */
	public CASServerConnectionHandle createConnectionHandle(CASServerConnection connection)
	{
		synchronized(serverHandles)
		{
			CASServerConnectionHandle returnHandle = new CASServerConnectionHandle(nextConnectionHandle);
			nextConnectionHandle ++;
			serverHandles.put(returnHandle, new CASServerClientConnection(returnHandle, connection));
			return returnHandle;
		}
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#getConnectionForHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public CASServerClientConnection getConnectionForHandle(CASServerConnectionHandle handle)
	{
		synchronized(serverHandles)
		{
			return serverHandles.get(handle);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#removeConnectionHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public CASServerClientConnection removeConnectionHandle(CASServerConnectionHandle handle)
	{
		synchronized(serverHandles)
		{
			return serverHandles.remove(handle);
		}
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#getClientEntityAuthentication()
	 */
	public EntityAuthentication getClientEntityAuthentication()
	{
		return clientEntityAuthentication;
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#getClientEntityID()
	 */
	public EntityID getClientEntityID()
	{
		return clientEntityAuthentication.getEntityID();
	}

	public void closeConnections()
	{
		synchronized(serverHandles)
		{
			for (CASServerClientConnection curConnection:serverHandles.values())
			{
				curConnection.close();
			}
		}
	}

}
