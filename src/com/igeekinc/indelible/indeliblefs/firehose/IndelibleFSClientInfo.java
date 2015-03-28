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
package com.igeekinc.indelible.indeliblefs.firehose;

import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.igeekinc.firehose.SSLFirehoseChannel;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManagerConnection;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSClientInfoIF;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSManagerConnectionIF;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSClientInfo implements IndelibleFSClientInfoIF
{
	private SSLFirehoseChannel firehoseChannel;
	private EntityAuthentication clientEntityAuthentication;
	private HashMap<IndelibleFSServerConnectionHandle, IndelibleFSClientConnection> serverHandles = new HashMap<IndelibleFSServerConnectionHandle, IndelibleFSClientConnection>();
	private long nextConnectionHandle = 0x0100000000000000L | System.currentTimeMillis();
	public IndelibleFSClientInfo(SSLFirehoseChannel firehoseChannel) throws AuthenticationFailureException
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
	public IndelibleFSServerConnectionHandle createConnectionHandle(IndelibleFSManagerConnectionIF connection)
	{
		synchronized(serverHandles)
		{
			IndelibleFSServerConnectionHandle returnHandle = new IndelibleFSServerConnectionHandle(nextConnectionHandle);
			nextConnectionHandle ++;
			serverHandles.put(returnHandle, new IndelibleFSClientConnection(returnHandle, (IndelibleFSManagerConnection)connection));
			return returnHandle;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#getConnectionForHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public IndelibleFSClientConnection getConnectionForHandle(IndelibleFSServerConnectionHandle handle)
	{
		synchronized(serverHandles)
		{
			return serverHandles.get(handle);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#removeConnectionHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public IndelibleFSClientConnection removeConnectionHandle(IndelibleFSServerConnectionHandle handle)
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
			for (IndelibleFSClientConnection curConnection:serverHandles.values())
			{
				try
				{
					curConnection.close();
				} catch (IOException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
		}
	}
}
