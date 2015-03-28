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

import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventListener;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleVersionIteratorHandle;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASCollectionQueuedEventMsgPack;

public class CASCollectionClientConnection implements IndelibleEventListener
{
	private CASServerClientConnection parent;
	private CASCollectionConnectionHandle connectionHandle;
	private CASCollectionConnection localConnection;
	private long nextEventIteratorHandle = 0x0600000000000000L | System.currentTimeMillis();
	private long nextVersionIteratorHandler = 0x0700000000000000L | System.currentTimeMillis();

	private HashMap<IndelibleEventIteratorHandle, IndelibleEventIterator> iteratorHandles = new HashMap<IndelibleEventIteratorHandle, IndelibleEventIterator>();
	private HashMap<IndelibleVersionIteratorHandle, IndelibleVersionIterator> versionIteratorHandles = new HashMap<IndelibleVersionIteratorHandle, IndelibleVersionIterator>();

	public CASCollectionClientConnection(CASServerClientConnection parent, CASCollectionConnectionHandle connectionHandle, CASCollectionConnection localConnection)
	{
		this.parent = parent;
		this.connectionHandle = connectionHandle;
		this.localConnection = localConnection;
	}

	public CASCollectionConnectionHandle getConnectionHandle()
	{
		return connectionHandle;
	}

	public CASCollectionConnection getLocalConnection()
	{
		return localConnection;
	}
	
	public void close()
	{
		//localConnection.close();	// No way to close a collection connection???
		//localConnection.removeListener(this);
	}
	
	public IndelibleEventIteratorHandle createIteratorHandle(IndelibleEventIterator eventIterator)
	{
		synchronized(iteratorHandles)
		{
			IndelibleEventIteratorHandle returnHandle = new IndelibleEventIteratorHandle(nextEventIteratorHandle);
			nextEventIteratorHandle ++;
			iteratorHandles.put(returnHandle, eventIterator);
			return returnHandle;
		}
	}
	
	public IndelibleEventIterator getEventIteratorForHandle(IndelibleEventIteratorHandle handle)
	{
		synchronized(iteratorHandles)
		{
			return iteratorHandles.get(handle);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#removeConnectionHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public IndelibleEventIterator removeEventIteratorHandle(IndelibleEventIteratorHandle handle)
	{
		synchronized(iteratorHandles)
		{
			return iteratorHandles.remove(handle);
		}
	}
	
	public IndelibleVersionIteratorHandle createIteratorHandle(IndelibleVersionIterator iterator)
	{
		synchronized(iteratorHandles)
		{
			IndelibleVersionIteratorHandle returnHandle = new IndelibleVersionIteratorHandle(nextVersionIteratorHandler);
			nextVersionIteratorHandler ++;
			versionIteratorHandles.put(returnHandle, iterator);
			return returnHandle;
		}
	}
	
	public IndelibleVersionIterator getVersionIteratorForHandle(IndelibleVersionIteratorHandle handle)
	{
		synchronized(versionIteratorHandles)
		{
			return versionIteratorHandles.get(handle);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#removeConnectionHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public IndelibleVersionIterator removeVersionIteratorHandle(IndelibleVersionIteratorHandle handle)
	{
		synchronized(versionIteratorHandles)
		{
			return versionIteratorHandles.remove(handle);
		}
	}
	
	public void listenForEvents()
	{
		localConnection.addListener(this);
	}

	@Override
	public void indelibleEvent(IndelibleEvent event)
	{
		CASCollectionEvent curEvent = (CASCollectionEvent)event;
		parent.queueCollectionEvent(new CASCollectionQueuedEventMsgPack(localConnection.getCollectionID(), curEvent));
	}
}
