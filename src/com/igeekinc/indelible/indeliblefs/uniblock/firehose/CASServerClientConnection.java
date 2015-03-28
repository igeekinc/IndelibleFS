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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASCollectionQueuedEventMsgPack;

public class CASServerClientConnection
{
	private CASServerConnectionHandle connectionHandle;
	private CASServerConnection localConnection;
	private long nextCollectionConnectionHandle = 0x0500000000000000L | System.currentTimeMillis();
	private long nextEventIteratorHandle = 0x0600000000000000L | System.currentTimeMillis();
	private HashMap<CASCollectionConnectionHandle, CASCollectionClientConnection> collectionConnectionHandles = new HashMap<CASCollectionConnectionHandle, CASCollectionClientConnection>();
	private HashMap<IndelibleEventIteratorHandle, IndelibleEventIterator> iteratorHandles = new HashMap<IndelibleEventIteratorHandle, IndelibleEventIterator>();
	private LinkedBlockingQueue<CASCollectionQueuedEventMsgPack>collectionEvents = new LinkedBlockingQueue<CASCollectionQueuedEventMsgPack>();
	
	public CASServerClientConnection(CASServerConnectionHandle connectionHandle, CASServerConnection localConnection)
	{
		this.connectionHandle = connectionHandle;
		this.localConnection = localConnection;
	}

	public CASServerConnectionHandle getConnectionHandle()
	{
		return connectionHandle;
	}

	public CASServerConnection getLocalConnection()
	{
		return localConnection;
	}

	public void close()
	{
		localConnection.close();
	}
	
	public CASCollectionConnectionHandle createCollectionConnectionHandle(CASCollectionConnection collectionConnection)
	{
		synchronized(collectionConnectionHandles)
		{
			CASCollectionConnectionHandle returnHandle = new CASCollectionConnectionHandle(nextCollectionConnectionHandle);
			nextCollectionConnectionHandle ++;
			CASCollectionClientConnection collectionClientConnection = new CASCollectionClientConnection(this, returnHandle, collectionConnection);
			collectionConnectionHandles.put(returnHandle, collectionClientConnection);
			return returnHandle;
		}
	}
	
	public CASCollectionClientConnection getCollectionConnectionForHandle(CASCollectionConnectionHandle handle)
	{
		synchronized(collectionConnectionHandles)
		{
			return collectionConnectionHandles.get(handle);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfoIF#removeConnectionHandle(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	public CASCollectionClientConnection removeConnectionHandle(CASCollectionConnectionHandle handle)
	{
		synchronized(collectionConnectionHandles)
		{
			return collectionConnectionHandles.remove(handle);
		}
	}
	
	public IndelibleEventIteratorHandle createIteratorHandle(IndelibleEventIterator collectionConnection)
	{
		synchronized(iteratorHandles)
		{
			IndelibleEventIteratorHandle returnHandle = new IndelibleEventIteratorHandle(nextEventIteratorHandle);
			nextEventIteratorHandle ++;
			iteratorHandles.put(returnHandle, collectionConnection);
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
	
	public void queueCollectionEvent(CASCollectionQueuedEventMsgPack collectionEvent)
	{
		collectionEvents.add(collectionEvent);
	}
	
	public CASCollectionQueuedEventMsgPack [] pollCollectionEventQueue(int maxEvents, long timeout) throws InterruptedException
	{
		ArrayList<CASCollectionQueuedEventMsgPack> returnList = new ArrayList<CASCollectionQueuedEventMsgPack>();
		if (maxEvents > 0)
		{
			CASCollectionQueuedEventMsgPack firstEvent = collectionEvents.poll(timeout, TimeUnit.MILLISECONDS);
			if (firstEvent != null)
			{
				// OK, we have at least one
				returnList.add(firstEvent);
				if (maxEvents > 1)
					collectionEvents.drainTo(returnList, maxEvents - 1);
			}
		}
		CASCollectionQueuedEventMsgPack [] returnEvents = returnList.toArray(new CASCollectionQueuedEventMsgPack[returnList.size()]);
		return returnEvents;
	}
}