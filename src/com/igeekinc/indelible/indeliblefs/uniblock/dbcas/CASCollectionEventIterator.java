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
 
package com.igeekinc.indelible.indeliblefs.uniblock.dbcas;

import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;

public abstract class CASCollectionEventIterator implements
		IndelibleEventIterator
{

	protected CASCollectionConnection connection;
	protected CASCollectionEvent []	cachedEvents;
	protected int	offset;
	protected long	lastEventID;
	protected boolean	closed	= false;

	public CASCollectionEventIterator(CASCollectionConnection connection)
	{
		this.connection = connection;
		lastEventID = 0;
	}
	
	@Override
	public synchronized boolean hasNext()
	{
		if (cachedEvents == null || offset == cachedEvents.length)
		{
			loadCache();
		}
		if (cachedEvents != null && offset < cachedEvents.length)
			return true;
		return false;
	}

	@Override
	public synchronized IndelibleEvent next()
	{
		if (cachedEvents == null || offset >= cachedEvents.length - 1)
			loadCache();
		if (!closed)
		{
			if (cachedEvents != null && offset < cachedEvents.length)
			{
				lastEventID = cachedEvents[offset].getEventID() + 1;	// When we go to reload the cache, we will start with the lastEventID
				return cachedEvents[offset ++];
			}
		}
		return null;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void close()
	{
		closed = true;
		cachedEvents = null;
	}

	abstract void loadCache();
}
