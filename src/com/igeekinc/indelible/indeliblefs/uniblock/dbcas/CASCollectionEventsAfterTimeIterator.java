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

import java.io.IOException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerInternal;
import com.igeekinc.util.logging.ErrorLogMessage;

public class CASCollectionEventsAfterTimeIterator extends CASCollectionEventIterator
{
	public CASCollectionEventsAfterTimeIterator(CASCollectionConnection connection, long timestamp)
	{
		super(connection);
		CASCollectionEvent[] events;
		try
		{
			events = ((CASServerInternal)connection.getCASServer()).getCollectionEventsAfterTimestamp(((DBCASCollectionConnection)connection).getServerConnection(), connection.getCollectionID(),
					 timestamp, 1);
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			closed = true;
			return;
		}
		if (events.length > 0)
		{
			lastEventID = events[0].getEventID();
			lastEventID--;	// Rollback one
		}
		else
		{
			// No events after timestamp apparently
			closed = true;
		}
	}
	
	void loadCache()
	{
		if (!closed)
		{
			try
			{
				cachedEvents = ((CASServerInternal)connection.getCASServer()).getCollectionEventsAfterEventID(((DBCASCollectionConnection)connection).getServerConnection(), 
						connection.getCollectionID(), lastEventID, 1000);
				offset = 0;
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				closed = true;
				return;
			}
		}
	}
}
