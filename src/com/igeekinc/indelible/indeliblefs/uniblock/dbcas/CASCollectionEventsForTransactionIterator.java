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

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.util.logging.ErrorLogMessage;

public class CASCollectionEventsForTransactionIterator extends CASCollectionEventIterator
{
	IndelibleFSTransaction transaction;
	public CASCollectionEventsForTransactionIterator(CASCollectionConnection connection, IndelibleFSTransaction transaction)
	{
		super(connection);
		this.transaction = transaction;
		this.lastEventID = -1;	// Start at the first event for this transaction
	}
	
	void loadCache()
	{
		if (!closed)
		{
			try
			{
				cachedEvents = ((DBCASServer)connection.getCASServer()).getCollectionEventsAfterEventIDForTransaction(((DBCASCollectionConnection)connection).getServerConnection(), 
						connection.getCollection().getID(), lastEventID, 1000, transaction);
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
