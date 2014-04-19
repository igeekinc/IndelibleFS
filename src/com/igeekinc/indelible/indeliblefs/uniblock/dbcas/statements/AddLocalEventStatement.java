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
 
package com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements;

import java.sql.Connection;
import java.sql.SQLException;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionSegmentEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.oid.ObjectID;

public class AddLocalEventStatement extends StatementWrapper
{
	public AddLocalEventStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "insert into localevents(eventid, collectionid, cassegmentid, eventtype, timestamp, transactionid, versionid) values(?, ?, ?, ?, ?, ?, ?)");
	}
	
	public void addLocalEvent(CASCollectionEvent event, int internalCollectionID, IndelibleFSTransaction transaction) throws SQLException
	{
		getStatement().setLong(1, event.getEventID());
		getStatement().setInt(2, internalCollectionID);
		ObjectID eventSegmentID = null;
		if (event instanceof CASCollectionSegmentEvent)
		{
			eventSegmentID = ((CASCollectionSegmentEvent)event).getSegmentID();
			getStatement().setBytes(3, eventSegmentID.getBytes());
		} else
			getStatement().setBytes(3, new byte[0]);	// Metadata events do not have segment id's
		getStatement().setString(4, Character.toString(event.getEventType().getEventType()));
		getStatement().setLong(5, event.getTimestamp());
		getStatement().setLong(6, transaction.getTransactionID());
		IndelibleVersion version = null;
		if (event instanceof TransactionCommittedEvent)
		{
			version = ((TransactionCommittedEvent)event).getTransaction().getVersion();
			if (version == null)
				throw new InternalError("Transaction started without version assigned");
		}
		if (version != null)
		{
			getStatement().setLong(7, version.getVersionID());
		} 
		else
		{
			getStatement().setLong(7, -1);
		}
		getStatement().execute();
	}
}
