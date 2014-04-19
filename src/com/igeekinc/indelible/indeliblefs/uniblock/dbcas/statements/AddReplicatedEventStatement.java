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

public class AddReplicatedEventStatement extends StatementWrapper
{
	public AddReplicatedEventStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "insert into replicatedevents(servernum, eventid, collectionid, cassegmentid, eventtype, timestamp, transactionid, versionid) values(?, ?, ?, ?, ?, ?, ?, ?)");
	}
	
	public void addReplicatedEvent(int serverNum, CASCollectionEvent event, int internalCollectionID, IndelibleFSTransaction transaction) throws SQLException
	{
		getStatement().setInt(1, serverNum);
		getStatement().setLong(2, event.getEventID());
		getStatement().setInt(3, internalCollectionID);
		ObjectID eventSegmentID = null;
		if (event instanceof CASCollectionSegmentEvent)
		{
			eventSegmentID = ((CASCollectionSegmentEvent)event).getSegmentID();
			getStatement().setBytes(4, eventSegmentID.getBytes());
		} else
			getStatement().setBytes(4, new byte[0]);	// Metadata events do not have segment id's
		getStatement().setString(5, Character.toString(event.getEventType().getEventType()));
		getStatement().setLong(6, event.getTimestamp());
		getStatement().setLong(7, transaction.getTransactionID());
		IndelibleVersion version = null;
		if (event instanceof TransactionCommittedEvent)
			version = ((TransactionCommittedEvent)event).getTransaction().getVersion();
		if (version != null)
			getStatement().setLong(8, version.getVersionID());
		else
			getStatement().setLong(8, -1);
		getStatement().execute();
	}
}
