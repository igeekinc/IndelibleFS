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
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CollectionCreatedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CollectionDestroyedEvent;
import com.igeekinc.indelible.oid.CASCollectionID;

public class AddReplicatedServerEventStatement extends StatementWrapper
{
	public AddReplicatedServerEventStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "insert into replicatedserverevents(servernum, eventid, collectionid, eventtype, timestamp, transactionid) values(?, ?, ?, ?, ?, ?)");
	}
	
	public void addReplicatedServerEventStatement(int serverNum, CASServerEvent event, IndelibleFSTransaction transaction) throws SQLException
	{
		getStatement().setInt(1, serverNum);
		getStatement().setLong(2, event.getEventID());
		CASCollectionID collectionID = null;
		if (event instanceof CollectionCreatedEvent)
		{
			collectionID = ((CollectionCreatedEvent)event).getCollectionID();
		}
		if (event instanceof CollectionDestroyedEvent)
		{
			collectionID = ((CollectionDestroyedEvent)event).getCollectionID();
		}
		if (collectionID != null)
		{
			getStatement().setBytes(3, (collectionID).getBytes());
		}
		else
		{
			getStatement().setString(3, "");
		}
		getStatement().setString(4, Character.toString(event.getEventType().getEventType()));
		getStatement().setLong(5, event.getTimestamp());
		getStatement().setLong(6, transaction.getTransactionID());
		getStatement().execute();
	}
}
