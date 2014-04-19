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
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetReplicatedEventsAfterStatement extends StatementWrapper
{
	public GetReplicatedEventsAfterStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "select * from replicatedevents where servernum=? and collectionid=? and eventid >= ? order by eventid limit ?");
	}
	
	public ResultSet getReplicatedEventsAfter(int serverNum, int internalCollectionID, long startingEventID) throws SQLException
	{
		getStatement().setInt(1, serverNum);
		getStatement().setInt(2, internalCollectionID);
		getStatement().setLong(3, startingEventID);
		return getStatement().executeQuery();
	}
}
