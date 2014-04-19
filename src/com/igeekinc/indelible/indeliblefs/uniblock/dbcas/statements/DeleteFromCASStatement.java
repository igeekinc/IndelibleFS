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

import com.igeekinc.indelible.oid.ObjectID;

public class DeleteFromCASStatement extends StatementWrapper
{
	public DeleteFromCASStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "delete from cas where collectionid=? and cassegmentid=? and versionid=?");
	}
	
	public void deleteFromCAS(int collectionID, ObjectID casSegmentID, long versionID) throws SQLException
	{
		getStatement().setInt(1, collectionID);
		getStatement().setBytes(2, casSegmentID.getBytes());
		getStatement().setLong(3, versionID);
		getStatement().execute();
	}
}
