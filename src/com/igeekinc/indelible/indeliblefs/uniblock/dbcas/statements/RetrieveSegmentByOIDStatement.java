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

import com.igeekinc.indelible.oid.ObjectID;

/**
 * Used for segments identified by a CASSegmentID (assigned by CASServer layer, non-versioned)
 *
 */
public class RetrieveSegmentByOIDStatement extends StatementWrapper
{
	public RetrieveSegmentByOIDStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "select * from cas,data where cas.cassegmentid=? and cas.collectionid=? and data.id=cas.dataid");
	}
	
	public ResultSet retrieveSegmentByOID(ObjectID segmentID, int internalCollectionID) throws SQLException
	{
		getStatement().setBytes(1, segmentID.getBytes());
		getStatement().setInt(2, internalCollectionID);
		return getStatement().executeQuery();
	}
}
