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

import com.igeekinc.indelible.oid.CASCollectionID;

public class GetCollectionInternalIDStatement extends StatementWrapper
{
	public GetCollectionInternalIDStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "select internalid from collections where id=?");
	}
	
	public ResultSet getCollectionInternalID(CASCollectionID retreiveCollectionID) throws SQLException
	{
		getStatement().setBytes(1, retreiveCollectionID.getBytes());
		return getStatement().executeQuery();
	}
}
