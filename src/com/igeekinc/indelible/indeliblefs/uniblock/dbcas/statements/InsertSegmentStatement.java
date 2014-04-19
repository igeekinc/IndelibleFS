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

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;

public class InsertSegmentStatement extends StatementWrapper
{
	private boolean useGeneratedKeys;
	
	public InsertSegmentStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection);
		String driverName = dbConnection.getMetaData().getDriverName();
		if (driverName.startsWith("PostgreSQL"))
        {
        	setStatement("insert into data (casid, length, usage, storenum) values(?, ?, ?, ?) returning id");
        }
        else
        {
        	setStatement("insert into data (casid, length, usage, storenum) values(?, ?, ?, ?)");
        	useGeneratedKeys = true;
        }
	}
	
	public synchronized long insertSegment(CASIDDataDescriptor dataDescriptor, int storeNum) throws SQLException
	{
		getStatement().setBytes(1, dataDescriptor.getCASIdentifier().getBytes());
		getStatement().setLong(2, dataDescriptor.getLength());
		getStatement().setInt(3, 1); // Start with usage == 1
		getStatement().setInt(4, storeNum);
		
		ResultSet dataIDSet;
		if (useGeneratedKeys)
		{
			getStatement().execute();

			dataIDSet = getStatement().getGeneratedKeys();
		}
		else
		{
			dataIDSet = getStatement().executeQuery();	// "RETURNING ID"
		}
		/*
		PreparedStatement getCurrDataIDStmt = connection.getGetCurrDataIDStmt();
		ResultSet dataIDSet = getCurrDataIDStmt.executeQuery();
		*/
		dataIDSet.next();
		long dataID = dataIDSet.getLong(1);
		dataIDSet.close();
		return dataID;
	}
}
