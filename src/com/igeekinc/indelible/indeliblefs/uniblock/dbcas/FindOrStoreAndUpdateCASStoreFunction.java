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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.oid.ObjectID;

public class FindOrStoreAndUpdateCASStoreFunction
{
	public FindOrStoreAndUpdateCASStoreFunction(Connection dbConnection) throws SQLException
	{
		createFunction(dbConnection);
	}
	
	void createFunction(Connection dbConnection) throws SQLException
	{
		String functionText = "CREATE OR REPLACE FUNCTION findOrStoreAndUpdateCAS(param_casid bytea, param_length BIGINT, param_storenum INT, param_casSegmentID bytea, param_collectionID INT, param_versionID BIGINT) RETURNS BOOLEAN\n"+
								"AS\n"+
								"$$\n"+
								"DECLARE\n"+
								"	findOrStoreResult findOrStoreSegmentType;\n"+
								"BEGIN\n"+
								"	SELECT * from findOrStoreSegment(param_casid, param_length, param_storenum) into findOrStoreResult;\n"+
								"	INSERT INTO cas (collectionid, dataid, cassegmentid, versionid) values(param_collectionID, findOrStoreResult.dataid, param_casSegmentID, param_versionID);\n"+
								"	RETURN findOrStoreResult.exists;\n"+
								"END;\n"+
								"$$\n"+
								"LANGUAGE 'plpgsql' VOLATILE\n"+
								"SECURITY DEFINER\n"+
								"COST 10;";
		Statement createFunctionStmt = dbConnection.createStatement();
		createFunctionStmt.execute(functionText);
		createFunctionStmt.close();
	}
	
	public void findOrStoreAndUpdateCAS(Connection dbConnection, CASIdentifier casIdentifier, long length, int storeNum, 
			ObjectID segmentID, int internalCollectionID, long transactionID) 
					throws SQLException, IOException
	{
		PreparedStatement functionCallStmt = dbConnection.prepareStatement("select * from findOrStoreAndUpdateCAS(?, ?, ?, ?, ?, ?)");
		functionCallStmt.setBytes(1, casIdentifier.getBytes());
		functionCallStmt.setLong(2, length);
		functionCallStmt.setInt(3, storeNum);
		functionCallStmt.setBytes(4, segmentID.getBytes());
		functionCallStmt.setInt(5, internalCollectionID);
		functionCallStmt.setLong(6, transactionID);
		functionCallStmt.executeQuery();
		functionCallStmt.close();
	}
}
