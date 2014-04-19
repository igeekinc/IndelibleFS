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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;

class FindOrStoreSegmentResult
{
	private long dataID;
	private boolean alreadyExists;
	
	public FindOrStoreSegmentResult(long dataID, boolean alreadyExists)
	{
		this.dataID = dataID;
		this.alreadyExists = alreadyExists;
	}
	
	public long getDataID()
	{
		return dataID;
	}
	
	public boolean alreadyExists()
	{
		return alreadyExists;
	}
}

public class FindOrStoreSegmentStoredFunction
{
	public FindOrStoreSegmentStoredFunction(Connection dbConnection) throws SQLException
	{
		createFunction(dbConnection);
	}
	
	void createFunction(Connection dbConnection) throws SQLException
	{
		if (dbConnection.createStatement().executeQuery("select * from pg_type where typname ilike 'findOrStoreSegmentType'").next())
		{
			dbConnection.createStatement().execute("drop type findOrStoreSegmentType cascade");	// Drop the type and anything that depends on it
		}
		String functionText = 
		"CREATE TYPE findOrStoreSegmentType as (dataid BIGINT, exists BOOLEAN);\n"+
		"CREATE OR REPLACE FUNCTION findOrStoreSegment(param_casid bytea, param_length BIGINT, param_storenum INT)\n"+
	    "RETURNS findOrStoreSegmentType AS\n"+
		"$$\n"+
		"DECLARE\n"+
		"dataid BIGINT;\n"+
		"exists BOOLEAN;\n"+
		"results findOrStoreSegmentType;\n"+
		"BEGIN\n"+
		//"SELECT id FROM data WHERE casid=param_casid and length=param_length INTO dataid;\n"+	// We're not using length anymore as the casid has it embedded
		"SELECT id FROM data WHERE casid=param_casid INTO dataid;\n"+
		"exists := TRUE;\n"+
		"IF NOT FOUND THEN\n"+
		//"	INSERT INTO data (casid, length, usage, storenum) VALUES(param_casid, param_length, 1, param_storenum);\n"+// We're not using length anymore as the casid has it embedded
		"	INSERT INTO data (casid, usage, storenum) VALUES(param_casid, 1, param_storenum);\n"+
		"	SELECT INTO dataid CURRVAL('data_id_seq');\n"+
		"	exists := FALSE;\n"+
		"END IF;\n"+
		"results.dataid := dataid;\n"+
		"results.exists := exists;\n"+
		"RETURN results;\n"+
		"END;\n"+ 
		"$$\n"+
		"LANGUAGE 'plpgsql' VOLATILE\n"+
		"SECURITY DEFINER\n"+
		"COST 10;";
		Statement createFunctionStmt = dbConnection.createStatement();
		createFunctionStmt.execute(functionText);
		createFunctionStmt.close();
	}
	
	public FindOrStoreSegmentResult findOrStoreSegment(Connection dbConnection, CASIdentifier casIdentifier, long length, int storeNum) throws SQLException
	{
		PreparedStatement functionCallStmt = dbConnection.prepareStatement("select * from findOrStoreSegment(?, ?, ?)");
		functionCallStmt.setBytes(1, casIdentifier.getBytes());
		functionCallStmt.setLong(2, length);
		functionCallStmt.setInt(3, storeNum);
		ResultSet results = functionCallStmt.executeQuery();
		results.next();
		FindOrStoreSegmentResult returnResult = new FindOrStoreSegmentResult(results.getLong(1), results.getBoolean(2));
		results.close();
		functionCallStmt.close();
		return returnResult;
	}
}
