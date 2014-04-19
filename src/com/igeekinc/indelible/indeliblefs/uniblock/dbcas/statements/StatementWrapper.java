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
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StatementWrapper
{
	private PreparedStatement statement;
	private Connection dbConnection;
	
	protected StatementWrapper(Connection dbConnection)
	{
		this.dbConnection = dbConnection;
	}
	
	protected StatementWrapper(Connection dbConnection, PreparedStatement statement)
	{
		this.dbConnection = dbConnection;
		this.statement = statement;
	}
	
	protected StatementWrapper(Connection dbConnection, String statementSQL) throws SQLException
	{
		this.dbConnection = dbConnection;
		this.statement = dbConnection.prepareStatement(statementSQL);
	}
	
	protected void setStatement(PreparedStatement statement)
	{
		this.statement = statement;
	}
	
	protected void setStatement(String statementSQL) throws SQLException
	{
		statement = dbConnection.prepareStatement(statementSQL);
	}
	
	protected PreparedStatement getStatement()
	{
		return statement;
	}
	protected Connection getDbConnection()
	{
		return dbConnection;
	}
	
	public void close() throws SQLException
	{
		if (statement != null)
		{
			statement.close();
			statement = null;
		}
	}
}
