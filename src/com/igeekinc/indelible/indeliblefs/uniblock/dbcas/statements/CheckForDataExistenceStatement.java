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
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;

public class CheckForDataExistenceStatement extends StatementWrapper
{
	public CheckForDataExistenceStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "select * from data where casid=?");
	}
	
	public synchronized ResultSet checkForDataExistence(CASIDDataDescriptor dataDescriptor) throws SQLException
	{
    	CASIdentifier casIdentifier = dataDescriptor.getCASIdentifier();
		long length = dataDescriptor.getLength();
		

		getStatement().setBytes(1, casIdentifier.getBytes());

		return getStatement().executeQuery();
	}

}
