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

public class RemoveCASStatement extends StatementWrapper
{
	public RemoveCASStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "delete from data where id=?");
	}
	
	public void removeCAS(long dataID) throws SQLException
	{
		getStatement().setLong(1, dataID);
		getStatement().execute();
	}
}
