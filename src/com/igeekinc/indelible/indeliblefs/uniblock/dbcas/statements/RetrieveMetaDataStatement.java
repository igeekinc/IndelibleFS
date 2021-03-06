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

public class RetrieveMetaDataStatement extends StatementWrapper
{
	public RetrieveMetaDataStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "select collections.id as collections_id, collections.internalid, collections.metadataid, data.id as data_id, data.casid, data.usage, data.storenum from collections, data where internalid=? and data.id=collections.metadataid");
	}
	
	public ResultSet retrieveMetaData(int internalCollectionID) throws SQLException
	{
		getStatement().setInt(1, internalCollectionID);
		return getStatement().executeQuery();
	}
}
