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

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.util.logging.ErrorLogMessage;

public class StoreReplicatedVersionStatement extends StatementWrapper
{
	private boolean useGeneratedKeys = false;
	public StoreReplicatedVersionStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection);
		String driverName = dbConnection.getMetaData().getDriverName();
        if (driverName.startsWith("PostgreSQL"))
        {
        	setStatement(dbConnection.prepareStatement("insert into versions(versiontime, uniquifier) values(?, ?) returning versionid"));
        }
        else
        {
        	setStatement(dbConnection.prepareStatement("insert into versions(versiontime, uniquifier) values(?, ?)"));
        	useGeneratedKeys = true;
        }
	}
	
	public synchronized IndelibleVersion storeReplicatedVersion(CASServer manager, CASServerConnection connection, IndelibleVersion replicatedVersion)
    {
        ResultSet versionIDRS;
        try
        {
        	getStatement().setLong(1, replicatedVersion.getVersionTime());
        	getStatement().setInt(2, replicatedVersion.getUniquifier());
        	if (useGeneratedKeys)
        	{
        		getStatement().execute();
        		versionIDRS = getStatement().getGeneratedKeys();
        	}
        	else
        	{
        		versionIDRS = getStatement().executeQuery();
        	}
            versionIDRS.next();
            long versionID = versionIDRS.getLong(1);
            versionIDRS.close();
            return new IndelibleVersion(versionID, replicatedVersion.getVersionTime(), replicatedVersion.getUniquifier());
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Got SQLException allocating transaction");
        }
    }
}
