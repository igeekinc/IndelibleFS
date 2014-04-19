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

public class CreateVersionStatement extends StatementWrapper
{
	private boolean useGeneratedKeys = false;
	public CreateVersionStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection);
		String driverName = dbConnection.getMetaData().getDriverName();
        if (driverName.startsWith("PostgreSQL"))
        {
        	setStatement(dbConnection.prepareStatement("insert into versions(versiontime, uniquifier) values("+Long.MAX_VALUE+", "+Integer.MAX_VALUE+") returning versionid"));
        }
        else
        {
        	setStatement(dbConnection.prepareStatement("insert into versions(versiontime, uniquifier) values("+Long.MAX_VALUE+", "+Integer.MAX_VALUE+")"));
        	useGeneratedKeys = true;
        }
	}
	
	public synchronized IndelibleVersion createVersion(CASServer manager, CASServerConnection connection)
    {
        ResultSet transactionIDRS;
        try
        {
        	if (useGeneratedKeys)
        	{
        		getStatement().execute();
        		transactionIDRS = getStatement().getGeneratedKeys();
        	}
        	else
        	{
        		transactionIDRS = getStatement().executeQuery();
        	}
            transactionIDRS.next();
            long transactionID = transactionIDRS.getLong(1);
            transactionIDRS.close();
            return new IndelibleVersion(manager, connection, transactionID);
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Got SQLException allocating transaction");
        }
    }
}
