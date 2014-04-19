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

import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.oid.ObjectID;

public class RetrieveSegmentByOIDNearestVersionStatement extends StatementWrapper
{
	public RetrieveSegmentByOIDNearestVersionStatement(Connection dbConnection) throws SQLException
	{
		super(dbConnection, "select data.casid, data.storenum, cas.versionid,versions.versiontime,versions.uniquifier from data,cas,versions where data.id=cas.dataid and cas.cassegmentid=? and cas.versionid=versions.versionid and versions.versiontime<=? and versions.uniquifier<=? and cas.collectionid=? order by versions.versiontime DESC, versions.uniquifier DESC limit 1");
	}
	
	public ResultSet retrieveSegmentByOIDNearestVersion(ObjectID segmentID, IndelibleVersion version, int internalCollectionID) throws SQLException
	{
		getStatement().setBytes(1, segmentID.getBytes());
		getStatement().setLong(2, version.getVersionTime());
		getStatement().setInt(3, version.getUniquifier());
		getStatement().setInt(4, internalCollectionID);
		return getStatement().executeQuery();
	}
}
