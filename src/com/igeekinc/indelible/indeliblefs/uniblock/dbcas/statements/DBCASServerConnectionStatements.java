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

import org.apache.log4j.Logger;

import com.igeekinc.util.logging.ErrorLogMessage;

/**
 * Just a container for all of the prepared statements
 *
 */
public class DBCASServerConnectionStatements
{
    private Connection dbConnection;
    private FindCollectionStatement findCollection;
    private CheckForExistenceStatement checkForExistence;
    private CheckForDataExistenceStatement checkForDataExistence;
    private RetrieveSegmentStatement retrieveSegment;
    private InsertSegmentStatement insertSegment;
    private RetrieveMetaDataStatement retrieveMetaData;
    private SetMetaDataStatement setMetaData;
    private RetrieveSegmentByOIDStatement retrieveSegmentByOID;
    private UpdateDataUsageStatement updateDataUsage;
    private RemoveSegmentStatement removeSegment;
    private GetReferencesForCASStatement getReferencesForCAS;
    private RemoveCASStatement removeCAS;
    private RetrieveSegmentIDForOIDStatement retrieveSegmentIDForOID;
    private AddLocalEventStatement addLocalEvent;
    private AddReplicatedEventStatement addReplicatedEvent;
    private GetLastLocalEventStatement getLastLocalEvent;
    private GetLocalEventsAfterStatement getLocalEventsAfter;
    private GetLocalEventsAfterTimestampStatement getLocalEventsAfterTimestamp;
    private GetReplicatedEventsAfterStatement getReplicatedEventsAfter;
    private GetLastTransactionIDStatement getLastTransactionID;
    private GetLocalEventsAfterForTransactionStatement getLocalEventsAfterForTransaction;
    private AddConnectedServerStatement addConnectedServer;
    private GetConnectedServersStatement getConnectedServers;
    private GetLastReplicatedEventStatement getLastReplicatedEvent;
    private GetLocalServerEventsAfterStatement getLocalServerEventsAfter;
    private AddLocalServerEventStatement addLocalServerEvent;
    private AddReplicatedServerEventStatement addReplicatedServerEvent;
    private InsertNewCollectionStatement insertNewCollection;
    private DeleteCollectionStatement deleteCollection;
    private GetCurrentInternalIDStatement getCurrentInternalID;
    private ListCollectionsStatement listCollections;
    private GetCollectionInternalIDStatement getCollectionInternalID;
    private InsertCASTableStatement insertCASTable;
    private ListSegmentIDsStatement listSegmentIDs;
    private GetLastLocalServerEventIDStatement getLastLocalServerEventID;
    private CreateVersionStatement createVersionStatement;
    private CommitVersionStatement commitVersionStatement;
    private GetVersionIDForVersionStatement getVersionIDForVersionStatement;
    private GetVersionInfoForVersionIDStatement getVersionInfoForVersionIDStatement;
    private RetrieveSegmentByOIDExactVersionStatement retrieveSegmentByOIDExactVersionStatement;
    private RetrieveSegmentByOIDNearestVersionStatement retrieveSegmentByOIDNearestVersionStatement;
    private DeleteFromCASStatement deleteFromCASStatement;
    private ListVersionsForSegmentStatment listVersionsForSegmentStatment;
    private GetServerNumForServerIDStatement getServerNumForServerIDStatement;
    private StoreReplicatedVersionStatement storeReplicatedVersionStatement;
    
    public DBCASServerConnectionStatements(Connection dbConnection)
    {
    	this.dbConnection = dbConnection;
    	
    	try
		{
			findCollection = new FindCollectionStatement(dbConnection);
			checkForExistence = new CheckForExistenceStatement(dbConnection);
			checkForDataExistence = new CheckForDataExistenceStatement(dbConnection);
			retrieveSegment = new RetrieveSegmentStatement(dbConnection);
			insertSegment = new InsertSegmentStatement(dbConnection);
			retrieveMetaData = new RetrieveMetaDataStatement(dbConnection);
			setMetaData = new SetMetaDataStatement(dbConnection);
			retrieveSegmentByOID = new RetrieveSegmentByOIDStatement(dbConnection);
			updateDataUsage = new UpdateDataUsageStatement(dbConnection);
			removeSegment = new RemoveSegmentStatement(dbConnection);
			getReferencesForCAS = new GetReferencesForCASStatement(dbConnection);
			removeCAS = new RemoveCASStatement(dbConnection);
			retrieveSegmentIDForOID = new RetrieveSegmentIDForOIDStatement(dbConnection);
			addLocalEvent = new AddLocalEventStatement(dbConnection);
			addReplicatedEvent = new AddReplicatedEventStatement(dbConnection);
			getLastLocalEvent = new GetLastLocalEventStatement(dbConnection);
			getLocalEventsAfter = new GetLocalEventsAfterStatement(dbConnection);
			getLocalEventsAfterTimestamp = new GetLocalEventsAfterTimestampStatement(dbConnection);
			getReplicatedEventsAfter = new GetReplicatedEventsAfterStatement(dbConnection);
			getLastTransactionID = new GetLastTransactionIDStatement(dbConnection);
			getLocalEventsAfterForTransaction = new GetLocalEventsAfterForTransactionStatement(dbConnection);
			addConnectedServer = new AddConnectedServerStatement(dbConnection);
			getConnectedServers = new GetConnectedServersStatement(dbConnection);
			getLastReplicatedEvent = new GetLastReplicatedEventStatement(dbConnection);
			getLocalServerEventsAfter = new GetLocalServerEventsAfterStatement(dbConnection);
			addLocalServerEvent = new AddLocalServerEventStatement(dbConnection);
			addReplicatedServerEvent = new AddReplicatedServerEventStatement(dbConnection);
			insertNewCollection = new InsertNewCollectionStatement(dbConnection);
			deleteCollection = new DeleteCollectionStatement(dbConnection);
			getCurrentInternalID = new GetCurrentInternalIDStatement(dbConnection);
			listCollections = new ListCollectionsStatement(dbConnection);
			getCollectionInternalID = new GetCollectionInternalIDStatement(dbConnection);
			insertCASTable = new InsertCASTableStatement(dbConnection);
			listSegmentIDs = new ListSegmentIDsStatement(dbConnection);
			getLastLocalServerEventID = new GetLastLocalServerEventIDStatement(dbConnection);
			createVersionStatement = new CreateVersionStatement(dbConnection);
			commitVersionStatement = new CommitVersionStatement(dbConnection);
			getVersionIDForVersionStatement = new GetVersionIDForVersionStatement(dbConnection);
			getVersionInfoForVersionIDStatement = new GetVersionInfoForVersionIDStatement(dbConnection);
			retrieveSegmentByOIDExactVersionStatement = new RetrieveSegmentByOIDExactVersionStatement(dbConnection);
			retrieveSegmentByOIDNearestVersionStatement = new RetrieveSegmentByOIDNearestVersionStatement(dbConnection);
			deleteFromCASStatement = new DeleteFromCASStatement(dbConnection);
			listVersionsForSegmentStatment = new ListVersionsForSegmentStatment(dbConnection);
			getServerNumForServerIDStatement = new GetServerNumForServerIDStatement(dbConnection);
			storeReplicatedVersionStatement = new StoreReplicatedVersionStatement(dbConnection);
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InternalError("Could not initialize prepared statements");
		}
    }

	public Connection getDBConnection()
    {
    	return dbConnection;
    }
    
	public FindCollectionStatement getFindCollection()
	{
		return findCollection;
	}
	
	public CheckForExistenceStatement getCheckForExistence()
	{
		return checkForExistence;
	}

	public CheckForDataExistenceStatement getCheckForDataExistence()
	{
		return checkForDataExistence;
	}

	public RetrieveSegmentStatement getRetrieveSegment()
	{
		return retrieveSegment;
	}

	public InsertSegmentStatement getInsertSegment()
	{
		return insertSegment;
	}

	public RetrieveMetaDataStatement getRetrieveMetaData()
	{
		return retrieveMetaData;
	}

	public SetMetaDataStatement getSetMetaData()
	{
		return setMetaData;
	}

	public RetrieveSegmentByOIDStatement getRetrieveSegmentByOID()
	{
		return retrieveSegmentByOID;
	}

	public UpdateDataUsageStatement getUpdateDataUsage()
	{
		return updateDataUsage;
	}

	public RemoveSegmentStatement getRemoveSegment()
	{
		return removeSegment;
	}

	public GetReferencesForCASStatement getGetReferencesForCAS()
	{
		return getReferencesForCAS;
	}

	public RemoveCASStatement getRemoveCAS()
	{
		return removeCAS;
	}

	public RetrieveSegmentIDForOIDStatement getRetrieveSegmentIDForOID()
	{
		return retrieveSegmentIDForOID;
	}
	
	public AddLocalEventStatement getAddLocalEvent()
	{
		return addLocalEvent;
	}

	public AddReplicatedEventStatement getAddReplicatedEvent()
	{
		return addReplicatedEvent;
	}

	public GetLastLocalEventStatement getGetLastLocalEvent()
	{
		return getLastLocalEvent;
	}

	public GetLocalEventsAfterStatement getGetLocalEventsAfter()
	{
		return getLocalEventsAfter;
	}

	public GetLocalEventsAfterTimestampStatement getGetLocalEventsAfterTimestamp()
	{
		return getLocalEventsAfterTimestamp;
	}

	public GetReplicatedEventsAfterStatement getGetReplicatedEventsAfter()
	{
		return getReplicatedEventsAfter;
	}

	public GetLastTransactionIDStatement getGetLastTransactionID()
	{
		return getLastTransactionID;
	}

	public GetLocalEventsAfterForTransactionStatement getGetLocalEventsAfterForTransaction()
	{
		return getLocalEventsAfterForTransaction;
	}

	public AddConnectedServerStatement getAddConnectedServer()
	{
		return addConnectedServer;
	}

	public GetConnectedServersStatement getGetConnectedServers()
	{
		return getConnectedServers;
	}

	public GetLastReplicatedEventStatement getGetLastReplicatedEvent()
	{
		return getLastReplicatedEvent;
	}

	public GetLocalServerEventsAfterStatement getGetLocalServerEventsAfter()
	{
		return getLocalServerEventsAfter;
	}
	
    public AddLocalServerEventStatement getAddLocalServerEvent()
	{
		return addLocalServerEvent;
	}

	public AddReplicatedServerEventStatement getAddReplicatedServerEvent()
	{
		return addReplicatedServerEvent;
	}

	public InsertNewCollectionStatement getInsertNewCollection()
	{
		return insertNewCollection;
	}

	public DeleteCollectionStatement getDeleteCollection()
	{
		return deleteCollection;
	}
	
	public GetCurrentInternalIDStatement getGetCurrentInternalID()
	{
		return getCurrentInternalID;
	}

	public ListCollectionsStatement getListCollections()
	{
		return listCollections;
	}

	public GetCollectionInternalIDStatement getGetCollectionInternalID()
	{
		return getCollectionInternalID;
	}

	public InsertCASTableStatement getInsertCASTable()
	{
		return insertCASTable;
	}

	public ListSegmentIDsStatement getListSegmentIDs()
	{
		return listSegmentIDs;
	}

	public GetLastLocalServerEventIDStatement getGetLastLocalServerEventID()
	{
		return getLastLocalServerEventID;
	}

	public CreateVersionStatement getCreateVersionStatement()
	{
		return createVersionStatement;
	}

	public CommitVersionStatement getCommitVersionStatement()
	{
		return commitVersionStatement;
	}

	public GetVersionIDForVersionStatement getGetVersionIDForVersionStatement()
	{
		return getVersionIDForVersionStatement;
	}

	public GetVersionInfoForVersionIDStatement getGetVersionInfoForVersionIDStatement()
	{
		return getVersionInfoForVersionIDStatement;
	}

	public RetrieveSegmentByOIDExactVersionStatement getRetrieveSegmentByOIDExactVersionStatement()
	{
		return retrieveSegmentByOIDExactVersionStatement;
	}

	public RetrieveSegmentByOIDNearestVersionStatement getRetrieveSegmentByOIDNearestVersionStatement()
	{
		return retrieveSegmentByOIDNearestVersionStatement;
	}

	public DeleteFromCASStatement getDeleteFromCASStatement()
	{
		return deleteFromCASStatement;
	}

	public ListVersionsForSegmentStatment getListVersionsForSegmentStatment()
	{
		return listVersionsForSegmentStatment;
	}

	public GetServerNumForServerIDStatement getGetServerNumForServerIDStatement()
	{
		return getServerNumForServerIDStatement;
	}

	public StoreReplicatedVersionStatement getStoreReplicatedVersionStatement()
	{
		return storeReplicatedVersionStatement;
	}
}
