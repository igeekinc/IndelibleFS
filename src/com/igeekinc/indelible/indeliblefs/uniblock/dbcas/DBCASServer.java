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
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.indelible.indeliblefs.IndelibleEntity;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionListIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEventType;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.CASSegmentIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerEventType;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerInternal;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreOperationStatus;
import com.igeekinc.indelible.indeliblefs.uniblock.CollectionCreatedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CollectionDestroyedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.MetadataModifiedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentCreatedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentReleasedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLastLocalServerEventIDStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLastReplicatedEventStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLastTransactionIDStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLocalEventsAfterForTransactionStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLocalEventsAfterStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLocalEventsAfterTimestampStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetLocalServerEventsAfterStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.GetVersionInfoForVersionIDStatement;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentExists;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.logging.DebugLogMessage;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.objectcache.LRUQueue;
import com.igeekinc.util.perf.MBPerSecondLog4jStopWatch;

class StoreAsyncCompletionHandler<A> implements AsyncCompletion<Void, CASStoreInfo>
{
	private AsyncCompletion<CASStoreInfo, A> completionHandler;
	private A completionAttachment;
	
	public StoreAsyncCompletionHandler(AsyncCompletion<CASStoreInfo, A> completionHandler, A completionAttachment)
	{
		this.completionHandler = completionHandler;
		this.completionAttachment = completionAttachment;
	}
	
	@Override
	public void completed(Void result,
			CASStoreInfo attachment)
	{
		completionHandler.completed(attachment, completionAttachment);
	}

	@Override
	public void failed(Throwable exc,
			CASStoreInfo attachment)
	{
		completionHandler.failed(exc, completionAttachment);
	}
	
}
/**
 * Hybrid SQL/file system CAS Server.  Stores MD and lookup info in a SQL database and
 * actual data in the file system
 */
public class DBCASServer extends IndelibleEntity implements CASServerInternal <DBCASServerConnection>
{
	public static final String kVerifyDuplicateSegmentsPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer.verifyDuplicateSegments";
	private static final String	kStopWatchName	= DBCASServer.class.getName();
	// This is here because CASCollectionConnection is an interface and cannot have an initializer
	static
	{
		ObjectIDFactory.addMapping(CASCollectionConnection.class, CASCollectionID.class);
		ObjectIDFactory.addMapping(CASIDDataDescriptor.class, CASSegmentID.class);	// We use CASIDDataDescriptor because we need something to map to a CASSegmentID
	}
    private GeneratorID generatorID;
    private ObjectIDFactory oidFactory;
    private String dbURL, dbUser, dbPassword;
    private EntityID securityServerID;
    
    private CASStoreManager storeManager;	// All collection share this CASStoreManager
    private CASStoreID [] stores;
    private long nextTransactionID, nextLocalServerEventID;
    private HashMap<EntityID, Integer>serverNums = new HashMap<EntityID, Integer>();
    private HashMap<Integer, EntityID>serverIDs = new HashMap<Integer, EntityID>();
    private HashMap<CASCollectionID, DBCASCollection>collections;
    private ArrayList<DBCASServerConnection>connections = new ArrayList<DBCASServerConnection>();
    private FindOrStoreSegmentStoredFunction findOrStoreSegmentFunction;
	private FindOrStoreAndUpdateCASStoreFunction	findOrStoreAndUpdateCASStoreFunction;
	private boolean useGeneratedKeys = false;
	private LRUQueue<CASIdentifier, Integer> segmentToStoreMapCache = new LRUQueue<CASIdentifier, Integer>(1024*1024);
	private boolean verifyDuplicatedStoredSegments = false;
    /**
     * 
     */
    public DBCASServer(String dbURL, String dbUser, String dbPassword, CASStoreManager storeManager)
    throws SQLException
    {
    	super(null);
    	this.dbURL = dbURL;
    	this.dbUser = dbUser;
    	this.dbPassword = dbPassword;
    	ArrayList<CASStoreID> storeList = new ArrayList<CASStoreID>();
    	collections = new HashMap<CASCollectionID, DBCASCollection>();
    	Connection dbConnection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
    	this.storeManager = storeManager;
    	try
    	{
    		dbConnection.setAutoCommit(false);
    		PreparedStatement checkStmt = dbConnection.prepareStatement("select * from connectedservers");
    		ResultSet serverIDSet = checkStmt.executeQuery();
    		while (serverIDSet.next())
    		{
    			int serverNum = serverIDSet.getInt(1);
    			EntityID curServerID = (EntityID)getObjectIDFromResultSet(serverIDSet, "serverid");
    			if (curServerID == null)
    				throw new InternalError("Server ID was corrupted");
    			if (serverNum == 1)
    			{
    				id = curServerID;
    				byte [] genIDBytes = serverIDSet.getBytes("genid");
    				generatorID = new GeneratorID(genIDBytes);
    				oidFactory = new ObjectIDFactory(generatorID);  // If not initialized here, we will do it in initDB
    			}
    			serverNums.put(curServerID, serverNum);
    			serverIDs.put(serverNum, curServerID);
    		}
    		if (id == null)
    		{
    			throw new SQLException("Generator ID not inited");
    		}

    		Statement getStores = dbConnection.createStatement();
    		ResultSet storesResults = getStores.executeQuery("select * from stores order by storenum");

    		while (storesResults.next())
    		{
    			int curStoreNum = storesResults.getInt("storenum");
    			CASStoreID curStoreID = (CASStoreID)getObjectIDFromResultSet(storesResults, "casstoreid");
    			storeList.add(curStoreNum, curStoreID);
    		}


    	}
    	catch(SQLException e)
    	{
    		dbConnection.rollback();
    		initDB(dbConnection);
    		dbConnection.commit();
    	}
    	int addStoreNum = storeList.size();
    	CASStoreID [] allStores = storeManager.listStoreIDs();
    	PreparedStatement addStoreStmt = dbConnection.prepareStatement("insert into stores(storenum, casstoreid) values(?, ?)");
    	for (CASStoreID curStoreID:allStores)
    	{
    		if (!storeList.contains(curStoreID))
    		{
    			storeList.add(addStoreNum, curStoreID);
    			addStoreStmt.setInt(1, addStoreNum);
    			addStoreStmt.setBytes(2, curStoreID.getBytes());
    			addStoreStmt.execute();
    			addStoreNum++;
    		}
    	}
    	addStoreStmt.close();
    	stores = new CASStoreID[storeList.size()];
    	stores = storeList.toArray(stores);
    	
    	GetLastTransactionIDStatement getLastTransactionID = new GetLastTransactionIDStatement(dbConnection);
    	ResultSet transactionIDResultSet = getLastTransactionID.getLastTransactionID();
    	if (transactionIDResultSet.next() && transactionIDResultSet.getLong(2) > 0)
    	{
    		nextTransactionID = transactionIDResultSet.getLong(1) + 1;
    	}
    	else
    	{
    		nextTransactionID = 0;
    	}
    	transactionIDResultSet.close();
    	getLastTransactionID.close();
    	Statement getLastLocalServerEventIDStmt = dbConnection.createStatement();
    	ResultSet lastLocalServerEventIDSet = getLastLocalServerEventIDStmt.executeQuery("select max(eventid), count(*) from localserverevents");
    	if (lastLocalServerEventIDSet.next() && lastLocalServerEventIDSet.getLong(2) > 0)
    	{
    		nextLocalServerEventID = lastLocalServerEventIDSet.getLong(1) + 1;
    	}
    	else
    	{
    		nextLocalServerEventID = 0;
    	}
    	initStoredFunctions(dbConnection);
    	dbConnection.setAutoCommit(true);
    	verifyDuplicatedStoredSegments = IndelibleServerPreferences.getProperties().getProperty(kVerifyDuplicateSegmentsPropertyName, "N").equals("Y");
    }

    private void initDB(Connection dbConnection) throws SQLException
    {
        String driverName = dbConnection.getMetaData().getDriverName();
        Statement initStmt = dbConnection.createStatement();
        String binaryType="blob";
        if (driverName.startsWith("PostgreSQL"))
        {
        	binaryType = "bytea";
            initStmt.execute("create table collections (id "+binaryType+", internalid serial, metadataid bigint)");
            initStmt.execute("create table data (id bigserial, casid "+binaryType+" primary key, usage int, storenum int)");
            // No need for casidindex because primary key always gets an index

        }
        if (driverName.equals("SQLiteJDBC"))
        {
        	binaryType="blob";
            initStmt.execute("create table collections (id "+binaryType+", internalid integer primary key, metadataid bigint)");
            initStmt.execute("create table data (id integer primary key, casid "+binaryType+", usage int, storenum int)");
            initStmt.execute("create index casidindex on data(casid)");
        }
        if (driverName.startsWith("H2"))
        {
        	binaryType="bytea";
            initStmt.execute("create table collections (id "+binaryType+", internalid serial, metadataid bigint)");
            initStmt.execute("create table data (id bigint auto_increment, casid bytea primary key, usage int, storenum int)");
            // No need for casidindex because primary key always gets an index
        }
        
        initStmt.execute("create table cas (collectionID int4, cassegmentid "+binaryType+", versionid bigint, dataid bigint)");
        
        initStmt.execute("create index dataidindex on data(id)");
        
        initStmt.execute("create index objectidindex on cas(cassegmentid)");
        initStmt.execute("create index casdataidindex on cas(dataid)");
        
        initStmt.execute("create table transactions(transactionID bigint primary key, versionid bigint)");
        initStmt.execute("create index versionindex on transactions(versionid)");
        
        
        initStmt.execute("create table stores(storenum int, casstoreid "+binaryType+")");
        initStmt.execute("create table servermd (id char(32) primary key, md "+binaryType+")");
        initStmt.execute("create table connectedservers (servernum serial, serverid "+binaryType+", genid "+binaryType+", securityserverid "+binaryType+")");
        initStmt.execute("create table localevents (eventid bigint, collectionid int4, cassegmentid "+binaryType+", eventtype char(1), timestamp bigint, transactionid bigint, versionid bigint)");
        initStmt.execute("create index leindex on localevents(eventtype, collectionid, eventid)");
        initStmt.execute("create index maxindex on localevents(collectionid, eventid)");	// TODO - this speeds up startup but there's probably a better way to cache the last event ID
        initStmt.execute("create table replicatedevents (servernum int, eventid bigint, collectionid int4, cassegmentid "+binaryType+", eventtype char(1), timestamp bigint, transactionid bigint, versionid bigint)");
        initStmt.execute("create table localserverevents (eventid bigint, collectionid "+binaryType+", eventtype char(1), timestamp bigint, transactionid bigint)");
        initStmt.execute("create table replicatedserverevents (servernum int, eventid bigint, collectionid "+binaryType+", eventtype char(1), timestamp bigint, transactionid bigint)");
        initStmt.execute("create table versions (versionid serial, versiontime bigint, uniquifier int4)");
        initStmt.execute("create index vid on versions(versionid)");
        
        GeneratorIDFactory idFactory = new GeneratorIDFactory();
        generatorID = idFactory.createGeneratorID();
        oidFactory = new ObjectIDFactory(generatorID);  // If not initialized here, we will do it in initDB
        id = (EntityID)oidFactory.getNewOID(getClass());
        PreparedStatement insertOurServerStmt = dbConnection.prepareStatement("insert into connectedservers (serverid, genid) values(?, ?)");
        insertOurServerStmt.setBytes(1, id.getBytes());
        insertOurServerStmt.setBytes(2, generatorID.getBytes());
        insertOurServerStmt.execute();
        // We do not set the security server ID here.  It needs to be set later
    }
    
	void initStoredFunctions(Connection dbConnection) throws SQLException
	{
		String driverName = dbConnection.getMetaData().getDriverName();
		if (driverName.startsWith("PostgreSQL"))
		{
			findOrStoreSegmentFunction = new FindOrStoreSegmentStoredFunction(dbConnection);
			findOrStoreAndUpdateCASStoreFunction = new FindOrStoreAndUpdateCASStoreFunction(dbConnection);
		}
	}
	protected void speedTest(DBCASServerConnection connection) throws IOException
	{
		connection.startTransaction();
		
		byte [] data = new byte[16*1024];
		for (int curByteNum = 0; curByteNum < data.length; curByteNum++)
		{
			data[curByteNum] = (byte) (Math.random() * 256);
		}
		long startTime = System.currentTimeMillis();
		for (int curInsert = 0; curInsert < 10000; curInsert++)
		{
			BitTwiddle.intToJavaByteArray(curInsert, data, 0);
			CASIDDataDescriptor storeDescriptor = new CASIDMemoryDataDescriptor(data);
			try
			{
				connection.storeSegment(storeDescriptor, -1);
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		long endTime = System.currentTimeMillis();
		long elapsed = endTime - startTime;
		logger.error("DBCASServer start up insert test - 10000 16K segments inserted in "+elapsed+" "+(10000.0/((double)elapsed/1000.0))+" records/sec");
	}
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServer#getCollection(com.igeekinc.indelible.oid.CASCollectionID)
     */
    public CASCollectionConnection openCollection(DBCASServerConnection connection, CASCollectionID id)
    throws CollectionNotFoundException
    {
        DBCASServerConnection dbCASSserverConnection = (DBCASServerConnection)connection;
        
        DBCASCollection returnCollection = getCollection(dbCASSserverConnection, id);
        DBCASCollectionConnection returnConnection = new DBCASCollectionConnection(returnCollection, dbCASSserverConnection);
        return returnConnection;
    }

	private DBCASCollection getCollection(DBCASServerConnection connection, CASCollectionID id)
			throws CollectionNotFoundException
	{
		String idStr = id.toString();
        DBCASCollection returnCollection;
        synchronized(collections)
        {
        	returnCollection = collections.get(id);
        	if (returnCollection == null)
        	{
        		try {
        			ResultSet findResults = connection.getStatements().getFindCollection().findCollection(id);
        			if (findResults.next())
        			{
        				int internalID = findResults.getInt("internalid");
        				findResults.close();
        				long nextEventID = 0L;

        				ResultSet lastEventIDResults = connection.getStatements().getGetLastLocalEvent().getLasLocalEvent(internalID);
        				if (lastEventIDResults.next())
        				{
        					nextEventID = lastEventIDResults.getLong("eventid");
        				}
        				returnCollection = new DBCASCollection(id, internalID, nextEventID + 1);
        				collections.put(id, returnCollection);
        			}
        			else
        			{ 
        				throw new CollectionNotFoundException("Collection "+idStr+" not found");
        			}
        		} 
        		catch (SQLException e) 
        		{
        			e.printStackTrace();
        			throw new CollectionNotFoundException("Caught an SQL exception retrieving");
        		}
        	}
        }
		return returnCollection;
	}
    

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServer#createNewCollection()
     */
    public CASCollectionConnection createNewCollection(DBCASServerConnection connection) throws IOException
    {
        CASCollectionID newID = (CASCollectionID)oidFactory.getNewOID(CASCollectionConnection.class);
        return addCollection(connection, newID);
    }

	public CASCollectionConnection addCollection(DBCASServerConnection connection, CASCollectionID newID) throws IOException
	{
        try
        {
        	connection.getStatements().getInsertNewCollection().insertNewCollection(newID);
            ResultSet currIDSet = connection.getStatements().getGetCollectionInternalID().getCollectionInternalID(newID);
            if (currIDSet.next())
            {
                int internalID = currIDSet.getInt(1);
                currIDSet.close();
                DBCASCollection returnCollection = new DBCASCollection(newID, internalID, 0L);
                DBCASCollectionConnection returnConnection = new DBCASCollectionConnection(returnCollection, (DBCASServerConnection)connection);
                synchronized(collections)
                {
                	collections.put(returnCollection.getID(), returnCollection);
                }
                ((CASServerConnection)connection).logEvent(getServerID(), new CollectionCreatedEvent(getServerID(), returnCollection.getID()));
                return returnConnection;
            }
            else
                throw new InternalError("Failed to retrieve new internal ID number");
        }
        catch (SQLException e)
        {
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Failed to create collection");
        }
    }


	
	@Override
	public void deleteCollection(DBCASServerConnection connection, CASCollectionID id) throws CollectionNotFoundException, IOException
	{
		DBCASCollection deleteCollection = getCollection(connection, id);
		CASCollectionConnection deleteConnection = openCollection(connection, id);
		CASSegmentIDIterator deleteIDIterator = deleteConnection.listSegments();
		while (deleteIDIterator.hasNext())
		{
			ObjectID deleteID = deleteIDIterator.next();
			deleteConnection.releaseSegment(deleteID);
		}
		// Now, delete the meta data
        try
        {
            int internalCollectionID = deleteCollection.getInternalCollectionID();
			ResultSet mdSet = connection.getStatements().getRetrieveMetaData().retrieveMetaData(internalCollectionID);
            if (mdSet.next())
            {
                
				try
				{
					CASIdentifier casID = getCASIDFromResultSet(mdSet, "casid");
	                int storeNum = mdSet.getInt("storenum");
	                long dataID = mdSet.getLong("data_id");
	                
	                deleteFromCAS(connection, storeNum, casID, dataID);
				}
				finally
				{
					mdSet.close();
				}
            }
        } catch (SQLException e)
        {
        	logger.error(new ErrorLogMessage("Caught SQLException retrieving metadata"), e);
            throw new IOException("Caught SQLException retrieving metadata");
        }
		try
		{
			connection.getStatements().getDeleteCollection().deleteCollection(id);
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Failed to create collection");
		}
	}

	/* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServer#getServerID()
     */
    public EntityID getServerID() 
    {
        return id;
    }
    
    public CASCollectionID [] listCollections(DBCASServerConnection connection)
    {
        ArrayList<CASCollectionID> collectionsArrayList = new ArrayList<CASCollectionID>();
        try
        {
            ResultSet collectionsSet = connection.getStatements().getListCollections().listCollections();
            while (collectionsSet.next())
            {
                CASCollectionID curID = (CASCollectionID) getObjectIDFromResultSet(collectionsSet, "id");
                collectionsArrayList.add(curID);
            }
            collectionsSet.close();
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
        CASCollectionID [] returnList = new CASCollectionID[collectionsArrayList.size()];
        collectionsArrayList.toArray(returnList);
        return returnList;
    }

    public synchronized DataDescriptor retrieveMetaData(DBCASServerConnection connection, String name) throws IOException
    {
        /*
        try
        {
            PreparedStatement retrieveMetaDataStmt = ((DBFSCASServerConnection)connection).getRetrieveMetaDataStmt();
            ResultSet mdSet = retrieveMetaDataStmt.executeQuery();
            if (mdSet.next())
            {
                CASIdentifier casID = new CASIdentifier(mdSet
                        .getString("casid"));
                InputStream dataStream = mdSet.getBinaryStream("data");
                DataDescriptor returnDescriptor = null;
                mdSet.close();
                return returnDescriptor;
            } else
            {
                throw new IOException("Could not find metadata");
            }
        } catch (SQLException e)
        {
            throw new IOException("Caught SQLException retrieving metadata");
        }
        */
        return null;    // TODO - fix me!
    }

    public ObjectIDFactory getOIDFactory()
    {
        return oidFactory;
    }

    public void close(DBCASServerConnection closeConnection)
    {
        try {
			closeConnection.getStatements().getDBConnection().close();
		} catch (SQLException e) {
			Logger.getLogger(getClass()).error(new ErrorLogMessage("SQLEception in close"), e);
		}
    }

    public DBCASServerConnection open(EntityID openingEntity)
    {
        if (securityServerID == null)
            throw new InternalError("SecurityServerID is not set");
        try
        {
            Connection dbForConnection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
            DBCASServerConnection returnConnection = new DBCASServerConnection(this, dbForConnection, openingEntity);
            connections.add(returnConnection);
            return returnConnection;
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Could not open database connection");
        }
    }

    public DBCASServerConnection open(EntityAuthentication openingEntity)
    {
        if (securityServerID == null)
            throw new InternalError("SecurityServerID is not set");
        try
        {
            Connection dbForConnection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
            DBCASServerConnection returnConnection = new DBCASServerConnection(this, dbForConnection, openingEntity);
            connections.add(returnConnection);
            return returnConnection;
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Could not open database connection");
        }
    }

    @Override
	public DBCASServerConnection open() throws PermissionDeniedException, IOException
	{
		throw new PermissionDeniedException();	// This is only for remote access where we already know who the opening entity is
	}

	public IndelibleFSTransaction commit(DBCASServerConnection connection) throws IOException
    {
    	boolean commitSucceeded = false;
        try
        {
        	for (int storeNum = 0; storeNum < stores.length; storeNum++)
        	{
        		try
				{
					storeManager.getCASStore(stores[storeNum]).sync();
				} catch (IOException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					throw new InternalError("Sync of casstore "+stores[storeNum]+" failed with IOException");
				}
        	}
        	DBCASServerConnection dbCASServerConnection = (DBCASServerConnection)connection;
			IndelibleFSTransaction transaction = dbCASServerConnection.getReplicatedTransaction();
			CASServerEvent [] serverEvents = new CASServerEvent[dbCASServerConnection.getServerLog().size()];
			serverEvents = dbCASServerConnection.getServerLog().toArray(serverEvents);
			dbCASServerConnection.getServerLog().clear();
        	if (transaction == null)
        	{
        		synchronized(this)
        		{
        			transaction = new IndelibleFSTransaction(id, nextTransactionID);
        			nextTransactionID++;
        			transaction.setVersion(connection.getVersionForTransaction());
        			long timestamp = System.currentTimeMillis();
            		for (CASServerEvent logEvent:serverEvents)
            		{
            			logEvent.setEventID(nextLocalServerEventID++);
            			logEvent.setTimestamp(timestamp);
            			logServerEvent(dbCASServerConnection, logEvent.getSource(), logEvent, transaction);
            		}
        		}
        		for (ObjectID eventSource:((CASServerConnection)connection).listEventSources())
        		{
        			collections.get(eventSource).logEvents(dbCASServerConnection, transaction);
        		}
        		dbCASServerConnection.getStatements().getDBConnection().commit();
        		for (CASServerEvent fireEvent:serverEvents)
        		{
        			fireServerEvent(fireEvent);
        		}
        		for (ObjectID eventSource:((CASServerConnection)connection).listEventSources())
        		{
        			collections.get(eventSource).fireTransactionEvent(dbCASServerConnection, transaction);
        		}
        		commitVersion(dbCASServerConnection, connection.getVersionForTransaction());
        		dbCASServerConnection.getStatements().getDBConnection().setAutoCommit(true);
        		commitSucceeded = true;
        		
        	}
        	else
        	{
        		TransactionCommittedEvent transactionEvent = dbCASServerConnection.getReplicatedTransactionEvent();
        		for (ObjectID eventSource:((CASServerConnection)connection).listEventSources())
        		{
        			collections.get(eventSource).logReplicatedEvents(dbCASServerConnection, transaction);
        		}
        		for (ObjectID eventSource:((CASServerConnection)connection).listEventSources())
        		{
        			collections.get(eventSource).fireReplicatedTransactionEvent(dbCASServerConnection, transaction, transactionEvent);
        		}
        		IndelibleVersion commitVersion = connection.getVersionForTransaction();
        		if (commitVersion != null)
        			commitVersion(dbCASServerConnection, commitVersion);
        		dbCASServerConnection.getStatements().getDBConnection().commit();
        		dbCASServerConnection.getStatements().getDBConnection().setAutoCommit(true);
        		commitSucceeded = true;
        	}
            return transaction;
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Transaction commit failed with SQL exception");
        }
        finally
        {
        	if (!commitSucceeded)
        	{
        		rollback(connection);
        	}
        }
    }

    public void rollback(DBCASServerConnection connection)
    {
        try
        {
            ((DBCASServerConnection)connection).getStatements().getDBConnection().rollback();
            ((DBCASServerConnection)connection).getStatements().getDBConnection().setAutoCommit(true);
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
    }

    @Override
    public IndelibleVersion startTransaction(DBCASServerConnection connection) throws IOException
    {
        try
        {
        	synchronized(connection)
        	{
        		((DBCASServerConnection)connection).getStatements().getDBConnection().setAutoCommit(false);
        		return connection.getStatements().getCreateVersionStatement().createVersion(this, connection);
        	}
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new IOException("Could not create new transaction");
        }
    }

    
    @Override
	public IndelibleVersion startReplicatedTransaction(DBCASServerConnection connection, TransactionCommittedEvent transactionEvent) throws IOException
	{
    	try
    	{
    		synchronized(connection)
    		{
    			((DBCASServerConnection)connection).setReplicatedTransactionEvent(transactionEvent);
    			((DBCASServerConnection)connection).getStatements().getDBConnection().setAutoCommit(false);
    			return connection.getStatements().getStoreReplicatedVersionStatement().storeReplicatedVersion(this, connection, transactionEvent.getTransaction().getVersion());
    		}
    	} catch (SQLException e)
    	{
    		Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    		throw new IOException("Could not create new transaction");
    	}
	}

	public void storeMetaData(DBCASServerConnection connection, String name,
            DataDescriptor metaData) throws IOException
    {
        // TODO Auto-generated method stub
        
    }
    
    public void setSecurityServerID(EntityID securityServerID)
    {
        try
        {
            Connection dbConnection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
            Statement updateStatement = dbConnection.createStatement();
            updateStatement.execute("update connectedservers set securityserverid='"+securityServerID.toString()+"' where serverid='"+id.toString()+"'");
        } catch(SQLException e)
        {
        	logger.error(new ErrorLogMessage("Got SQL Exception setting security server id"), e);
            throw new InternalError("Could not update securityserverid");
        }
        this.securityServerID = securityServerID;
    }
    public EntityID getSecurityServerID()
    {
        return securityServerID;
    }

	public CASStore getStoreAtIndex(int storeNum)
	{
		return storeManager.getCASStore(stores[storeNum]);
	}

	public int getNumStores()
	{
		return stores.length;
	}
	
	public int getIndexOfStore(CASStoreID storeID)
	{
		for (int checkStoreNum = 0; checkStoreNum < stores.length; checkStoreNum++)
		{
			if (stores[checkStoreNum].equals(storeID))
				return checkStoreNum;
		}
		return -1;
	}
	public CASStore getCASStoreForWriting(long bytesToWrite) throws IOException
	{
		return storeManager.getCASStoreForWriting(bytesToWrite);
	}

	public CASIDDataDescriptor retrieveSegment(DBCASServerConnection connection, CASIdentifier segmentID, int internalCollectionID) throws IOException, SegmentNotFound
	{
		synchronized(connection)
		{
			Integer casstore = segmentToStoreMapCache.get(segmentID);
			if (casstore == null)
			{
				try
				{
					ResultSet retrieveResults = connection.getStatements().getRetrieveSegment().retrieveSegment(segmentID, internalCollectionID);
					if (retrieveResults.next())
					{
						casstore = retrieveResults.getInt("storenum");
					}
					retrieveResults.close();
				}
				catch (SQLException e)
				{
					e.printStackTrace();
					throw new IOException("SQL Exception while retrieving");
				}
			}
			CASIDDataDescriptor returnDescriptor = null;
			if (casstore != null)
			{
				CASStore retrieveStore = connection.getStoreAtIndex(casstore);
				returnDescriptor = retrieveStore.retrieveSegment(segmentID);
			}
			return returnDescriptor;
		}
	}
	
	public Future<CASIDDataDescriptor> retrieveSegmentAsync(DBCASServerConnection connection, CASIdentifier segmentID, int internalCollectionID) throws IOException, SegmentNotFound
	{
		/*
		synchronized(connection)
		{
			Integer casstore = segmentToStoreMapCache.get(segmentID);
			if (casstore == null)
			{
				try
				{
					ResultSet retrieveResults = connection.getStatements().getRetrieveSegment().retrieveSegment(segmentID, internalCollectionID);
					if (retrieveResults.next())
					{
						casstore = retrieveResults.getInt("storenum");
					}
					retrieveResults.close();
				}
				catch (SQLException e)
				{
					e.printStackTrace();
					throw new IOException("SQL Exception while retrieving");
				}
			}
			Future<CASIDDataDescriptor> returnFuture = null;
			if (casstore != null)
			{
				CASStore retrieveStore = connection.getStoreAtIndex(casstore);
				returnFuture = retrieveStore.retrieveSegmentAsync(segmentID);
			}
			return returnFuture;
		}
		*/
		ComboFutureBase<CASIDDataDescriptor>returnFuture = new ComboFutureBase<CASIDDataDescriptor>();
		retrieveSegmentAsync(connection, segmentID, internalCollectionID, returnFuture);
		return returnFuture;
	}
	
	public <A> void retrieveSegmentAsync(DBCASServerConnection connection, CASIdentifier segmentID, int internalCollectionID, AsyncCompletion<CASIDDataDescriptor, A>completionHandler,
			A attachment) throws IOException, SegmentNotFound
	{
		/*
		synchronized(connection)
		{
			Integer casstore = segmentToStoreMapCache.get(segmentID);
			if (casstore == null)
			{
				try
				{
					ResultSet retrieveResults = connection.getStatements().getRetrieveSegment().retrieveSegment(segmentID, internalCollectionID);
					if (retrieveResults.next())
					{
						casstore = retrieveResults.getInt("storenum");
					}
					retrieveResults.close();
				}
				catch (SQLException e)
				{
					e.printStackTrace();
					throw new IOException("SQL Exception while retrieving");
				}
			}
			Future<CASIDDataDescriptor> returnFuture = null;
			if (casstore != null)
			{
				CASStore retrieveStore = connection.getStoreAtIndex(casstore);
				retrieveStore.retrieveSegmentAsync(segmentID, completionHandler, attachment);
			}
		}
		*/
		ComboFutureBase<CASIDDataDescriptor>future = new ComboFutureBase<CASIDDataDescriptor>(completionHandler, attachment);
		retrieveSegmentAsync(connection, segmentID, internalCollectionID, future);
	}
	
	private void retrieveSegmentAsync(DBCASServerConnection connection, CASIdentifier segmentID, int internalCollectionID, ComboFutureBase<CASIDDataDescriptor>future) throws IOException, SegmentNotFound
	{
		synchronized(connection)
		{
			Integer casstore = segmentToStoreMapCache.get(segmentID);
			if (casstore == null)
			{
				try
				{
					ResultSet retrieveResults = connection.getStatements().getRetrieveSegment().retrieveSegment(segmentID, internalCollectionID);
					if (retrieveResults.next())
					{
						casstore = retrieveResults.getInt("storenum");
					}
					retrieveResults.close();
				}
				catch (SQLException e)
				{
					e.printStackTrace();
					throw new IOException("SQL Exception while retrieving");
				}
			}
			if (casstore != null)
			{
				CASStore retrieveStore = connection.getStoreAtIndex(casstore);
				retrieveStore.retrieveSegmentAsync(segmentID, future, null);
			}
		}
	}
	
	public DataVersionInfo retrieveSegment(DBCASServerConnection connection, ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		synchronized(connection)
		{
			try
			{
				ResultSet retrieveSet;
				if (flags == RetrieveVersionFlags.kExact)
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(segmentID, version, internalCollectionID);
				else
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDNearestVersionStatement().retrieveSegmentByOIDNearestVersion(segmentID, version, internalCollectionID);
				if (retrieveSet.next())
				{
					CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
					int storeNum = retrieveSet.getInt("storenum");
					CASStore casStore = connection.getStoreAtIndex(storeNum);
					CASIDDataDescriptor returnDescriptor;
					try
					{
						returnDescriptor = casStore.retrieveSegment(casID);
					} catch (SegmentNotFound e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						// TODO - this would be a good place to try to retrieve the segment from a backup server
						throw new IOException("Could not find segment "+segmentID);
					}
					long versionID = retrieveSet.getLong("versionid");
					long versionTime = retrieveSet.getLong("versiontime");
					int uniquifier = retrieveSet.getInt("uniquifier");
					retrieveSet.close();
					IndelibleVersion returnVersion = new IndelibleVersion(versionID, versionTime, uniquifier);
					DataVersionInfo returnInfo = new DataVersionInfo(returnDescriptor, returnVersion);
					return returnInfo;
				}
				else
				{
					retrieveSet.close();
					throw new IOException("Could not find segment "+segmentID);
				}
			} catch (SQLException e)
			{
				logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", segmentID), e);
				throw new IOException("Caught SQLException retrieving segment "+segmentID);
			}
		}
	}
	
	class RetrieveSegmentAsyncCompletionHandler implements AsyncCompletion<CASIDDataDescriptor, Void>
	{
		long versionID, versionTime;
		int uniquifier;
		ComboFutureBase<DataVersionInfo>future;
		
		public RetrieveSegmentAsyncCompletionHandler(long versionID, long versionTime, int uniquifier, ComboFutureBase<DataVersionInfo>future)
		{
			this.versionID = versionID;
			this.versionTime = versionTime;
			this.uniquifier = uniquifier;
			this.future = future;
		}

		@Override
		public synchronized void completed(CASIDDataDescriptor result,
				Void attachment)
		{
			IndelibleVersion returnVersion = new IndelibleVersion(versionID, versionTime, uniquifier);
			DataVersionInfo returnInfo = new DataVersionInfo(result, returnVersion);
			future.completed(returnInfo, null);
		}

		@Override
		public void failed(Throwable exc, Void attachment)
		{
			future.failed(exc, attachment);
		}
	}
	
	public Future<DataVersionInfo>retrieveSegmentAsync(DBCASServerConnection connection, ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		ComboFutureBase<DataVersionInfo>future = new ComboFutureBase<DataVersionInfo>();
		retrieveSegmentAsync(connection, segmentID, version, flags, internalCollectionID, future);
		return future;
	}
	
	public <A> void retrieveSegmentAsync(DBCASServerConnection connection, ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, 
			int internalCollectionID, AsyncCompletion<DataVersionInfo, A>completion, A attachment) throws IOException
	{
		ComboFutureBase<DataVersionInfo>future = new ComboFutureBase<DataVersionInfo>(completion, attachment);
		retrieveSegmentAsync(connection, segmentID, version, flags, internalCollectionID, future);
	}
	
	private void retrieveSegmentAsync(DBCASServerConnection connection, ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID,
			ComboFutureBase<DataVersionInfo>future) throws IOException
	{
		synchronized(connection)
		{
			try
			{
				ResultSet retrieveSet;
				if (flags == RetrieveVersionFlags.kExact)
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(segmentID, version, internalCollectionID);
				else
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDNearestVersionStatement().retrieveSegmentByOIDNearestVersion(segmentID, version, internalCollectionID);
				if (retrieveSet.next())
				{
					CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
					int storeNum = retrieveSet.getInt("storenum");
					long versionID = retrieveSet.getLong("versionid");
					long versionTime = retrieveSet.getLong("versiontime");
					int uniquifier = retrieveSet.getInt("uniquifier");
					retrieveSet.close();
					CASStore casStore = connection.getStoreAtIndex(storeNum);
					RetrieveSegmentAsyncCompletionHandler completionHandler = new RetrieveSegmentAsyncCompletionHandler(versionID, versionTime, 
							uniquifier, future);
					try
					{
						casStore.retrieveSegmentAsync(casID, completionHandler, null);
					} catch (SegmentNotFound e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						// TODO - this would be a good place to try to retrieve the segment from a backup server
						throw new IOException("Could not find segment "+segmentID);
					}
				}
				else
				{
					retrieveSet.close();
					throw new IOException("Could not find segment "+segmentID);
				}
			} catch (SQLException e)
			{
				logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", segmentID), e);
				throw new IOException("Caught SQLException retrieving segment "+segmentID);
			}
		}
	}
	public SegmentInfo retrieveSegmentInfo(DBCASServerConnection connection, ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		synchronized(connection)
		{
			try
			{
				ResultSet retrieveSet;
				if (flags == RetrieveVersionFlags.kExact)
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(segmentID, version, internalCollectionID);
				else
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDNearestVersionStatement().retrieveSegmentByOIDNearestVersion(segmentID, version, internalCollectionID);
				if (retrieveSet.next())
				{
					CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
					long versionID = retrieveSet.getLong("versionid");
					long versionTime = retrieveSet.getLong("versiontime");
					int uniquifier = retrieveSet.getInt("uniquifier");
					retrieveSet.close();
					IndelibleVersion returnVersion = new IndelibleVersion(versionID, versionTime, uniquifier);
					SegmentInfo returnInfo = new SegmentInfo(casID, returnVersion);
					return returnInfo;
				}
				else
				{
					retrieveSet.close();
					throw new IOException("Could not find segment "+segmentID);
				}
			} catch (SQLException e)
			{
				logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", segmentID), e);
				throw new IOException("Caught SQLException retrieving segment "+segmentID);
			}
		}
	}
	
	public boolean verifySegment(DBCASServerConnection connection, ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		synchronized(connection)
		{
			try
			{
				ResultSet retrieveSet;
				if (flags == RetrieveVersionFlags.kExact)
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(segmentID, version, internalCollectionID);
				else
					retrieveSet = connection.getStatements().getRetrieveSegmentByOIDNearestVersionStatement().retrieveSegmentByOIDNearestVersion(segmentID, version, internalCollectionID);
				if (retrieveSet.next())
				{
					CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
					int storeNum = retrieveSet.getInt("storenum");
					CASStore casStore = connection.getStoreAtIndex(storeNum);
					long versionID = retrieveSet.getLong("versionid");
					long versionTime = retrieveSet.getLong("versiontime");
					int uniquifier = retrieveSet.getInt("uniquifier");
					retrieveSet.close();
					IndelibleVersion returnVersion = new IndelibleVersion(versionID, versionTime, uniquifier);
					CASIDDataDescriptor returnDescriptor;
					try
					{
						returnDescriptor = casStore.retrieveSegment(casID);
						ByteBuffer checkBuffer = returnDescriptor.getByteBuffer();
						SHA1HashID checkID = new SHA1HashID(checkBuffer);
						CASIdentifier checkIdentifier = new CASIdentifier(checkID, checkBuffer.capacity());
						return casID.equals(checkIdentifier);
					} catch (SegmentNotFound e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						// TODO - this would be a good place to try to retrieve the segment from a backup server
						throw new IOException("Could not find segment "+segmentID);
					}
				}
				else
				{
					retrieveSet.close();
					throw new IOException("Could not find segment "+segmentID);
				}
			} catch (SQLException e)
			{
				logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", segmentID), e);
				throw new IOException("Caught SQLException retrieving segment "+segmentID);
			}
		}
	}
	

	public void repairSegment(DBCASServerConnection connection,
			ObjectID repairSegmentID, IndelibleVersion version,
			DataVersionInfo masterData, int internalCollectionID) throws IOException
	{
		synchronized(connection)
		{
			try
			{
				ResultSet retrieveSet;
				retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(repairSegmentID, version, internalCollectionID);
				if (retrieveSet.next())
				{
					CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
					int storeNum = retrieveSet.getInt("storenum");
					CASStore casStore = connection.getStoreAtIndex(storeNum);
					long versionID = retrieveSet.getLong("versionid");
					long versionTime = retrieveSet.getLong("versiontime");
					int uniquifier = retrieveSet.getInt("uniquifier");
					retrieveSet.close();
					IndelibleVersion returnVersion = new IndelibleVersion(versionID, versionTime, uniquifier);
					CASIDDataDescriptor returnDescriptor;
					// Our idea of what should be in here matches what we're receiving, so we can just whack it into the CASStore
					if (casID.equals(masterData.getDataDescriptor().getCASIdentifier()))
					{
						CASIdentifier checkIdentifier = null;
						try
						{
							returnDescriptor = casStore.retrieveSegment(casID);
							ByteBuffer checkBuffer = returnDescriptor.getByteBuffer();
							SHA1HashID checkID = new SHA1HashID(checkBuffer);
							checkIdentifier = new CASIdentifier(checkID, checkBuffer.capacity());
							// Data doesn't compare.  
							
						} catch (SegmentNotFound e)
						{
							logger.error(new ErrorLogMessage("Could not find segment {0} under repair, will replace", repairSegmentID));
						}
						if (!casID.equals(checkIdentifier))
						{
							casStore.repairSegment(casID, masterData.getDataDescriptor());
						}
					}
				}
				else
				{
					retrieveSet.close();
					throw new IOException("Could not find segment "+repairSegmentID);
				}
			} catch (SQLException e)
			{
				logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", repairSegmentID), e);
				throw new IOException("Caught SQLException retrieving segment "+repairSegmentID);
			}
		}
	}
    public void storeSegment(DBCASServerConnection connection, ObjectID segmentID, IndelibleVersion version, CASIDDataDescriptor segmentDescriptor, int internalCollectionID) throws IOException, SegmentExists
    {
    	synchronized(connection)
    	{
    		try
    		{
    			ResultSet retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(segmentID, version, internalCollectionID);
    			long dataID = -1L;
    			if (retrieveSet.next())
    			{
    				if (version.isFinal())
    				{
    					throw new SegmentExists(segmentID);
    				}
    				else
    				{
    	            	dataID = retrieveSet.getLong("id");
                        CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
    	            	if (!segmentDescriptor.getCASIdentifier().equals(casID))
    	            	{
    	            		connection.getStatements().getDeleteFromCASStatement().deleteFromCAS(internalCollectionID, segmentID, version.getVersionID());
    	            		ResultSet referencesResults = connection.getStatements().getGetReferencesForCAS().getReferencesForCAS(dataID);
    	            		if (referencesResults.next())
    	            		{
    	            			int numReferences = referencesResults.getInt(1);
    	            			if (numReferences == 0)
    	            			{
    	            				connection.getStatements().getRemoveCAS().removeCAS(dataID);
    	            				int storeNum = retrieveSet.getInt("storenum");
    	            				connection.getStoreAtIndex(storeNum).deleteSegment(casID);
    	            			}
    	            		}
    	            		dataID = -1L;	// Segment needs to be inserted
    	            	}
    				}
    			}
    			retrieveSet.close();
    			if (dataID < 0)
    			{
    				dataID = findOrStoreDataDescriptor(connection, segmentDescriptor);
    				// Update the CAS table
    				insertIntoCASTable(connection, version.getVersionID(), dataID, segmentID, internalCollectionID);
    			}
    		} catch (SQLException e)
    		{
    			logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", segmentID), e);
    			throw new IOException("Caught SQLException retrieving segment "+segmentID);
    		}
    	}
        
    }
    
    public <A> void storeSegmentAsync(DBCASServerConnection connection, ObjectID segmentID, IndelibleVersion version, 
    		CASIDDataDescriptor segmentDescriptor, int internalCollectionID, AsyncCompletion<Void, A>completionHandler,
			A attachment) throws IOException
    {
    	synchronized(connection)
    	{
    		try
    		{
    			ResultSet retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(segmentID, version, internalCollectionID);
    			long dataID = -1L;
    			if (retrieveSet.next())
    			{
    				if (version.isFinal())
    				{
    					throw new IOException(segmentID+" already exists in collection");
    				}
    				else
    				{
    	            	dataID = retrieveSet.getLong("id");
                        CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
    	            	if (!segmentDescriptor.getCASIdentifier().equals(casID))
    	            	{
    	            		connection.getStatements().getDeleteFromCASStatement().deleteFromCAS(internalCollectionID, segmentID, version.getVersionID());
    	            		ResultSet referencesResults = connection.getStatements().getGetReferencesForCAS().getReferencesForCAS(dataID);
    	            		if (referencesResults.next())
    	            		{
    	            			int numReferences = referencesResults.getInt(1);
    	            			if (numReferences == 0)
    	            			{
    	            				connection.getStatements().getRemoveCAS().removeCAS(dataID);
    	            				int storeNum = retrieveSet.getInt("storenum");
    	            				connection.getStoreAtIndex(storeNum).deleteSegment(casID);
    	            			}
    	            		}
    	            		dataID = -1L;	// Segment needs to be inserted
    	            	}
    				}
    			}
    			retrieveSet.close();
    			if (dataID < 0)
    			{
    				dataID = findOrStoreDataDescriptorAsync(connection, segmentDescriptor, completionHandler, attachment);
    				// Update the CAS table
    				insertIntoCASTable(connection, version.getVersionID(), dataID, segmentID, internalCollectionID);
    			}
    			else
    			{
    				completionHandler.completed(null, attachment);
    			}
    		} catch (SQLException e)
    		{
    			logger.error(new ErrorLogMessage("Caught SQLException retrieving segment {0}", segmentID), e);
    			throw new IOException("Caught SQLException retrieving segment "+segmentID);
    		}
    	}
        
    }
    
	public CASStoreInfo storeSegment(DBCASServerConnection connection, CASIDDataDescriptor segmentDescriptor, int internalCollectionID) throws IOException
	{
		MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".storeSegment");
		IndelibleVersion version = connection.getVersionForTransaction();
		long segmentLength = segmentDescriptor.getLength();
		try
		{
			long dataID = -1;
			// First, check to see if this exists in the database
			synchronized(connection)
			{
				Log4JStopWatch checkForExistenceWatch = new Log4JStopWatch(kStopWatchName+".storeSegment.checkForExistence");
				ResultSet checkResults = connection.getStatements().getCheckForExistence().checkForExistence(segmentDescriptor);
				checkForExistenceWatch.stop();


				if (checkResults.next())
				{
					int storeNum = checkResults.getInt("storenum");
					CASStore checkStore = connection.getStoreAtIndex(storeNum);
					if (checkStore == null)
					{
						// OK, something is really broken
						// TODO - take the CASStore offline and trigger a rebuild
						throw new InternalError("Cannot find storenum "+storeNum);
					}

					if (verifyDuplicatedStoredSegments)
					{
						if (!checkStore.verifySegment(segmentDescriptor.getCASIdentifier()))
						{
							logger.error("Verify failed for segment "+segmentDescriptor.getCASIdentifier()+" - reinserting");
							checkStore.storeSegment(segmentDescriptor);
						}
					}
					dataID = checkResults.getInt("dataid");
					do
					{
						if (checkResults.getInt("collectionid") == internalCollectionID)
						{
							// If it's already entered as a CASSegmentID we can share the ID

							ObjectID checkID = getObjectIDFromResultSet(checkResults, "cassegmentid");
							if (checkID instanceof CASSegmentID)
							{
								CASSegmentID returnID = (CASSegmentID)checkID;
								// update data set usage=? where id=?

								int usage = checkResults.getInt("usage");
								logger.debug(new DebugLogMessage("Updating data usage for "+dataID+" usage = "+usage));
								connection.getStatements().getUpdateDataUsage().updateDataUsage(dataID, usage);
								checkResults.close();
								segmentDescriptor.close();
								return new CASStoreInfo(CASStoreOperationStatus.kSegmentExists, returnID);    // It's already entered for us
							}
						}
					}
					while(checkResults.next());
					checkResults.close();
					CASSegmentID returnID = (CASSegmentID) oidFactory.getNewOID(CASIDDataDescriptor.class);

					// Update the CAS table
					insertIntoCASTable(connection, version.getVersionID(), dataID, returnID, internalCollectionID);
					return new CASStoreInfo(CASStoreOperationStatus.kSegmentCreated, returnID);
				}
				checkResults.close();
			}
			CASSegmentID returnID = (CASSegmentID) oidFactory.getNewOID(CASIDDataDescriptor.class);
			//dataID = storeDataSegment(connection, segmentDescriptor, casIDStr);      
			findOrStoreAndUpdateCAS(connection, segmentDescriptor, version.getVersionID(), returnID, internalCollectionID);
			return new CASStoreInfo(CASStoreOperationStatus.kSegmentCreated, returnID);

		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Got SQL Exception inserting segment {0}", segmentDescriptor.getCASIdentifier()), e);
			throw new IOException("Could not insert segment "+segmentDescriptor.getCASIdentifier().toString(), e);
		}
		finally
		{
			wholeWatch.bytesProcessed(segmentLength);
			wholeWatch.stop();
		}
	}
	
	public <A> void storeSegmentAsync(DBCASServerConnection connection, CASIDDataDescriptor segmentDescriptor, int internalCollectionID, AsyncCompletion<CASStoreInfo, A>completionHandler,
			A attachment) throws IOException
    {

    	synchronized(connection)
    	{
    		IndelibleVersion version = connection.getVersionForTransaction();
    		try
    		{
    			long dataID = -1;
    			// First, check to see if this exists in the database
    			Log4JStopWatch checkForExistenceWatch = new Log4JStopWatch(kStopWatchName+".storeSegment.checkForExistence");
    			ResultSet checkResults = connection.getStatements().getCheckForExistence().checkForExistence(segmentDescriptor);
    			checkForExistenceWatch.stop();
    			
    			if (checkResults.next())
    			{
    				int storeNum = checkResults.getInt("storenum");
    				CASStore checkStore = connection.getStoreAtIndex(storeNum);
    				if (checkStore == null)
    				{
    					// OK, something is really broken
    					// TODO - take the CASStore offline and trigger a rebuild
    					throw new InternalError("Cannot find storenum "+storeNum);
    				}

    				if (verifyDuplicatedStoredSegments)
    				{
    					if (!checkStore.verifySegment(segmentDescriptor.getCASIdentifier()))
    					{
    						logger.error("Verify failed for segment "+segmentDescriptor.getCASIdentifier()+" - reinserting");
    						checkStore.storeSegment(segmentDescriptor);
    					}
    				}
    				dataID = checkResults.getInt("dataid");
    				do
    				{
    					if (checkResults.getInt("collectionid") == internalCollectionID)
    					{
    						// If it's already entered as a CASSegmentID we can share the ID

    						ObjectID checkID = getObjectIDFromResultSet(checkResults, "cassegmentid");
    						if (checkID instanceof CASSegmentID)
    						{
    							CASSegmentID returnID = (CASSegmentID)checkID;
    							// update data set usage=? where id=?

    							int usage = checkResults.getInt("usage");

    							connection.getStatements().getUpdateDataUsage().updateDataUsage(dataID, usage);
    							checkResults.close();
    							segmentDescriptor.close();
    							CASStoreInfo returnInfo = new CASStoreInfo(CASStoreOperationStatus.kSegmentExists, returnID);    // It's already entered for us
    							completionHandler.completed(returnInfo, attachment);
    							return;
    						}
    					}
    				}
    				while(checkResults.next());
    				checkResults.close();
    				CASSegmentID returnID = (CASSegmentID) oidFactory.getNewOID(CASIDDataDescriptor.class);

    				// Update the CAS table
    				insertIntoCASTable(connection, version.getVersionID(), dataID, returnID, internalCollectionID);
    				CASStoreInfo returnInfo = new CASStoreInfo(CASStoreOperationStatus.kSegmentExists, returnID);    // It's already entered for us
					completionHandler.completed(returnInfo, attachment);
    			}
    			else
    			{
    				checkResults.close();
    				CASSegmentID returnID = (CASSegmentID) oidFactory.getNewOID(CASIDDataDescriptor.class);
    				CASStoreInfo returnInfo = new CASStoreInfo(CASStoreOperationStatus.kSegmentCreated, returnID);
    				StoreAsyncCompletionHandler<A> internalCompletionHandler = new StoreAsyncCompletionHandler<A>(completionHandler, attachment);
    				dataID = findOrStoreDataDescriptorAsync(connection, segmentDescriptor, internalCompletionHandler, returnInfo);
    				// Update the CAS table
    				insertIntoCASTable(connection, version.getVersionID(), dataID, returnID, internalCollectionID);
    				return;
    			}
    		} catch (SQLException e)
    		{
    			Logger.getLogger(getClass()).error(new ErrorLogMessage("Got SQL Exception inserting segment {0}", segmentDescriptor.getCASIdentifier()), e);
    			throw new IOException("Could not insert segment "+segmentDescriptor.getCASIdentifier().toString(), e);
    		}
    	}
    }
	
	private synchronized void findOrStoreAndUpdateCAS(DBCASServerConnection connection, CASIDDataDescriptor dataDescriptor, long transactionID, 
			ObjectID segmentID, int internalCollectionID) 
					throws SQLException, IOException
	{
		Log4JStopWatch wholeWatch = new Log4JStopWatch(kStopWatchName+".findOrStoreAndUpdateCAS");
		if (findOrStoreAndUpdateCASStoreFunction != null)
		{		
			CASStore casStore = connection.getCASStoreForWriting(dataDescriptor.getLength());
			int storeNum = ((DBCASServer)connection.getServer()).getIndexOfStore(casStore.getCASStoreID());
			findOrStoreAndUpdateCASStoreFunction.findOrStoreAndUpdateCAS(connection.getStatements().getDBConnection(), dataDescriptor.getCASIdentifier(), dataDescriptor.getLength(), 
					storeNum, segmentID, internalCollectionID, transactionID);
			casStore.storeSegment(dataDescriptor);
		}
		else
		{
			long dataID = findOrStoreDataDescriptor(connection, dataDescriptor);
			// Update the CAS table
			insertIntoCASTable(connection, transactionID, dataID, segmentID, internalCollectionID);
		}
		wholeWatch.stop();
	}
    /**
     * First, checks the local "data" table to see if the hash (CASID) exists in the table, if it does, returns the short dataid.
     * If it doesn't, selects a CASStore to store it in, stores the data in the CASStore and puts the record into the data table.
     * @param dataDescriptor
     * @return
     * @throws SQLException
     * @throws IOException 
     * @throws NoSpaceException 
     */
    private synchronized long findOrStoreDataDescriptor(DBCASServerConnection connection, CASIDDataDescriptor dataDescriptor) throws SQLException, IOException
    {
    	long dataID = -1;
    	boolean alreadyStored = false;
    	Log4JStopWatch wholeWatch = new Log4JStopWatch(kStopWatchName+".findOrStoreDataDescriptor");

		CASStore casStore = connection.getCASStoreForWriting(dataDescriptor.getLength());
		int storeNum = ((DBCASServer)connection.getServer()).getIndexOfStore(casStore.getCASStoreID());
		
		Log4JStopWatch findOrStoreSegmentDBWatch = new Log4JStopWatch(kStopWatchName+".findOrStoreDataDescriptor.findOrStoreSegmentDB");

		Connection dbConnection = connection.getStatements().getDBConnection();
		if (findOrStoreSegmentFunction != null)
		{
			FindOrStoreSegmentResult result = findOrStoreSegmentFunction.findOrStoreSegment(dbConnection, dataDescriptor.getCASIdentifier(), dataDescriptor.getLength(), storeNum);
			dataID = result.getDataID();
			alreadyStored = result.alreadyExists();
		}
		else
		{
			// First, check to see if this exists in the database
			ResultSet checkResults = connection.getStatements().getCheckForDataExistence().checkForDataExistence(dataDescriptor);

			try
			{
				if (checkResults.next())
				{
					dataID = checkResults.getInt("id");
					alreadyStored = true;
				}
				else
				{
					boolean returnAutoCommit = false;	// TODO - remove this and get the commit semantics right!
					if (dbConnection.getAutoCommit())
					{
						dbConnection.setAutoCommit(false);
						returnAutoCommit = true;
					}
					//Savepoint storeSavepoint = connection.getDBConnection().setSavepoint();
					try
					{
						dataID = connection.getStatements().getInsertSegment().insertSegment(dataDescriptor, storeNum);

						segmentToStoreMapCache.put(dataDescriptor.getCASIdentifier(), storeNum);
						//connection.getDBConnection().releaseSavepoint(storeSavepoint);

					} 
					catch (SQLException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught error in storeDataSegment for {0} aborting", dataDescriptor.getCASIdentifier()), e);
						//connection.getDBConnection().rollback(storeSavepoint);
						throw e;
					}
					catch (Error e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught error in storeDataSegment for {0} aborting", dataDescriptor.getCASIdentifier()), e);
						//connection.getDBConnection().rollback(storeSavepoint);
						throw e;
					}
					finally
					{
						if (returnAutoCommit)
							dbConnection.setAutoCommit(true);
					}       

				}
			}
			finally
			{
				checkResults.close();
			}
		}
		findOrStoreSegmentDBWatch.stop();
		if (!alreadyStored)
		{
			// We store the segment last in anticipation of replacing all of the JDBC calls above with a single call to a stored procedure
			casStore.storeSegment(dataDescriptor);
		}
		else
		{
			dataDescriptor.close();	// Even if we're not going to use it, we need to release it
		}
		wholeWatch.stop();
    	return dataID;
    }
    
    private <A> long findOrStoreDataDescriptorAsync(DBCASServerConnection connection, CASIDDataDescriptor dataDescriptor, AsyncCompletion<Void, A> completionHandler, A attachment) throws SQLException, IOException
    {
    	long dataID = -1;
    	boolean alreadyStored = false;
    	Log4JStopWatch wholeWatch = new Log4JStopWatch(kStopWatchName+".findOrStoreDataDescriptor");

		CASStore casStore = connection.getCASStoreForWriting(dataDescriptor.getLength());
		int storeNum = ((DBCASServer)connection.getServer()).getIndexOfStore(casStore.getCASStoreID());
		
		Log4JStopWatch findOrStoreSegmentDBWatch = new Log4JStopWatch(kStopWatchName+".findOrStoreDataDescriptor.findOrStoreSegmentDB");

		Connection dbConnection = connection.getStatements().getDBConnection();
		if (findOrStoreSegmentFunction != null)
		{
			FindOrStoreSegmentResult result = findOrStoreSegmentFunction.findOrStoreSegment(dbConnection, dataDescriptor.getCASIdentifier(), dataDescriptor.getLength(), storeNum);
			dataID = result.getDataID();
			alreadyStored = result.alreadyExists();
		}
		else
		{
			// First, check to see if this exists in the database
			ResultSet checkResults = connection.getStatements().getCheckForDataExistence().checkForDataExistence(dataDescriptor);

			try
			{
				if (checkResults.next())
				{
					dataID = checkResults.getInt("id");
					alreadyStored = true;
				}
				else
				{
					boolean returnAutoCommit = false;	// TODO - remove this and get the commit semantics right!
					if (dbConnection.getAutoCommit())
					{
						dbConnection.setAutoCommit(false);
						returnAutoCommit = true;
					}
					//Savepoint storeSavepoint = connection.getDBConnection().setSavepoint();
					try
					{
						dataID = connection.getStatements().getInsertSegment().insertSegment(dataDescriptor, storeNum);

						segmentToStoreMapCache.put(dataDescriptor.getCASIdentifier(), storeNum);
						//connection.getDBConnection().releaseSavepoint(storeSavepoint);

					} 
					catch (SQLException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught error in storeDataSegment for {0} aborting", dataDescriptor.getCASIdentifier()), e);
						//connection.getDBConnection().rollback(storeSavepoint);
						throw e;
					}
					catch (Error e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught error in storeDataSegment for {0} aborting", dataDescriptor.getCASIdentifier()), e);
						//connection.getDBConnection().rollback(storeSavepoint);
						throw e;
					}
					finally
					{
						if (returnAutoCommit)
							dbConnection.setAutoCommit(true);
					}       

				}
			}
			finally
			{
				checkResults.close();
			}
		}
		findOrStoreSegmentDBWatch.stop();
		if (!alreadyStored)
		{
			// We store the segment last in anticipation of replacing all of the JDBC calls above with a single call to a stored procedure
			casStore.storeSegmentAsync(dataDescriptor, completionHandler, attachment);
		}
		else
		{
			completionHandler.completed(null, attachment);
			dataDescriptor.close();	// Even if we're not going to use it, we need to release it
		}
		wholeWatch.stop();
    	return dataID;
    }
    /**
     * @param mutable
     * @param dataID
     * @param returnID
     * @throws SQLException
     */
    private void insertIntoCASTable(DBCASServerConnection connection, long transactionID, long dataID, ObjectID returnID, int internalCollectionID) throws SQLException
    {
		Log4JStopWatch wholeWatch = new Log4JStopWatch(kStopWatchName+".insertIntoCASTable");
        connection.getStatements().getInsertCASTable().insertCASTable(transactionID, dataID, returnID, internalCollectionID);
        wholeWatch.stop();
    }
      
    
    public synchronized void storeMetaData(DBCASServerConnection connection, CASIDDataDescriptor metaDataDescriptor, int internalCollectionID) throws IOException
    {
    	try
    	{
    		// TODO - handle meta data in a more sane way.  There needs to be reference counting, etc. on the data descriptor

    		ResultSet mdSet = connection.getStatements().getRetrieveMetaData().retrieveMetaData(internalCollectionID);
    		if (mdSet.next())
    		{
    			CASIdentifier casID = getCASIDFromResultSet(mdSet, "casid");
    			int storeNum = mdSet.getInt("storenum");
    			long dataID = mdSet.getLong("data_id");

    			deleteFromCAS(connection, storeNum, casID, dataID);

    			try
    			{
    				CASStore retrieveStore = connection.getStoreAtIndex(storeNum);
    				if (retrieveStore == null)
    					throw new IOException("Could not retrieve store "+internalCollectionID+" storeNum = "+storeNum);
    				retrieveStore.deleteSegment(casID);
    			}
    			finally
    			{
    				mdSet.close();
    			}
    		}

    		long dataID = findOrStoreDataDescriptor(connection, metaDataDescriptor);
    		connection.getStatements().getSetMetaData().setMetaData(dataID, internalCollectionID);
    	}
    	catch (SQLException e)
    	{
    		throw new IOException("Got SQLException storing metadata");
    	}
    }
    
	public synchronized void removeMetaData(DBCASServerConnection connection, int internalCollectionID) throws IOException
	{
        try
        {
        	ResultSet mdSet = connection.getStatements().getRetrieveMetaData().retrieveMetaData(internalCollectionID);
            if (mdSet.next())
            {
                CASIdentifier casID = getCASIDFromResultSet(mdSet, "casid");
                int storeNum = mdSet.getInt("storenum");
                long dataID = mdSet.getLong("data_id");
                
                deleteFromCAS(connection, storeNum, casID, dataID);

				try
				{
					CASStore retrieveStore = connection.getStoreAtIndex(storeNum);
					if (retrieveStore == null)
						throw new IOException("Could not retrieve store "+internalCollectionID+" storeNum = "+storeNum);
					retrieveStore.deleteSegment(casID);
				}
				finally
				{
					mdSet.close();
				}
            }
        	// dataID 0 does not exist
        	
            connection.getStatements().getSetMetaData().setMetaData(0, internalCollectionID);
        }
        catch (SQLException e)
        {
            throw new IOException("Got SQLException storing metadata");
        }
	}    
	
    public synchronized CASIDDataDescriptor retrieveMetaData(DBCASServerConnection connection, int internalCollectionID) throws IOException
    {
        try
        {
            ResultSet mdSet = connection.getStatements().getRetrieveMetaData().retrieveMetaData(internalCollectionID);
            if (mdSet.next())
            {
                CASIdentifier casID = getCASIDFromResultSet(mdSet, "casid");
                int storeNum = mdSet.getInt("storenum");
                CASIDDataDescriptor returnDescriptor;
				try
				{
					CASStore retrieveStore = connection.getStoreAtIndex(storeNum);
					if (retrieveStore == null)
						throw new IOException("Could not retrieve store "+internalCollectionID+" storeNum = "+storeNum);
					returnDescriptor = retrieveStore.retrieveSegment(casID);
				} catch (SegmentNotFound e)
				{
					// TODO This would be a good place to check a backup server
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					throw new IOException("Could not find segment "+casID);
				}
				finally
				{
					mdSet.close();
				}
                return returnDescriptor;
            } else
            {
                return null;
            }
        } catch (SQLException e)
        {
        	logger.error(new ErrorLogMessage("Caught SQLException retrieving metadata"), e);
            throw new IOException("Caught SQLException retrieving metadata");
        }
    }

	public CASIdentifier getCASIDFromResultSet(ResultSet mdSet, String field)
			throws SQLException
	{
		return new CASIdentifier(mdSet.getBytes(field));
	}
    
	public synchronized boolean releaseSegment(DBCASServerConnection connection, ObjectID releaseID, int internalCollectionID) throws IOException
	{
        try
        {
            ResultSet retrieveSet = connection.getStatements().getRetrieveSegmentByOID().retrieveSegmentByOID(releaseID, internalCollectionID);
            while (retrieveSet.next())
            {
            	long dataID = retrieveSet.getLong("dataid");
            	long versionID = retrieveSet.getLong("versionid");
            	connection.getStatements().getDeleteFromCASStatement().deleteFromCAS(internalCollectionID, releaseID, versionID);
            	ResultSet referencesResults = connection.getStatements().getGetReferencesForCAS().getReferencesForCAS(dataID);
            	if (referencesResults.next())
            	{
            		int numReferences = referencesResults.getInt(1);
            		if (numReferences == 0)
            		{
                        CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
                        int storeNum = retrieveSet.getInt("storenum");
                        deleteFromCAS(connection, storeNum, casID, dataID);
            		}
            	}
            	return true;
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            throw new IOException("SQL Exception while retrieving");
        }
		return false;
	}

	private void deleteFromCAS(DBCASServerConnection connection, int storeNum, CASIdentifier casID, long dataID) throws SQLException, IOException
	{
		connection.getStatements().getRemoveCAS().removeCAS(dataID);
		connection.getStoreAtIndex(storeNum).deleteSegment(casID);
	}
	
	public synchronized boolean releaseVersionedSegment(DBCASServerConnection connection, ObjectID releaseID, IndelibleVersion version, int internalCollectionID) throws IOException
	{
        try
        {
            ResultSet retrieveSet = connection.getStatements().getRetrieveSegmentByOIDExactVersionStatement().retrieveSegmentByOIDExactVersion(releaseID, version, internalCollectionID);
            if (retrieveSet.next())
            {
            	long dataID = retrieveSet.getLong("id");
            	ResultSet referencesResults = connection.getStatements().getGetReferencesForCAS().getReferencesForCAS(dataID);
            	if (referencesResults.next())
            	{
            		int numReferences = referencesResults.getInt(1);
            		if (numReferences == 0)
            		{
                        CASIdentifier casID = getCASIDFromResultSet(retrieveSet, "casid");
                        int storeNum = retrieveSet.getInt("storenum");
                        deleteFromCAS(connection, storeNum, casID, dataID);
            		}
            	}
            	return true;
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            throw new IOException("SQL Exception while retrieving");
        }
		return false;
	}
    public CASIdentifier retrieveCASIdentifier(DBCASServerConnection connection, CASSegmentID segmentID, int internalCollectionID) throws IOException
    {
    	CASIdentifier returnIdentifier = null;
    	try
    	{
    		ResultSet retrieveResults = connection.getStatements().getRetrieveSegmentIDForOID().retrieveSegmentIDForOID(segmentID, internalCollectionID);
    		if (retrieveResults.next())
    		{
    			returnIdentifier = getCASIDFromResultSet(retrieveResults, "casid");
    		}
    	}
    	catch (SQLException e)
    	{
    		e.printStackTrace();
    		throw new IOException("SQL Exception while retrieving");
    	}
    	return returnIdentifier;
    }

    /**
     * Logs the event to the database and fires it
     * @param connection
     * @param event
     * @param internalCollectionID
     * @param transaction 
     * @return
     */
    protected void logCollectionEventToDB(DBCASServerConnection connection, CASCollectionEvent event, int internalCollectionID, IndelibleFSTransaction transaction)
    {	
    	try
		{
			connection.getStatements().getAddLocalEvent().addLocalEvent(event, internalCollectionID, transaction);
		} catch (SQLException e)
		{
			logger.error("Unable to log event due to SQL Exception" ,e);
			throw new InternalError("Unable to log event due to SQL Exception");
		}
    	return;
    }

    protected void logReplicatedEventToDB(DBCASServerConnection connection, CASCollectionEvent event, int internalCollectionID, IndelibleFSTransaction transaction)
    {
    	try
		{
    		int serverNum = getServerNumForID(event.getSource());			
			connection.getStatements().getAddReplicatedEvent().addReplicatedEvent(serverNum, event, internalCollectionID, transaction);
		} catch (SQLException e)
		{
			logger.error("Unable to log event due to SQL Exception" ,e);
			throw new InternalError("Unable to log event due to SQL Exception");
		}
    	return;
    }

	@Override
	public void logServerEvent(DBCASServerConnection connection, ObjectID source, CASServerEvent event, IndelibleFSTransaction transaction)
	{
		if (event.getTimestamp() == 0)
			event.setTimestamp(System.currentTimeMillis());
		if (event.getEventID() < 0)
		{
			synchronized(this)
			{
				event.setEventID(nextLocalServerEventID++);
			}
		}
		
		boolean fireEvent = false;
		if (transaction == null)
		{
        	synchronized(this)
        	{
        		transaction = new IndelibleFSTransaction(id, nextTransactionID);
        		nextTransactionID++;
        		fireEvent = true;	// We won't go through commit
        	}
		}
		logServerEventToDB(connection, source, event, transaction);
		if (fireEvent)
		{
			fireServerEvent(event);
		}
	}
	
	public void fireServerEvent(CASServerEvent eventToFire)
	{
		synchronized(connections)
		{
			for (DBCASServerConnection curConnection:connections)
			{
				curConnection.fireIndelibleEvent(eventToFire);
			}
		}
	}
    public void logServerEventToDB(DBCASServerConnection connection, ObjectID source, CASServerEvent event, IndelibleFSTransaction transaction)
    {    	
    	try
		{
    		connection.getStatements().getAddLocalServerEvent().addLocalServerEvent(event, transaction);
		} catch (SQLException e)
		{
			logger.error("Unable to log event due to SQL Exception" ,e);
			throw new InternalError("Unable to log event due to SQL Exception");
		}
    	return;
    }

    public void logReplicatedServerEvent(DBCASServerConnection connection, ObjectID source, CASServerEvent event, IndelibleFSTransaction transaction)
    {
    	try
		{
    		int serverNum = getServerNumForID(event.getSource());
			connection.getStatements().getAddReplicatedServerEvent().addReplicatedServerEventStatement(serverNum, event, transaction);
		} catch (SQLException e)
		{
			logger.error("Unable to log event due to SQL Exception" ,e);
			throw new InternalError("Unable to log event due to SQL Exception");
		}
    	return;
    }
    
	@Override
	public void logCASCollectionEvent(DBCASServerConnection casServerConnection, ObjectID source, CASCollectionEvent event, IndelibleFSTransaction transaction) throws IOException
	{
		DBCASCollection collection = collections.get(source);
		if (collection == null)
			throw new IllegalArgumentException("Can't find collection for ID "+source.toString());
		collection.setEventIDAndTimestamp(event);	// Fill in event ID and timestamp if they haven't been set already
		boolean fireEvent = false;
		if (transaction == null)
		{
			throw new InternalError("Can't log a collection event outside of a transaction");
		}
		int internalCollectionID = collection.getInternalCollectionID();
		logCollectionEventToDB(casServerConnection, event, internalCollectionID, transaction);
		if (fireEvent)
		{
			collection.fireTransactionEvent(casServerConnection, transaction);
		}
	}
    
	
    @Override
	public void logReplicatedCASCollectionEvent(DBCASServerConnection casServerConnection, ObjectID source, CASCollectionEvent event, IndelibleFSTransaction transaction)
	{
		DBCASCollection collection = collections.get(source);
		if (collection == null)
			throw new IllegalArgumentException("Can't find collection for ID "+source.toString());
		if (transaction == null)
		{
        	throw new IllegalArgumentException("Cannot log replicated events without a transaction");
		}
		logReplicatedEventToDB((DBCASServerConnection)casServerConnection, event, collection.getInternalCollectionID(), transaction);
	}

    public CASCollectionEvent[] getCollectionEventsAfterEventID(DBCASServerConnection connection, CASCollectionID collectionID, long startingEventID, int numToReturn) throws IOException
    {
    	DBCASCollection eventCollection = collections.get(collectionID);
    	if (eventCollection == null)
    		throw new IllegalArgumentException("Could not find collection with ID "+collectionID);
    	try
    	{
    		GetLocalEventsAfterStatement getLocalEventsAfter = connection.getStatements().getGetLocalEventsAfter();
    		synchronized(getLocalEventsAfter)
    		{
    			ResultSet eventsResults = getLocalEventsAfter.getLocalEventsAfter(eventCollection.getInternalCollectionID(), startingEventID, numToReturn);
    			return getCollectionEventsFromResultSet(connection, eventsResults);
    		}
    	} catch (SQLException e)
    	{
    		Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    		throw new IOException("Got SQLException retrieving events");
    	}
    }
    
    public CASCollectionEvent [] getCollectionEventsAfterTimestamp(DBCASServerConnection connection, CASCollectionID collectionID, long timestamp, int numToReturn) throws IOException
    {
    	DBCASCollection eventCollection = collections.get(collectionID);
    	if (eventCollection == null)
    		throw new IllegalArgumentException("Could not find collection with ID "+collectionID);
    	GetLocalEventsAfterTimestampStatement getLocalEventsAfterTimestamp = connection.getStatements().getGetLocalEventsAfterTimestamp();
    	synchronized(getLocalEventsAfterTimestamp)
    	{
    		try
    		{
    			ResultSet localEvents = getLocalEventsAfterTimestamp.getLocalEventsAfterTimestamp(eventCollection.getInternalCollectionID(), timestamp, numToReturn);
    			return getCollectionEventsFromResultSet(connection, localEvents);
    		} catch (SQLException e)
    		{
    			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    			throw new IOException("Got SQLException retrieving events");
    		}
    	}
    }
    
    public CASCollectionEvent [] getCollectionEventsAfterEventIDForTransaction(DBCASServerConnection connection, CASCollectionID collectionID, long startingEventID, int numToReturn, IndelibleFSTransaction transaction) throws IOException
    {
    	DBCASCollection eventCollection = collections.get(collectionID);
    	if (eventCollection == null)
    		throw new IllegalArgumentException("Could not find collection with ID "+collectionID);
    		try
    		{
    			GetLocalEventsAfterForTransactionStatement getLocalEventsAfterForTransaction = connection.getStatements().getGetLocalEventsAfterForTransaction();
    			synchronized(getLocalEventsAfterForTransaction)
    			{
    				ResultSet localEventsResults = getLocalEventsAfterForTransaction.getLocalEventsAfterForTransaction(eventCollection.getInternalCollectionID(), startingEventID, transaction, numToReturn);
    				return getCollectionEventsFromResultSet(connection, localEventsResults);
    			}
    		} catch (SQLException e)
    		{
    			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    			throw new IOException("Got SQLException retrieving events");
    		}
    	
    }
    
    public long getLastReplicatedEventID(DBCASServerConnection connection, EntityID sourceServerID, CASCollectionID collectionID) throws SQLException
    {
    	DBCASCollection eventCollection = collections.get(collectionID);
    	if (eventCollection == null)
    		throw new IllegalArgumentException("Could not find collection with ID "+collectionID);
    	int internalServerNum = getServerNumForID(sourceServerID);
		GetLastReplicatedEventStatement getLastReplicatedEvent = connection.getStatements().getGetLastReplicatedEvent();
		long returnEventID = -1L;
    	synchronized(getLastReplicatedEvent)
    	{

			ResultSet lastEventResultSet = getLastReplicatedEvent.getLastReplicatedEvent(internalServerNum, eventCollection.getInternalCollectionID());
    		if (lastEventResultSet.next())
    		{
    			if (lastEventResultSet.getLong(2) > 0)	// The count must be > 0 for max ID to make any sense
    				returnEventID = lastEventResultSet.getLong(1);
    		}
    		lastEventResultSet.close();
    	}
    	return returnEventID;
    }
    
    
	@Override
	public long getLastServerEventID(DBCASServerConnection connection)
	{
		// select max(eventid) from localserverevents
		GetLastLocalServerEventIDStatement getLastLocalServerEventID = connection.getStatements().getGetLastLocalServerEventID();
		try
		{
			synchronized(getLastLocalServerEventID)
			{
				ResultSet lastLocalServerEventIDResultSet = getLastLocalServerEventID.getLastLocalServerEventID();
				if (lastLocalServerEventIDResultSet.next() && lastLocalServerEventIDResultSet.getLong(2) > 0)
				{
					return lastLocalServerEventIDResultSet.getLong(1);
				}
			}
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
    	return -1;
	}

	public static ObjectID getObjectIDFromResultSet(ResultSet set, String field) throws SQLException
	{
		byte[] idBytes = set.getBytes(field);
		ObjectID returnID = null;
		if (idBytes.length == ObjectID.kTotalBytes)
		{
			returnID = ObjectIDFactory.reconstituteFromBytes(idBytes);
		}
		return returnID;
	}
	
	private CASCollectionEvent[] getCollectionEventsFromResultSet(DBCASServerConnection connection,
			ResultSet eventsResults) throws SQLException
	{
		ArrayList<CASCollectionEvent>eventList = new ArrayList<CASCollectionEvent>();
    	while (eventsResults.next())
    	{
    		long curEventID = eventsResults.getLong("eventid");
    		ObjectID segmentID = getObjectIDFromResultSet(eventsResults, "cassegmentid");
    		String eventTypeStr = eventsResults.getString("eventtype").trim();
    		long timestamp = eventsResults.getLong("timestamp");
    		long transactionID = eventsResults.getLong("transactionid");

    		CASCollectionEventType eventType = CASCollectionEventType.eventTypeForChar(eventTypeStr.charAt(0));
    		CASCollectionEvent event;
    		switch (eventType)
    		{
    		case kMetadataModified:
    			event = new MetadataModifiedEvent(id, curEventID, timestamp);
    			break;
    		case kSegmentCreated:
    			event = new SegmentCreatedEvent(segmentID, id, curEventID, timestamp);
    			break;
    		case kSegmentReleased:
    			event = new SegmentReleasedEvent(segmentID, id, curEventID, timestamp);
    			break;
    		case kTransactionCommited:
        		long versionID = eventsResults.getLong("versionid");
        		IndelibleVersion version = new IndelibleVersion(versionID, -1, -1);
        		loadTime(connection, version);
    			IndelibleFSTransaction transaction = new IndelibleFSTransaction(id, transactionID);
    			transaction.setVersion(version);
				event = new TransactionCommittedEvent(transaction, id, curEventID, timestamp);
    			break;
    		default:
    			continue;
    		}
    		eventList.add(event);
    	}
    	eventsResults.close();
    	CASCollectionEvent [] returnEvents = new CASCollectionEvent[eventList.size()];
    	returnEvents = eventList.toArray(returnEvents);
    	return returnEvents;
	}

	private CASServerEvent[] getServerEventsFromStatement(ResultSet eventsResults) throws SQLException
	{
		ArrayList<CASServerEvent>eventList = new ArrayList<CASServerEvent>();
    	while (eventsResults.next())
    	{
    		long curEventID = eventsResults.getLong("eventid");
    		CASCollectionID collectionID = (CASCollectionID)getObjectIDFromResultSet(eventsResults, "collectionid");
    		String eventTypeStr = eventsResults.getString("eventtype").trim();
    		long timestamp = eventsResults.getLong("timestamp");
    		long transactionID = eventsResults.getLong("transactionid");
    		
    		CASServerEventType eventType = CASServerEventType.eventTypeForChar(eventTypeStr.charAt(0));
    		CASServerEvent event;
    		switch (eventType)
    		{
    		case kCollectionCreated:
    			event = new CollectionCreatedEvent(id, curEventID, timestamp, collectionID); 
    			break;
    		case kCollectionDestroyed:
    			event = new CollectionDestroyedEvent(id, curEventID, timestamp, collectionID); 
    			break;
    		default:
    			continue;
    		}
    		eventList.add(event);
    	}
    	eventsResults.close();
    	CASServerEvent [] returnEvents = new CASServerEvent[eventList.size()];
    	returnEvents = eventList.toArray(returnEvents);
    	return returnEvents;
	}
	
	public void addConnectedServer(DBCASServerConnection connection, EntityID serverID, EntityID securityServerID)
	{
		try
		{
			synchronized(serverNums)
			{
				if (serverNums.containsKey(serverID))
					return;	// Already entered
				connection.getStatements().getAddConnectedServer().addConnectedServer(serverID, securityServerID);

				int serverNum = connection.getStatements().getGetServerNumForServerIDStatement().getServerNumForServerID(serverID);
				if (serverNum < 0)
					throw new InternalError("Cannot retrieve servernum");
				serverNums.put(serverID, serverNum);
				serverIDs.put(serverNum, serverID);
			}
		} catch (Throwable e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InternalError("Could not connect server");
		} 
	}
	
	private EntityID getServer(int serverNum)
	{
		synchronized(serverNums)
		{
			return serverIDs.get(serverNum);
		}
	}
	
	private int getServerNumForID(EntityID serverID)
	{
		synchronized(serverNums)
		{
			Integer returnServerNum = serverNums.get(serverID);
			if (returnServerNum != null)
				return returnServerNum;
			else
				return -1;
		}
	}

	public CASServerEvent[] getServerEventsAfterEventID(DBCASServerConnection connection, long startingEventID, int maxToReturn) throws IOException
	{
    	GetLocalServerEventsAfterStatement getLocalServerEventsAfterStmt = connection.getStatements().getGetLocalServerEventsAfter();
    	synchronized(getLocalServerEventsAfterStmt)
    	{
    		try
			{
				return getServerEventsFromStatement(getLocalServerEventsAfterStmt.getLocalServerEventsAfter(startingEventID, maxToReturn));
			} catch (SQLException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				throw new IOException("Got SQLException retrieving events");
			}
    	}
	}

	@Override
	public CASServerEvent[] getServerEventsAfterTimestamp(
			DBCASServerConnection connection, long timestamp, int numToReturn)
			throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CASServerEvent[] getServerEventsAfterEventIDForTransaction(
			DBCASServerConnection connection, CASCollectionID collectionID,
			long startingEventID, int numToReturn,
			IndelibleFSTransaction transaction) throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	public DBCASSegmentIDIterator listSegmentIDs(DBCASServerConnection connection, int internalCollectionID) throws IOException
	{
		try
		{
			ResultSet listSet = connection.getStatements().getListSegmentIDs().listSegmentIDs(internalCollectionID);
			return new DBCASSegmentIDIterator(listSet);
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IOException("Caught SQL Exception");
		}
	}
	
	@Override
	public CASStoreManager getCASStoreManager()
	{
		return storeManager;
	}
	
	public IndelibleVersion getNewVersion(DBCASServerConnection connection)
    {
        return connection.getStatements().getCreateVersionStatement().createVersion(this, connection);
    }

    public long getVersionIDForVersion(DBCASServerConnection connection, IndelibleVersion version)
    {
        try
        {
        	return connection.getStatements().getGetVersionIDForVersionStatement().getVersionIDForVersion(version);
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            throw new InternalError("Got SQLException retrieving transaction ID");
        }
    }

    public void loadTime(DBCASServerConnection connection, IndelibleVersion version)
    {
    	ResultSet versionRS = null;
    	GetVersionInfoForVersionIDStatement getVersionInfoForVersionIDStatement = connection.getStatements().getGetVersionInfoForVersionIDStatement();
    	synchronized (getVersionInfoForVersionIDStatement)
    	{
    		try
    		{
    			versionRS = getVersionInfoForVersionIDStatement.getVersionInfoForVersionID(version.getVersionID());
    			if (versionRS.next())
    			{
    				version.setVersionTime(versionRS.getLong(1));
    				version.setUniquifier(versionRS.getInt(2));
    			}
    		} catch (SQLException e)
    		{
    			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    			throw new InternalError("Got SQLException retrieving version info");
    		}
    		finally
    		{
    			if (versionRS != null)
					try
					{
						versionRS.close();
					} catch (SQLException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}
    		}
    	}
    }
    private long lastVersionTime;
    private int lastVersionUniquifier;
    
    private void commitVersion(DBCASServerConnection connection, IndelibleVersion version)
    throws IOException
    {
        finalizeVersion(version);
        try
        {
        	connection.getStatements().getCommitVersionStatement().commitVersion(version);
        }
        catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Got SQL Exception while committing version"), e);
            throw new IOException("Could not commit version");
        }
    }

    @Override
	public void finalizeVersion(IndelibleVersion version)
	{
		if (!version.isFinal())
        {	
        	// Sometimes we get called with replicated versions which will already have everything filled in
        	long curVersionTime = System.currentTimeMillis();
        	if (lastVersionTime == curVersionTime)
        		lastVersionUniquifier++;
        	else
        		lastVersionUniquifier = 0;
        	lastVersionTime = curVersionTime;
        	version.setVersionTime(curVersionTime);
        	version.setUniquifier(lastVersionUniquifier);
        }
	}

	public IndelibleVersionIterator listVersionsForSegment(
			DBCASServerConnection connection, ObjectID segmentID,
			int internalCollectionID) throws IOException
	{
		try
		{
			ResultSet versionsRS = connection.getStatements().getListVersionsForSegmentStatment().listVersionsForSegment(segmentID, internalCollectionID);
			ArrayList<IndelibleVersion>versionList = new ArrayList<IndelibleVersion>();
			while(versionsRS.next())
			{
				long versionID = versionsRS.getLong("versionID");
				long versionTime = versionsRS.getLong("versiontime");
				int uniquifier = versionsRS.getInt("uniquifier");
				IndelibleVersion curVersion = new IndelibleVersion(versionID, versionTime, uniquifier);
				versionList.add(curVersion);
			}
			return new IndelibleVersionListIterator(versionList);
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IOException("Got SQLException retrieving versions");
		}	
	}
}
