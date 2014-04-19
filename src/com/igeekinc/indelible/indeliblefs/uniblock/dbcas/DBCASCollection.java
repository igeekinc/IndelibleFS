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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEventType;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.CASSegmentIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreOperationStatus;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.MetadataModifiedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentCreatedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentReleasedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.logging.ErrorLogMessage;

class StoreVersionedSegmentFuture extends ComboFutureBase<Void>
{
	private DBCASCollectionConnection connection;
	private CASCollectionEvent sourceEvent;
	public StoreVersionedSegmentFuture(DBCASCollectionConnection connection, CASCollectionEvent sourceEvent)
	{
		this.connection = connection;
		this.sourceEvent = sourceEvent;
	}
	
	public <A>StoreVersionedSegmentFuture(DBCASCollectionConnection connection, CASCollectionEvent sourceEvent,
			AsyncCompletion<Void, ? super A>completionHandler, A attachment)
	{
		super(completionHandler, attachment);
		this.connection = connection;
		this.sourceEvent = sourceEvent;
	}
	@Override
	public synchronized void completed(Void result, Object attachment)
	{
		try
		{
			connection.logEvent(sourceEvent);
			super.completed(result, attachment);
		} catch (Throwable t)
		{
			super.failed(t, attachment);
		}
		
	}
}

class StoreSegmentFuture extends ComboFutureBase<CASStoreInfo>
{
	AsyncCompletion<CASStoreInfo, Object> completionHandler;
	Object attachment;
	
	public StoreSegmentFuture()
	{
		
	}
	
	public StoreSegmentFuture(AsyncCompletion<CASStoreInfo, Object> completionHandler, Object attachment)
	{
		this.completionHandler = completionHandler;
		this.attachment = attachment;
	}
}
public class DBCASCollection implements CASCollection
{
    private CASCollectionID id;
    private int internalCollectionID;
	private long nextEventID;
	private ArrayList<DBCASCollectionConnection> connections;
    
	public DBCASCollection(CASCollectionID id, int internalCollectionID, long nextEventID)
	{
		this.id = id;
		this.internalCollectionID = internalCollectionID;
		this.nextEventID = nextEventID;
		connections = new ArrayList<DBCASCollectionConnection>();
	}

	public CASCollectionID getID()
	{
		return id;
	}
	
	public CASIDDataDescriptor retrieveSegment(DBCASCollectionConnection connection, CASIdentifier segmentID)
		    throws IOException, SegmentNotFound
	{
		return connection.getServerConnection().retrieveSegment(segmentID, internalCollectionID);
	}
	
	public CASStoreInfo storeSegment(DBCASCollectionConnection connection, CASIDDataDescriptor segmentDescriptor) throws IOException
	{
		CASStoreInfo returnInfo = connection.getServerConnection().storeSegment(segmentDescriptor, internalCollectionID);
		if (returnInfo.getStatus() == CASStoreOperationStatus.kSegmentCreated)
		{
			SegmentCreatedEvent event = new SegmentCreatedEvent(returnInfo.getSegmentID(), connection.getServerConnection().getServerID());
			connection.logEvent(event);
		}
		return returnInfo;
	}
	
    public void storeVersionedSegment(DBCASCollectionConnection connection, ObjectID id, CASIDDataDescriptor segmentDescriptor) throws IOException
    {
    	connection.getServerConnection().storeVersionedSegment(id, segmentDescriptor, internalCollectionID);
		SegmentCreatedEvent event = new SegmentCreatedEvent(id, connection.getServerConnection().getServerID());
		connection.logEvent(event);
    }
    
	/**
	 * Replicates a segment from another server.  Should only be called by the ReplicationManager.  Needs to be
     * removed from the public API
	 * @param dbcasCollectionConnection
	 * @param replicateSegmentID
	 * @param segmentDescriptor
	 * @param sourceEvent
	 * @throws IOException 
	 */
	public void storeReplicatedSegment(
			DBCASCollectionConnection connection,
			ObjectID replicateSegmentID, IndelibleVersion version, CASIDDataDescriptor segmentDescriptor,
			CASCollectionEvent sourceEvent) throws IOException
	{
		if (sourceEvent.getEventType() != CASCollectionEventType.kSegmentCreated)
			throw new IllegalArgumentException("sourceEvent is not a SegmentCreated event");
		if (sourceEvent.getEventID() < 0 || sourceEvent.getTimestamp() < 0)
			throw new IllegalArgumentException("Replicated events must have event id and timestamp set");
    	connection.getServerConnection().storeVersionedSegment(replicateSegmentID, segmentDescriptor, internalCollectionID);
		connection.logEvent(sourceEvent);
	}
	
	public Future<Void> storeReplicatedSegmentAsync(DBCASCollectionConnection connection,
			ObjectID replicateSegmentID, IndelibleVersion replicateVersion, CASIDDataDescriptor segmentDescriptor, 
			CASCollectionEvent sourceEvent) throws IOException
	{
		if (sourceEvent.getEventType() != CASCollectionEventType.kSegmentCreated)
			throw new IllegalArgumentException("sourceEvent is not a SegmentCreated event");
		if (sourceEvent.getEventID() < 0 || sourceEvent.getTimestamp() < 0)
			throw new IllegalArgumentException("Replicated events must have event id and timestamp set");
		StoreVersionedSegmentFuture future = new StoreVersionedSegmentFuture(connection, sourceEvent);
		connection.getServerConnection().storeVersionedSegmentAsync(replicateSegmentID, segmentDescriptor, internalCollectionID,
				future, null);
		// The future will log the event when storeVersionedSegmentAsync finishes
		return future;
	}
	
	public <A>void storeReplicatedSegmentAsync(DBCASCollectionConnection connection,
			ObjectID replicateSegmentID, IndelibleVersion replicateVersion, CASIDDataDescriptor segmentDescriptor, CASCollectionEvent sourceEvent,
			AsyncCompletion<Void, ? super A>completionHandler, A attachment) throws IOException
	{
		if (sourceEvent.getEventType() != CASCollectionEventType.kSegmentCreated)
			throw new IllegalArgumentException("sourceEvent is not a SegmentCreated event");
		if (sourceEvent.getEventID() < 0 || sourceEvent.getTimestamp() < 0)
			throw new IllegalArgumentException("Replicated events must have event id and timestamp set");
		StoreVersionedSegmentFuture future = new StoreVersionedSegmentFuture(connection, sourceEvent, completionHandler, attachment);
		connection.getServerConnection().storeVersionedSegmentAsync(replicateSegmentID, segmentDescriptor, internalCollectionID,
				future, null);
		// The future will log the event when storeVersionedSegmentAsync finishes.  It will also call the completion handler
	}
	
	public Future<CASStoreInfo> storeSegmentAsync(DBCASCollectionConnection connection, CASIDDataDescriptor segmentDescriptor) throws IOException
	{
		StoreSegmentFuture future = new StoreSegmentFuture();
		connection.getServerConnection().storeSegmentAsync(segmentDescriptor, internalCollectionID,
				future, null);
		return future;
	}
	
	public <A>void storeSegmentAsync(DBCASCollectionConnection connection, CASIDDataDescriptor segmentDescriptor,
			AsyncCompletion<CASStoreInfo, ? super A>completionHandler, A attachment) throws IOException
	{
		StoreSegmentFuture future = new StoreSegmentFuture();
		connection.getServerConnection().storeSegmentAsync(segmentDescriptor, internalCollectionID,
				future, null);
		// The future will log the event when storeVersionedSegmentAsync finishes.  It will also call the completion handler
	}
	
    public CASIdentifier retrieveCASIdentifier(DBCASCollectionConnection connection, CASSegmentID segmentID) throws IOException
    {
    	return connection.getServerConnection().retrieveCASIdentifier(segmentID, internalCollectionID);
    }
    
    public DataVersionInfo retrieveSegment(DBCASCollectionConnection connection, ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags) throws IOException
	{
    	return connection.getServerConnection().retrieveSegment(segmentID, version, flags, internalCollectionID);
	}
    
    public Future<CASIDDataDescriptor> retrieveSegmentAsync(DBCASCollectionConnection connection, 
			CASIdentifier segmentID) throws IOException, SegmentNotFound
	{
    	return connection.getServerConnection().retrieveSegmentAsync(segmentID, internalCollectionID);
	}
    
    public <A> void retrieveSegmentAsync(DBCASCollectionConnection connection, 
			CASIdentifier segmentID, AsyncCompletion<CASIDDataDescriptor, A>completionHandler,
			A attachment) throws IOException, SegmentNotFound
	{
    	connection.getServerConnection().retrieveSegmentAsync(segmentID, internalCollectionID, completionHandler, attachment);
	}
    
    public SegmentInfo retrieveSegmentInfo(DBCASCollectionConnection connection, ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws IOException
	{
    	return connection.getServerConnection().retrieveSegmentInfo(connection, segmentID, version, flags, internalCollectionID);
	}

	public boolean verifySegment(DBCASCollectionConnection connection, ObjectID segmentID, IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException
	{
		return connection.getServerConnection().verifySegment(connection, segmentID, version, flags, internalCollectionID);
	}
    
	public void repairSegment(DBCASCollectionConnection connection, ObjectID repairSegmentID,
			IndelibleVersion transactionVersion, DataVersionInfo masterData) throws IOException
	{
		connection.getServerConnection().repairSegment(connection, repairSegmentID, transactionVersion, masterData, internalCollectionID);
	}
	
	public boolean releaseSegment(DBCASCollectionConnection connection, CASSegmentID releaseID) throws IOException
	{
    	boolean successful = connection.getServerConnection().releaseSegment(releaseID, internalCollectionID);
    	SegmentReleasedEvent event = new SegmentReleasedEvent(releaseID, connection.getServerConnection().getServerID());
    	connection.logEvent(event);
		return successful;
	}

	public boolean releaseVersionedSegment(DBCASCollectionConnection connection, ObjectID releaseID, IndelibleVersion version) throws IOException
	{
    	boolean successful = connection.getServerConnection().releaseVersionedSegment(releaseID, version, internalCollectionID);
    	SegmentReleasedEvent event = new SegmentReleasedEvent(releaseID, connection.getServerConnection().getServerID());
    	connection.logEvent(event);
		return successful;
	}
	
	public boolean[] bulkReleaseSegment(DBCASCollectionConnection connection, CASSegmentID[] releaseIDs)
			throws IOException
	{
		boolean [] returnVals = new boolean[releaseIDs.length];
		for (int curReleaseIDNum = 0; curReleaseIDNum < releaseIDs.length; curReleaseIDNum++)
			returnVals[curReleaseIDNum] = releaseSegment(connection, releaseIDs[curReleaseIDNum]);
		return returnVals;
	}
	
    public synchronized HashMap<String, Serializable> getMetaDataResource(DBCASCollectionConnection connection, String mdResourceName)
    throws PermissionDeniedException, IOException
    {
    	HashMap<String, HashMap<String, Serializable>>metadataHashmap = getMetaDataHashMap(connection);
    	if (metadataHashmap != null)
    	{
    		HashMap<String, Serializable>retrievedMap = metadataHashmap.get(mdResourceName);
    		if (retrievedMap != null)
    			return (HashMap<String, Serializable>)retrievedMap.clone();
    	}
        return null;
    }
    
    public synchronized void setMetaDataResource(DBCASCollectionConnection connection, String mdResourceName, HashMap<String, Serializable> resources)
    throws PermissionDeniedException, IOException
    {
    	HashMap<String, HashMap<String, Serializable>>metadataHashmap = getMetaDataHashMap(connection);
    	if (metadataHashmap == null)
    		metadataHashmap = new HashMap<String, HashMap<String, Serializable>>();
        metadataHashmap.put(mdResourceName, resources);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream mdOutStream = new ObjectOutputStream(baos);
        mdOutStream.writeObject(metadataHashmap);
        mdOutStream.close();
        baos.close();
        CASIDMemoryDataDescriptor mdDataDesciptor = new CASIDMemoryDataDescriptor(baos.toByteArray());
        storeMetaData(connection, mdDataDesciptor);
    }
    
	public String[] listMetaDataNames(DBCASCollectionConnection connection) throws PermissionDeniedException, IOException
	{
		String [] metaDataNames = new String[0];
    	HashMap<String, HashMap<String, Serializable>>metaDataHashmap = getMetaDataHashMap(connection);
    	if (metaDataHashmap != null)
    	{
    		metaDataNames = metaDataHashmap.keySet().toArray(metaDataNames);
    	}
    	return metaDataNames;
	}


    private synchronized HashMap<String, HashMap<String, Serializable>> getMetaDataHashMap(DBCASCollectionConnection connection)
    	    throws PermissionDeniedException, IOException
    {
    	CASIDDataDescriptor mdData = retrieveMetaData(connection);
    	if (mdData != null && mdData.getLength() > 0)
    	{
    		ByteArrayInputStream bais = new ByteArrayInputStream(mdData.getData());
    		ObjectInputStream mdis = new ObjectInputStream(bais);
    		try
    		{
    			Object mdObject = mdis.readObject();
    			if (mdObject != null && mdObject instanceof HashMap)
    			{
    				HashMap<String, HashMap<String, Serializable>>metadataHashmap = (HashMap<String, HashMap<String, Serializable>>)mdObject;
    				return metadataHashmap;
    			}
    		} catch (ClassNotFoundException e)
    		{
    			// TODO Auto-generated catch block
    			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    		}
    	}
    	return null;
    }
    private void storeMetaData(DBCASCollectionConnection connection, CASIDDataDescriptor metaDataDescriptor) throws IOException
    {
    	connection.getServerConnection().storeMetaData(metaDataDescriptor, internalCollectionID);
    	MetadataModifiedEvent event = new MetadataModifiedEvent(connection.getServerConnection().getServerID());
    	connection.logEvent(event);
    }
    
    private CASIDDataDescriptor retrieveMetaData(DBCASCollectionConnection connection) throws IOException
    {
    	return connection.getServerConnection().retrieveMetaData(internalCollectionID);
    }

	public long getLastEventID()
	{
		return nextEventID - 1;
	}

	protected synchronized void setEventIDAndTimestamp(CASCollectionEvent eventToLog)
	{
		if (eventToLog.getEventID() < 0)
		{
			eventToLog.setEventID(nextEventID);
			nextEventID ++ ;
		}
		if (eventToLog.getTimestamp() < 0)
		{
			eventToLog.setTimestamp(System.currentTimeMillis());
		}
	}
	protected synchronized void logEvents(DBCASServerConnection connection, IndelibleFSTransaction transaction) throws IOException
	{
		ArrayList<CASCollectionEvent> events = connection.getLogForSource(id);
		HashSet<ObjectID>seenObjectIDs = new HashSet<ObjectID>();	// No matter how many times a segment is modified we only want one event
																	// per transaction.  seenObjectIDs tracks what has been seen and discards
																	// duplicated events
		long now = System.currentTimeMillis();
		for (CASCollectionEvent curEvent:events)
		{
			if (curEvent.getEventType() == CASCollectionEventType.kSegmentCreated)
			{
				ObjectID eventObjectID = ((SegmentCreatedEvent)curEvent).getSegmentID();
				if (seenObjectIDs.contains(eventObjectID))
					continue;
				seenObjectIDs.add(eventObjectID);
			}
			curEvent.setEventID(nextEventID);
			nextEventID++;
			curEvent.setTimestamp(now);		
			connection.logEventToServer(id, curEvent, transaction);
		}
	}
	
	public synchronized void logReplicatedEvents(DBCASServerConnection connection,
			IndelibleFSTransaction transaction)
	{
		ArrayList<CASCollectionEvent> events = connection.getLogForSource(id);
		for (CASCollectionEvent curEvent:events)
		{
			if (curEvent.getEventID() < 0 || curEvent.getTimestamp() < 0)
				throw new InternalError("Replicated events must have event id and timestamp set");
			connection.logReplicatedEventToServer(id, curEvent, transaction);
		}
	}
	/*
	protected void fireEvents(CASServerConnection connection)
	{
		ArrayList<IndelibleEvent> events = connection.getLogForSource(id);
		for (IndelibleEvent curEvent:events)
		{
			synchronized(connections)
			{
				for (DBCASCollectionConnection collectionConnection:connections)
					collectionConnection.fireIndelibleEvent(curEvent);
			}
		}
	}
	*/
	protected void fireReplicatedTransactionEvent(DBCASServerConnection connection, IndelibleFSTransaction committedTransaction, TransactionCommittedEvent event)
	{
		((DBCASServer)connection.getServer()).logReplicatedEventToDB(connection, event, internalCollectionID, committedTransaction);
		synchronized(connections)
		{
			for (DBCASCollectionConnection collectionConnection:connections)
				collectionConnection.fireIndelibleEvent(event);
		}
	}
	
	protected void fireTransactionEvent(DBCASServerConnection connection, IndelibleFSTransaction committedTransaction)
	{
		TransactionCommittedEvent event = new TransactionCommittedEvent(committedTransaction, connection.getServerID());
		setEventIDAndTimestamp(event);
		((DBCASServer)connection.getServer()).logCollectionEventToDB(connection, event, internalCollectionID, committedTransaction);
		synchronized(connections)
		{
			for (DBCASCollectionConnection collectionConnection:connections)
				collectionConnection.fireIndelibleEvent(event);
		}
	}
	protected int getInternalCollectionID()
	{
		return internalCollectionID;
	}
	
	protected void addConnection(DBCASCollectionConnection connectionToAdd)
	{
		synchronized(connections)
		{
			connections.add(connectionToAdd);
		}
	}
	
	protected void removeConnection(DBCASCollectionConnection connectionToRemove)
	{
		synchronized(connections)
		{
			connections.remove(connectionToRemove);
		}
	}

	public long getLastReplicatedEventIF(DBCASCollectionConnection connection, EntityID sourceServerID, CASCollectionID collectionID) throws IOException
	{
		return connection.getServerConnection().getLastReplicatedEvent(sourceServerID, collectionID);
	}

	public void startTransaction(DBCASCollectionConnection connection) throws IOException
	{
		connection.getServerConnection().startTransaction();
	}

	public IndelibleFSTransaction commit(DBCASCollectionConnection connection) throws IOException
	{
		return connection.getServerConnection().commit();
	}

	public void rollback(DBCASCollectionConnection connection)
	{
		connection.getServerConnection().rollback();
	}

	public void startReplicatedTransaction(DBCASCollectionConnection connection, TransactionCommittedEvent transaction) throws IOException
	{
		connection.getServerConnection().startReplicatedTransaction(transaction);
	}

	public CASIDDataDescriptor getMetaDataForReplication(DBCASCollectionConnection connection) throws IOException
	{
		return retrieveMetaData(connection);
	}

	public void replicateMetaDataResource(DBCASCollectionConnection connection,
			CASIDDataDescriptor metaDataDescriptor,
			CASCollectionEvent replicateEvent) throws IOException
	{
		if (replicateEvent.getEventType() != CASCollectionEventType.kMetadataModified)
			throw new IllegalArgumentException("Event is not a metadata modified event");
		if (replicateEvent.getEventID() < 0 || replicateEvent.getTimestamp() < 0)
			throw new IllegalArgumentException("Replicated events must have event id and timestamp set");
    	connection.getServerConnection().storeMetaData(metaDataDescriptor, internalCollectionID);
    	connection.logEvent(replicateEvent);
	}

	public CASSegmentIDIterator listSegments(DBCASCollectionConnection connection)
	throws IOException
	{
		return connection.getServerConnection().listSegmentIDs(internalCollectionID);
	}

	public IndelibleVersionIterator listVersionsForSegment(DBCASCollectionConnection connection, ObjectID id) throws IOException
	{
		return connection.getServerConnection().listVersionsForSegment(id, internalCollectionID);
	}
}
