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
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventListener;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventSupport;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.uniblock.BasicDataDescriptorFactory;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.CASSegmentIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.logging.ErrorLogMessage;

class DataSegmentInfo
{
	private long dataID;
	private int storeNum;
	
	public DataSegmentInfo(int storeNum, long dataID)
	{
		this.storeNum = storeNum;
		this.dataID = dataID;
	}

	public long getDataID()
	{
		return dataID;
	}

	public int getStoreNum()
	{
		return storeNum;
	}
}

public class DBCASCollectionConnection extends BasicDataDescriptorFactory implements CASCollectionConnection
{
    private DBCASServerConnection serverConnection;
    private DBCASCollection collection;
	private IndelibleEventSupport eventSupport;
	
    DBCASCollectionConnection(DBCASCollection collection, DBCASServerConnection serverConnection)
    {
        this.collection = collection;
        this.serverConnection = serverConnection;
		eventSupport = new IndelibleEventSupport(this);
		collection.addConnection(this);
    }

    public DBCASCollection getCollection()
    {
    	return collection;
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.uniblock.CASCollection#retrieveSegment(com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier)
     */
    @Override
    public CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID)
    throws IOException, SegmentNotFound
    {
    	return collection.retrieveSegment(this, segmentID);
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.uniblock.CASCollection#storeSegment(com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier, com.igeekinc.indelible.indeliblefs.datamover.DataDescriptor)
     */
    @Override
    public CASStoreInfo storeSegment(CASIDDataDescriptor segmentDescriptor)
    throws IOException
    {
    	return collection.storeSegment(this, segmentDescriptor);
    }
    
	@Override
	public void storeVersionedSegment(ObjectID id, CASIDDataDescriptor segmentDescriptor)
	throws IOException
	{
		collection.storeVersionedSegment(this, id, segmentDescriptor);
	}

	
	@Override
	public void storeReplicatedSegment(ObjectID replicateSegmentID,
			IndelibleVersion replicateVersion,
			CASIDDataDescriptor sourceDescriptor, CASCollectionEvent curCASEvent)
			throws IOException
	{
		if (getServerConnection().getVersionForTransaction() != null && !replicateVersion.equals(getServerConnection().getVersionForTransaction()))
			throw new IllegalArgumentException("Cannot change version in middle of transaction");
		if (getServerConnection().getVersionForTransaction() == null)
			getServerConnection().setVersionForTransaction(replicateVersion);
		collection.storeReplicatedSegment(this, replicateSegmentID, replicateVersion, sourceDescriptor, curCASEvent);
	}

	@Override
	public Future<Void> storeReplicatedSegmentAsync(
			ObjectID replicateSegmentID, IndelibleVersion replicateVersion,
			CASIDDataDescriptor sourceDescriptor, CASCollectionEvent curCASEvent)
			throws IOException
	{
		if (getServerConnection().getVersionForTransaction() != null && !replicateVersion.equals(getServerConnection().getVersionForTransaction()))
			throw new IllegalArgumentException("Cannot change version in middle of transaction");
		if (getServerConnection().getVersionForTransaction() == null)
			getServerConnection().setVersionForTransaction(replicateVersion);
		return collection.storeReplicatedSegmentAsync(this, replicateSegmentID, replicateVersion, sourceDescriptor, curCASEvent);
	}

	@Override
	public <A> void storeReplicatedSegmentAsync(ObjectID replicateSegmentID,
			IndelibleVersion replicateVersion,
			CASIDDataDescriptor sourceDescriptor,
			CASCollectionEvent curCASEvent,
			AsyncCompletion<Void, ? super A> completionHandler, A attachment)
			throws IOException
	{
		if (getServerConnection().getVersionForTransaction() != null && !replicateVersion.equals(getServerConnection().getVersionForTransaction()))
			throw new IllegalArgumentException("Cannot change version in middle of transaction");
		if (getServerConnection().getVersionForTransaction() == null)
			getServerConnection().setVersionForTransaction(replicateVersion);
		collection.storeReplicatedSegmentAsync(this, replicateSegmentID, replicateVersion, sourceDescriptor, curCASEvent, completionHandler, attachment);
	}
	
	@Override
	public Future<CASStoreInfo> storeSegmentAsync(
			CASIDDataDescriptor sourceDescriptor) throws IOException
	{
		return collection.storeSegmentAsync(this, sourceDescriptor);
	}

	@Override
	public <A> void storeSegmentAsync(CASIDDataDescriptor sourceDescriptor,
			AsyncCompletion<CASStoreInfo, ? super A> completionHandler,
			A attachment) throws IOException
	{
		collection.storeSegmentAsync(this, sourceDescriptor, completionHandler, attachment);
	}

	@Override
    public CASIdentifier retrieveCASIdentifier(CASSegmentID segmentID) throws IOException
    {
    	return collection.retrieveCASIdentifier(this, segmentID);
    }
    
    @Override
	public DataVersionInfo retrieveSegment(ObjectID segmentID)
			throws IOException
	{
		return retrieveSegment(segmentID, IndelibleVersion.kLatestVersion, RetrieveVersionFlags.kNearest);
	}

	@Override
	public DataVersionInfo retrieveSegment(ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws IOException
	{
		return collection.retrieveSegment(this, segmentID, version, flags);
	}

	@Override
	public SegmentInfo retrieveSegmentInfo(ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws IOException
	{
		return collection.retrieveSegmentInfo(this, segmentID, version, flags);
	}

	@Override
	public boolean verifySegment(ObjectID segmentID, IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException
	{
		return collection.verifySegment(this, segmentID, version, flags);
	}

	@Override
	public void repairSegment(ObjectID repairSegmentID,
			IndelibleVersion transactionVersion, DataVersionInfo masterData)
			throws IOException
	{
		collection.repairSegment(this, repairSegmentID, transactionVersion, masterData);
	}

	@Override
	public boolean releaseSegment(CASSegmentID releaseID) throws IOException
	{
    	return collection.releaseSegment(this, releaseID);
	}
	
	@Override
	public boolean releaseVersionedSegment(ObjectID id, IndelibleVersion version)
			throws IOException
	{
		return collection.releaseVersionedSegment(this, id, version);
	}

	@Override
	public boolean[] bulkReleaseSegment(CASSegmentID[] releaseIDs)
			throws IOException
	{
		return collection.bulkReleaseSegment(this, releaseIDs);
	}

    
    @Override
	public HashMap<String, Serializable> getMetaDataResource(
			String mdResourceName) throws RemoteException,
			PermissionDeniedException, IOException
	{
    	return collection.getMetaDataResource(this, mdResourceName);
	}

	@Override
	public void setMetaDataResource(String mdResourceName,
			HashMap<String, Serializable> resource) throws RemoteException,
			PermissionDeniedException, IOException
	{
		collection.setMetaDataResource(this, mdResourceName, resource);
	}

    public CASServer getCASServer()
    {
        return serverConnection.getServer();
    }

    public long getLastEventID()
    {
    	return collection.getLastEventID();
    }

	protected DBCASServerConnection getServerConnection()
	{
		return serverConnection;
	}

	public void logEvent(CASCollectionEvent event) throws IOException
	{
		serverConnection.logEvent(collection.getID(), event);
	}
	
	@Override
	public IndelibleEventIterator eventsAfterID(long startingEventID)
	{
		return new CASCollectionEventsAfterEventIDIterator(this, startingEventID);
	}

	@Override
	public IndelibleEventIterator eventsAfterTime(long timestamp)
	{
		return new CASCollectionEventsAfterTimeIterator(this, timestamp);
	}

	@Override
	public void addListener(IndelibleEventListener listener)
	{
		eventSupport.addListener(listener);
	}

	@Override
	public void addListenerAfterID(IndelibleEventListener listener,
			long startingID)
	{
		eventSupport.addListenerAfterID(listener, startingID);
	}

	@Override
	public void addListenerAfterTime(IndelibleEventListener listener,
			long timestamp)
	{
		eventSupport.addListenerAfterTime(listener, timestamp);
	}
	
	protected void fireIndelibleEvent(IndelibleEvent fireEvent)
	{
		eventSupport.fireIndelibleEvent(fireEvent);
	}

	@Override
	public IndelibleEventIterator getEventsForTransaction(IndelibleFSTransaction transaction) throws IOException
	{
		return new CASCollectionEventsForTransactionIterator(this, transaction);
	}

	@Override
	public IndelibleEventIterator getTransactionEventsAfterEventID(long eventID, int timeToWait) throws IOException
	{
		IndelibleEventIterator returnIterator = new CASCollectionEventsAfterEventIDIterator(this, eventID);
		if (!returnIterator.hasNext())
		{
			returnIterator.close();
			WaitForEventsListener myListener = new WaitForEventsListener();
			eventSupport.addListenerAfterID(myListener, eventID);
			long timeToWakeup = System.currentTimeMillis() + timeToWait;
			while (!myListener.getEventsReceived() && System.currentTimeMillis() < timeToWakeup)
			{
				myListener.waitForEvents(timeToWait);
			}
			eventSupport.removeListener(myListener);
			returnIterator = new CASCollectionEventsAfterEventIDIterator(this, eventID);
		}
		return returnIterator;
	}

	@Override
	public long getLastReplicatedEventID(EntityID sourceServerID, CASCollectionID collectionID)
	{
		try
		{
			return collection.getLastReplicatedEventIF(this, sourceServerID, collectionID);
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			return -1;
		}
	}

	@Override
	public String[] listMetaDataNames() throws PermissionDeniedException,
			IOException
	{
		return collection.listMetaDataNames(this);
	}

	@Override
	public void startTransaction() throws IOException
	{
		collection.startTransaction(this);
	}

	@Override
	public IndelibleFSTransaction commit() throws IOException
	{
		return collection.commit(this);
	}

	@Override
	public void rollback() throws IOException
	{
		collection.rollback(this);
	}

	@Override
	public void startReplicatedTransaction(TransactionCommittedEvent transaction)
			throws IOException
	{
		collection.startReplicatedTransaction(this, transaction);
	}

	@Override
	public CASIDDataDescriptor getMetaDataForReplication() throws IOException
	{
		return collection.getMetaDataForReplication(this);
	}

	@Override
	public void replicateMetaDataResource(CASIDDataDescriptor metaDataDescriptor, CASCollectionEvent replicateEvent) throws IOException
	{
		collection.replicateMetaDataResource(this, metaDataDescriptor, replicateEvent);
	}
	
	public CASSegmentIDIterator listSegments() throws IOException
	{
		return collection.listSegments(this);
	}

	@Override
	public IndelibleVersionIterator listVersionsForSegment(ObjectID id)
			throws IOException
	{
		return collection.listVersionsForSegment(this, id);
	}

	@Override
	public IndelibleVersionIterator listVersionsForSegmentInRange(ObjectID id,
			IndelibleVersion first, IndelibleVersion last) throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<CASIDDataDescriptor> retrieveSegmentAsync(
			CASIdentifier segmentID) throws IOException, SegmentNotFound
	{
		//collection.retrieveSegmentA
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <A> void retrieveSegmentAsync(CASIdentifier segmentID,
			AsyncCompletion<CASIDDataDescriptor, A> completionHandler,
			A attachment) throws IOException, SegmentNotFound
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public Future<DataVersionInfo> retrieveSegmentAsync(ObjectID segmentID)
			throws IOException, SegmentNotFound
	{
		return retrieveSegmentAsync(segmentID, IndelibleVersion.kLatestVersion, RetrieveVersionFlags.kNearest);
	}

	@Override
	public <A> void retrieveSegmentAsync(ObjectID segmentID,
			AsyncCompletion<DataVersionInfo, A> completionHandler, A attachment)
			throws IOException, SegmentNotFound
	{
		retrieveSegmentAsync(segmentID, IndelibleVersion.kLatestVersion, RetrieveVersionFlags.kNearest, completionHandler, attachment);
	}
	
	@Override
	public Future<DataVersionInfo> retrieveSegmentAsync(ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws IOException, SegmentNotFound
	{
		ComboFutureBase<DataVersionInfo>returnFuture = new ComboFutureBase<DataVersionInfo>();
		retrieveSegmentAsync(segmentID, version, flags, returnFuture);
		return returnFuture;
	}

	@Override
	public <A> void retrieveSegmentAsync(ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags,
			AsyncCompletion<DataVersionInfo, A> completionHandler, A attachment)
			throws IOException, SegmentNotFound
	{
		ComboFutureBase<DataVersionInfo>returnFuture = new ComboFutureBase<DataVersionInfo>(completionHandler, attachment);
		retrieveSegmentAsync(segmentID, version, flags, returnFuture);
	}
	
	private void retrieveSegmentAsync(ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, ComboFutureBase<DataVersionInfo>future) throws IOException, SegmentNotFound
	{
		DataVersionInfo result = retrieveSegment(segmentID, version, flags);
		future.completed(result, null);
	}
}
