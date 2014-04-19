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
 
package com.igeekinc.indelible.indeliblefs.uniblock.server;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.events.RemoteIndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.events.RemoteIndelibleEventIteratorImpl;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastServerRef2;
import com.igeekinc.indelible.indeliblefs.server.RemoteCASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.server.RemoteCASSegmentIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.AsyncStoreReplicatedSegmentCommandBlock;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.RemoteCASServer;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.util.async.RemoteAsyncCommandBlock;
import com.igeekinc.util.async.RemoteAsyncCompletionStatus;
import com.igeekinc.util.async.RemoteAsyncManager;

public class RemoteCASCollectionConnectionImpl extends SSLUnicastObject implements
		RemoteCASCollectionConnection
{
	private static final long serialVersionUID = -7835299625908360021L;
	RemoteCASServer server;
	CASServerConnection localConnection;
	CASCollectionConnection localCollectionConnection;
	private RemoteAsyncManager asyncManager = new RemoteAsyncManager();
	
	public RemoteCASCollectionConnectionImpl(RemoteCASServer server, CASServerConnection localConnection, CASCollectionConnection localCollection,
			IndelibleEntityAuthenticationClientRMIClientSocketFactory csf,
    		IndelibleEntityAuthenticationClientRMIServerSocketFactory ssf) throws RemoteException
	{
		super(0, csf, ssf);
		this.server = server;
		this.localConnection = localConnection;
		this.localCollectionConnection = localCollection;
	}
	
	@Override
	public CASCollectionID getID() throws RemoteException
	{
		return localCollectionConnection.getCollection().getID();
	}

	@Override
	public NetworkDataDescriptor retrieveSegment(CASIdentifier segmentID)
			throws IOException, RemoteException, SegmentNotFound
	{
		NetworkDataDescriptor returnNetworkDescriptor = null;
		CASIDDataDescriptor localDataDescriptor = localCollectionConnection.retrieveSegment(segmentID);
		if (localDataDescriptor != null)
			returnNetworkDescriptor = localConnection.getDataMoverSession().registerDataDescriptor(localDataDescriptor);
		return returnNetworkDescriptor;
	}

	@Override
	public DataVersionInfo retrieveSegment(ObjectID segmentID)
			throws IOException, RemoteException
	{
		NetworkDataDescriptor returnNetworkDescriptor = null;
		DataVersionInfo localDataVersionInfo = localCollectionConnection.retrieveSegment(segmentID);
		if (localDataVersionInfo != null)
			returnNetworkDescriptor = localConnection.getDataMoverSession().registerDataDescriptor(localDataVersionInfo.getDataDescriptor());
		return new DataVersionInfo(returnNetworkDescriptor, localDataVersionInfo.getVersion());
	}

	
	@Override
	public DataVersionInfo retrieveSegment(ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws RemoteException, IOException
	{
		NetworkDataDescriptor returnNetworkDescriptor = null;
		DataVersionInfo localDataVersionInfo = localCollectionConnection.retrieveSegment(segmentID, version, flags);
		if (localDataVersionInfo != null)
			returnNetworkDescriptor = localConnection.getDataMoverSession().registerDataDescriptor(localDataVersionInfo.getDataDescriptor());
		return new DataVersionInfo(returnNetworkDescriptor, localDataVersionInfo.getVersion());
	}
	
	

	@Override
	public SegmentInfo retrieveSegmentInfo(ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws IOException, RemoteException
	{
		return localCollectionConnection.retrieveSegmentInfo(segmentID, version, flags);
	}

	@Override
	public boolean verifySegment(ObjectID segmentID, IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException, RemoteException
	{
		return localCollectionConnection.verifySegment(segmentID, version, flags);
	}

	@Override
	public void repairSegment(ObjectID checkSegmentID, IndelibleVersion transactionVersion, DataVersionInfo masterData) throws IOException, RemoteException
	{
		localCollectionConnection.repairSegment(checkSegmentID, transactionVersion, masterData);
	}

	@Override
	public CASStoreInfo storeSegment(NetworkDataDescriptor segmentDescriptor) throws IOException, RemoteException
	{
		return localCollectionConnection.storeSegment(segmentDescriptor);
	}

	@Override
	public void storeVersionedSegment(ObjectID id, NetworkDataDescriptor segmentDescriptor)
			throws IOException, RemoteException
	{
		localCollectionConnection.storeVersionedSegment(id, segmentDescriptor);
	}

	
	@Override
	public void storeReplicatedSegment(ObjectID replicateSegmentID, IndelibleVersion version,
			NetworkDataDescriptor networkDescriptor,
			CASCollectionEvent curCASEvent) throws IOException, RemoteException
	{
		localCollectionConnection.storeReplicatedSegment(replicateSegmentID, version, networkDescriptor, curCASEvent);
	}

	@Override
	public boolean releaseSegment(CASSegmentID releaseID) throws IOException,
			RemoteException
	{
		return localCollectionConnection.releaseSegment(releaseID);
	}

	@Override
	public boolean[] bulkReleaseSegment(CASSegmentID[] releaseIDs)
			throws IOException
	{
		return localCollectionConnection.bulkReleaseSegment(releaseIDs);
	}

	@Override
	public HashMap<String, Serializable> getMetaDataResource(
			String mdResourceName) throws RemoteException,
			PermissionDeniedException, IOException
	{
		return localCollectionConnection.getMetaDataResource(mdResourceName);
	}

	@Override
	public void setMetaDataResource(String mdResourceName,
			HashMap<String, Serializable> resource) throws RemoteException,
			PermissionDeniedException, IOException
	{
		localCollectionConnection.setMetaDataResource(mdResourceName, resource);
	}

	@Override
	public RemoteCASServer getCASServer()
	{
		return server;
	}

	@Override
	public CASIdentifier retrieveCASIdentifier(CASSegmentID casSegmentID)
			throws IOException
	{
		return localCollectionConnection.retrieveCASIdentifier(casSegmentID);
	}

	@Override
	public long getLastEventID() throws RemoteException
	{
		return localCollectionConnection.getLastEventID();
	}

	@Override
	public EntityID getMoverID() throws RemoteException
	{
		return DataMoverReceiver.getDataMoverReceiver().getEntityID();
	}

	@Override
	public EntityAuthentication getEntityAuthentication()
			throws RemoteException
	{
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getClientEntityAuthenticationForThread();
        }
        return authenticatedID;
	}

	@Override
	public RemoteIndelibleEventIterator getEventsForTransaction(IndelibleFSTransaction transaction) throws RemoteException, IOException
	{
		return new RemoteIndelibleEventIteratorImpl(localCollectionConnection.getEventsForTransaction(transaction));
	}

	@Override
	public RemoteIndelibleEventIterator getTransactionEventsAfterEventID(long eventID, int timeToWait) throws RemoteException, IOException
	{
		return new RemoteIndelibleEventIteratorImpl(localCollectionConnection.getTransactionEventsAfterEventID(eventID, timeToWait));
	}

	@Override
	public RemoteIndelibleEventIterator eventsAfterID(long startingID)
			throws RemoteException
	{
		return new RemoteIndelibleEventIteratorImpl(localCollectionConnection.eventsAfterID(startingID));
	}

	@Override
	public RemoteIndelibleEventIterator eventsAfterTime(long timestamp)
			throws RemoteException
	{
		return new RemoteIndelibleEventIteratorImpl(localCollectionConnection.eventsAfterTime(timestamp));
	}

	@Override
	public long getLastReplicatedEventID(EntityID sourceServerID, CASCollectionID collectionID) throws RemoteException, IOException
	{
		return localCollectionConnection.getLastReplicatedEventID(sourceServerID, collectionID);
	}

	@Override
	public String[] listMetaDataNames() throws PermissionDeniedException, IOException, RemoteException
	{
		return localCollectionConnection.listMetaDataNames();
	}

	@Override
	public void startTransaction() throws IOException, RemoteException
	{
		localCollectionConnection.startTransaction();
	}

	@Override
	public IndelibleFSTransaction commit() throws IOException
	{
		asyncManager.waitAllCompleted();
		return localCollectionConnection.commit();
	}

	@Override
	public void rollback() throws IOException
	{
		localCollectionConnection.rollback();
	}

	@Override
	public void startReplicatedTransaction(TransactionCommittedEvent transactionEvent)
			throws IOException
	{
		localCollectionConnection.startReplicatedTransaction(transactionEvent);
	}

	@Override
	public CASIDDataDescriptor getMetaDataForReplication()
			throws RemoteException, IOException
	{
		return localCollectionConnection.getMetaDataForReplication();
	}

	@Override
	public void replicateMetaDataResource(
			CASIDDataDescriptor replicateMetaData,
			CASCollectionEvent curCASEvent) throws IOException, RemoteException
	{
		localCollectionConnection.replicateMetaDataResource(replicateMetaData, curCASEvent);
	}

	@Override
	public RemoteCASSegmentIDIterator listSegmentIDs() throws RemoteException, IOException
	{
		return new RemoteCASSegmentIDIteratorImpl(localCollectionConnection.listSegments(), getClientSocketFactory(), getServerSocketFactory());
	}

	@Override
	public RemoteAsyncCompletionStatus[] executeAsync(RemoteAsyncCommandBlock[] commandsToExecute,
			long timeToWaitForNewCompletionsInMS) throws RemoteException
	{
		for (RemoteAsyncCommandBlock curCommand:commandsToExecute)
		{
			((AsyncStoreReplicatedSegmentCommandBlock)curCommand).setConnection(localCollectionConnection);
		}
		return asyncManager.executeAsync(commandsToExecute, timeToWaitForNewCompletionsInMS);
	}
	
	
}
