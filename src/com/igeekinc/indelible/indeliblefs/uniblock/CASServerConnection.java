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
 
package com.igeekinc.indelible.indeliblefs.uniblock;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSession;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventListener;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventSupport;
import com.igeekinc.indelible.indeliblefs.events.MultipleQueueDispatcher;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.CASServerEventsAfterEventIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.CASServerEventsAfterTimeIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.WaitForEventsListener;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.logging.ErrorLogMessage;

public class CASServerConnection implements CASServerConnectionIF
{
    protected CASServerInternal server;
    protected IndelibleFSTransaction transaction;
    protected DataMoverSession dataMoverSession;
    private boolean inTransaction;
    private HashMap<ObjectID, ArrayList<CASCollectionEvent>> collectionEventLogsForTransaction;
    private ArrayList<CASServerEvent>serverEventLogForTransaction;
    private HashMap<CASCollectionID, CASCollectionConnection>connectionCollections;
    private SessionAuthentication sessionAuthentication;
    private IndelibleEventSupport eventSupport;
    private IndelibleVersion versionForTransaction;
	private MultipleQueueDispatcher	dispatcher	= new MultipleQueueDispatcher();
    
    protected CASServerConnection(CASServerInternal<? extends CASServerConnectionIF> server, EntityID openingEntity)
    {
        this.server = server;
        collectionEventLogsForTransaction = new HashMap<ObjectID, ArrayList<CASCollectionEvent>>();
        serverEventLogForTransaction = new ArrayList<CASServerEvent>();
        connectionCollections = new HashMap<CASCollectionID, CASCollectionConnection>();
        dataMoverSession = DataMoverSource.getDataMoverSource().createDataMoverSession(server.getSecurityServerID());
        eventSupport = new IndelibleEventSupport(this, dispatcher);
    }
    
    protected CASServerConnection(CASServerInternal<? extends CASServerConnectionIF> server, EntityAuthentication openingEntity)
    {
        this.server = server;
        collectionEventLogsForTransaction = new HashMap<ObjectID, ArrayList<CASCollectionEvent>>();
        serverEventLogForTransaction = new ArrayList<CASServerEvent>();
        connectionCollections = new HashMap<CASCollectionID, CASCollectionConnection>();
        dataMoverSession = DataMoverSource.getDataMoverSource().createDataMoverSession(server.getSecurityServerID());
        eventSupport = new IndelibleEventSupport(this, dispatcher);
        try
        {
        	sessionAuthentication = dataMoverSession.addAuthorizedClient(openingEntity);
        }
        catch (Throwable t)
        {
        	throw new IllegalArgumentException("Could not authorize "+openingEntity);
        }
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#close()
	 */
    @Override
	public synchronized void close()
    {
    	if (inTransaction)
    	{
    		rollback();
    		inTransaction = false;
    	}
        if (server != null)
        {
            server.close(this);
            server = null;
        }
    }
    
    public boolean isClosed()
    {
    	return server==null;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#getCollectionConnection(com.igeekinc.indelible.oid.CASCollectionID)
	 */
    @Override
	public synchronized CASCollectionConnection openCollectionConnection(CASCollectionID id) throws CollectionNotFoundException
    {
    	CASCollectionConnection returnCollection = connectionCollections.get(id);
    	if (returnCollection == null)
    	{
    		returnCollection = server.openCollection((CASServerConnectionIF)this, id);
    		connectionCollections.put(id,  returnCollection);
    	}
    	return returnCollection;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#createNewCollection()
	 */
    @Override
	public synchronized CASCollectionConnection createNewCollection() throws IOException
    {
    	CASCollectionConnection returnConnection = server.createNewCollection(this);
    	connectionCollections.put(returnConnection.getCollectionID(),  returnConnection);
    	return returnConnection;
    }
    
    
    @Override
	public CASCollectionConnection addCollection(CASCollectionID addCollectionID) throws IOException
	{
    	CASCollectionConnection returnConnection = server.addCollection(this, addCollectionID);
    	synchronized(this)
    	{
    		connectionCollections.put(returnConnection.getCollectionID(),  returnConnection);
    	}
    	return returnConnection;
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#getServerID()
	 */
    @Override
	public EntityID getServerID()
    {
        try
		{
			return server.getServerID();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			return null;
		}
    }
    
	@Override
	public EntityID getSecurityServerID()
	{
		return server.getSecurityServerID();
	}
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#listCollections()
	 */
    @Override
	public synchronized CASCollectionID [] listCollections()
    {
        return server.listCollections(this);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#retrieveMetaData(java.lang.String)
	 */
    @Override
	public synchronized DataDescriptor retrieveMetaData(String name) throws IOException
    {
        return server.retrieveMetaData(this, name);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#storeMetaData(java.lang.String, com.igeekinc.util.datadescriptor.DataDescriptor)
	 */
    @Override
	public synchronized void storeMetaData(String name, DataDescriptor metaData) throws IOException
    {
        server.storeMetaData(this, name, metaData);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#startTransaction()
	 */
    @Override
	public synchronized IndelibleVersion startTransaction() throws IOException
    {
    	if (inTransaction)
    		throw new UnsupportedOperationException("Can't nest transactions");	// Can't nest transactions
    	if (collectionEventLogsForTransaction.size() > 0)
    		throw new UnsupportedOperationException("outstanding events from non-transaction");
        versionForTransaction = server.startTransaction(this);
        inTransaction = true;
        return versionForTransaction;
    }
    
	public synchronized void startReplicatedTransaction(TransactionCommittedEvent transaction) throws IOException
	{
    	if (inTransaction)
    		throw new UnsupportedOperationException("Can't nest transactions");	// Can't nest transactions
    	if (collectionEventLogsForTransaction.size() > 0)
    		throw new UnsupportedOperationException("outstanding events from non-transaction");
        versionForTransaction = server.startReplicatedTransaction(this, transaction);
        inTransaction = true;
	}
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#commit()
	 */
    @Override
	public synchronized IndelibleFSTransaction commit() throws IOException
    {
    	try
    	{
    		// TODO - make sure there are no outstanding async transactions
    		IndelibleFSTransaction returnTransaction = server.commit(this);
    		return returnTransaction;
    	}
    	finally
    	{
            // Clear all events - we should have logged them by now
            collectionEventLogsForTransaction = new HashMap<ObjectID, ArrayList<CASCollectionEvent>>();
            inTransaction = false;
    		versionForTransaction = null;
    	}
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#rollback()
	 */
    @Override
	public synchronized void rollback()
    {
        server.rollback(this);
        // Dump all of the events
        collectionEventLogsForTransaction = new HashMap<ObjectID, ArrayList<CASCollectionEvent>>();
        inTransaction = false;
    }
    
    public DataMoverSession getDataMoverSession()
    {
        return dataMoverSession;
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF#getServer()
	 */
    @Override
	public CASServerInternal getServer()
    {
        return server;
    }
    
    public void logEvent(ObjectID source, IndelibleEvent event) throws IOException
    {
    	if (event instanceof CASCollectionEvent)
    	{
    		CASCollectionEvent casEvent = (CASCollectionEvent)event;
    		if (inTransaction)
    		{
    			if (transaction != null)
    			{
    				if (event.getEventID() < 0 || event.getTimestamp() <= 0)
    					throw new IllegalArgumentException("Can't execute non-replicated events during a replicated transaction");
    			}
    			ArrayList<CASCollectionEvent>log;
    			synchronized(collectionEventLogsForTransaction)
    			{
    				log = collectionEventLogsForTransaction.get(source);
    				if (log == null)
    				{
    					log = new ArrayList<CASCollectionEvent>();
    					collectionEventLogsForTransaction.put(source, log);
    				}
    			}

    			synchronized(log)
    			{
    				log.add(casEvent);
    			}
    		}
    		else
    		{
    			if (transaction != null)
    				throw new InternalError("Replicated transaction found but not in transaction");
    			server.logCASCollectionEvent(this, source, casEvent, null);
    		}
    	}
    	if (event instanceof CASServerEvent)
    	{
    		CASServerEvent serverEvent = (CASServerEvent)event;
    		if (inTransaction)
    		{
    			synchronized(serverEventLogForTransaction)
    			{
    				serverEventLogForTransaction.add(serverEvent);
    			}
    		}
    		else
    		{
    			server.logServerEvent(this, source, serverEvent, null);
    		}
    	}
    }
    
    public void logEventToServer(ObjectID source, IndelibleEvent event, IndelibleFSTransaction transaction) throws IOException
    {
    	if (event instanceof CASCollectionEvent)
    	{
    		CASCollectionEvent casEvent = (CASCollectionEvent)event;
    		server.logCASCollectionEvent(this, source, casEvent, transaction);
    	}
    }
    

	public void logReplicatedEventToServer(ObjectID source, IndelibleEvent event, IndelibleFSTransaction transaction)
	{
    	if (event instanceof CASCollectionEvent)
    	{
    		CASCollectionEvent casEvent = (CASCollectionEvent)event;
    		server.logReplicatedCASCollectionEvent(this, source, casEvent, transaction);
    	}
	}
    public ObjectID [] listEventSources()
    {
    	synchronized(collectionEventLogsForTransaction)
    	{
    		Set<ObjectID> eventSourcesSet = collectionEventLogsForTransaction.keySet();
    		ObjectID [] returnIDs = new ObjectID[eventSourcesSet.size()];
    		returnIDs = eventSourcesSet.toArray(returnIDs);
    		return returnIDs;
    	}
    }
    
    public synchronized ArrayList<CASCollectionEvent> getLogForSource(ObjectID source)
    {
    	return collectionEventLogsForTransaction.get(source);
    }
    
    public synchronized ArrayList<CASServerEvent> getServerLog()
    {
    	return serverEventLogForTransaction;
    }
    
    public synchronized void addConnectedServer(EntityID serverID, EntityID securityServerID)
    {
    	server.addConnectedServer(this, serverID, securityServerID);
    }

	@Override
	public void addClientSessionAuthentication(SessionAuthentication sessionAuthenticationToAdd)
	{
		DataMoverReceiver.getDataMoverReceiver().addSessionAuthentication(sessionAuthenticationToAdd);
	}

	@Override
	public SessionAuthentication getSessionAuthentication()
	{
		return sessionAuthentication;
	}
	
	@Override
	public IndelibleEventIterator eventsAfterID(long startingEventID)
	{
		return new CASServerEventsAfterEventIDIterator(this, startingEventID);
	}

	@Override
	public IndelibleEventIterator eventsAfterTime(long timestamp)
	{
		return new CASServerEventsAfterTimeIterator(this, timestamp);
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

	@Override
	public long getLastEventID()
	{
		return server.getLastServerEventID(this);
	}

	@Override
	public long getLastReplicatedEventID(EntityID sourceServerID,
			CASCollectionID collectionID)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public IndelibleEventIterator getServerEventsAfterEventID(
			long eventID, int timeToWait) throws IOException
	{
		IndelibleEventIterator returnIterator = new CASServerEventsAfterEventIDIterator(this, eventID);
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
			returnIterator = new CASServerEventsAfterEventIDIterator(this, eventID);
		}
		return returnIterator;
	}
	
	protected void fireIndelibleEvent(IndelibleEvent eventToFire)
	{
		eventSupport.fireIndelibleEvent(eventToFire);
	}
	
	public IndelibleVersion getVersionForTransaction()
	{
		return versionForTransaction;
	}
	
	/**
	 * This should only be used when replicating
	 * @param version
	 */
	public void setVersionForTransaction(IndelibleVersion version)
	{
		versionForTransaction = version;
	}

	@Override
	public void finalizeVersion(IndelibleVersion snapshotVersion)
	{
		server.finalizeVersion(snapshotVersion);
	}

	@Override
	public void prepareForDirectIO(CASServerConnectionIF receivingServer)
			throws IOException, RemoteException, AuthenticationFailureException
	{
		return;	// Well, we could do something here I suppose but this isn't really for local usage
	}

	public MultipleQueueDispatcher getDispatcher()
	{
		return dispatcher;
	}

	@Override
	public void deleteCollection(CASCollectionID id) throws CollectionNotFoundException, IOException
	{
		server.deleteCollection(this, id);
	}
}
