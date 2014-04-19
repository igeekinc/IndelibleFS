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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.events.RemoteIndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.events.RemoteIndelibleEventIteratorImpl;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastServerRef2;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.server.RemoteCASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.RemoteCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.RemoteCASServerConnection;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.logging.ErrorLogMessage;

public class RemoteCASServerConnectionImpl extends SSLUnicastObject implements RemoteCASServerConnection
{
	private static final long serialVersionUID = -5088105650916176363L;
	private RemoteCASServerImpl server;
    private CASServerConnection localConnection;
    private Logger logger = Logger.getLogger(getClass());
    
    public RemoteCASServerConnectionImpl(RemoteCASServerImpl server, CASServerConnection localConnection)  throws RemoteException
    {
    	super(0, server.getClientSocketFactory(), server.getServerSocketFactory());
    	this.server = server;
        this.localConnection = localConnection;
    }
    
	@Override
	public void close() throws RemoteException
	{
		localConnection.close();
	}

	@Override
	public boolean isClosed() throws RemoteException
	{
		return localConnection.isClosed();
	}

	@Override
	public RemoteCASCollectionConnection getCollection(CASCollectionID id)
			throws CollectionNotFoundException, RemoteException
	{
		CASCollectionConnection localCollection = localConnection.getCollectionConnection(id);
		return new RemoteCASCollectionConnectionImpl(server, localConnection, localCollection, getClientSocketFactory(), getServerSocketFactory());
	}

	@Override
	public RemoteCASCollectionConnection createNewCollection() throws RemoteException, IOException
	{
		CASCollectionConnection localCollection = localConnection.createNewCollection();
		return new RemoteCASCollectionConnectionImpl(server, localConnection, localCollection, getClientSocketFactory(), getServerSocketFactory());
	}

	@Override
	public RemoteCASCollectionConnection addCollection(
			CASCollectionID addCollectionID) throws RemoteException, IOException
	{
		CASCollectionConnection localCollection = localConnection.addCollection(addCollectionID);
		return new RemoteCASCollectionConnectionImpl(server, localConnection, localCollection, getClientSocketFactory(), getServerSocketFactory());
	}

	@Override
	public EntityID getServerID() throws RemoteException
	{
		return localConnection.getServerID();
	}

	@Override
	public CASCollectionID[] listCollections() throws RemoteException
	{
		return localConnection.listCollections();
	}

	@Override
	public NetworkDataDescriptor retrieveMetaData(String name) throws IOException, RemoteException
	{
		DataDescriptor localDescriptor = localConnection.retrieveMetaData(name);
		return localConnection.getDataMoverSession().registerDataDescriptor(localDescriptor);
	}

	@Override
	public void storeMetaData(String name, NetworkDataDescriptor metaData) throws IOException, RemoteException
	{
		localConnection.storeMetaData(name, metaData);
	}

	@Override
	public IndelibleVersion startTransaction() throws IOException, RemoteException
	{
		return localConnection.startTransaction();
	}

	@Override
	public IndelibleFSTransaction commit() throws IOException, RemoteException
	{
		return localConnection.commit();
	}

	@Override
	public void rollback() throws RemoteException
	{
		localConnection.rollback();
	}

	@Override
	public RemoteCASServer getServer() throws RemoteException
	{
		return server;
	}
    public EntityID getSecurityServerID() throws RemoteException
    {
        return server.getSecurityServerID();
    }
    
    public EntityID getMoverID()
    {
        return DataMoverReceiver.getDataMoverReceiver().getEntityID();
    }

	@Override
	public EntityAuthentication getClientEntityAuthentication()
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
	public EntityAuthentication getServerEntityAuthentication()
			throws RemoteException
	{
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getServerEntityAuthenticationForThread();
        }
        return authenticatedID;
	}

	@Override
	public void addConnectedServer(EntityID serverID, EntityID securityServerID)
	throws RemoteException
	{
		localConnection.addConnectedServer(serverID, securityServerID);
	}

	@Override
	public void addClientSessionAuthentication(SessionAuthentication sessionAuthenticationToAdd) throws RemoteException
	{
		localConnection.addClientSessionAuthentication(sessionAuthenticationToAdd);
	}

	@Override
	public SessionAuthentication getSessionAuthentication()
			throws RemoteException
	{
		return localConnection.getSessionAuthentication();
	}

	@Override
	public RemoteIndelibleEventIterator getServerEventsAfterEventID(
			long eventID, int timeToWait) throws IOException, RemoteException
	{
		return new RemoteIndelibleEventIteratorImpl(localConnection.getServerEventsAfterEventID(eventID, timeToWait));
	}

	@Override
	public long getLastEventID() throws RemoteException
	{
		return localConnection.getLastEventID();
	}

	@Override
	public long getLastReplicatedEventID(EntityID sourceServerID,
			CASCollectionID collectionID) throws IOException, RemoteException
	{
		return localConnection.getLastReplicatedEventID(sourceServerID, collectionID);
	}

	@Override
	public RemoteIndelibleEventIterator eventsAfterID(long startingEventID)
			throws RemoteException
	{
		return new RemoteIndelibleEventIteratorImpl(localConnection.eventsAfterID(startingEventID));
	}

	@Override
	public RemoteIndelibleEventIterator eventsAfterTime(long timestamp)
			throws RemoteException
	{
		return new RemoteIndelibleEventIteratorImpl(localConnection.eventsAfterTime(timestamp));
	}

	@Override
	public void createStoresForProperties(Properties initDBFSStoresProperties)
			throws IOException, RemoteException
	{
		localConnection.getServer().getCASStoreManager().createStoresForProperties(initDBFSStoresProperties);
	}

	@Override
	public InetSocketAddress[] getMoverAddresses(EntityID securityServerID)
			throws RemoteException
	{
		return DataMoverSource.getDataMoverSource().getHostPorts(securityServerID);
	}

	@Override
	public void testReverseConnection(NetworkDataDescriptor testNetworkDescriptor) throws IOException,
			RemoteException
	{
		logger.error(new ErrorLogMessage("Starting testReverseConnection"));
		if (testNetworkDescriptor.getHostPorts().length > 0)
		{
			for (InetSocketAddress curHostPort:testNetworkDescriptor.getHostPorts())
			{
				logger.error(new ErrorLogMessage("testReverseConnection addr:{0}:{1}", new Object[]{
						curHostPort.getAddress(), curHostPort.getPort()
				}));
			}
		}
		else
		{
			logger.error(new ErrorLogMessage("testReverseConnection local only descriptor"));
		}
		testNetworkDescriptor.getData();
		logger.error(new ErrorLogMessage("testReverseConnection finished successfully"));
	}

	@Override
	public NetworkDataDescriptor getTestDescriptor() throws RemoteException
	{
		byte [] testBuffer = new byte[32*1024];
		CASIDDataDescriptor testDescriptor = new CASIDMemoryDataDescriptor(testBuffer);
		NetworkDataDescriptor testNetworkDescriptor = localConnection.getDataMoverSession().registerDataDescriptor(testDescriptor);
		return testNetworkDescriptor;
	}

	@Override
	public void setupReverseMoverConnection(EntityID securityServerID,
			InetAddress connectToAddress, int connectToPort)
			throws IOException, RemoteException, AuthenticationFailureException
	{
		DataMoverSource.getDataMoverSource().openReverseConnection(securityServerID, connectToAddress, connectToPort);
	}
}
