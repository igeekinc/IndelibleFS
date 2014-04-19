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
 
package com.igeekinc.indelible.indeliblefs.server;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManagerConnection;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSVolumeRemote;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastServerRef2;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;

public class IndelibleFSServerConnectionImpl extends SSLUnicastObject
        implements IndelibleFSServerConnectionRemote
{
    private static final long serialVersionUID = -5942619281932064696L;
    private IndelibleFSManagerConnection localConnection;
    
    // We retain hard references here to avoid object disappearing during the middle of long running transactions.  The client
    // is responsible for releasing objects as they are release/garbage collected on the client side
    private HashSet<UnicastRemoteObject>references = new HashSet<UnicastRemoteObject>();
    public IndelibleFSServerConnectionImpl(IndelibleFSManagerConnection localConnection, int port, IndelibleEntityAuthenticationClientRMIClientSocketFactory csf,
    		IndelibleEntityAuthenticationClientRMIServerSocketFactory ssf)  throws RemoteException
    {
    	super(port, csf, ssf);
        this.localConnection = localConnection;
    }
    

    public IndelibleFSVolumeRemote createVolume(Properties volumeProperties)
    throws IOException, RemoteException, PermissionDeniedException
    {
        IndelibleFSVolumeIF localVolume = localConnection.createVolume(volumeProperties);
        return (new IndelibleFSVolumeRemoteImpl(localVolume, this));
    }

    public IndelibleFSObjectID[] listVolumes() throws RemoteException, IOException
    {
        return localConnection.listVolumes();
    }

    public IndelibleFSVolumeRemote retrieveVolume(IndelibleFSObjectID retrieveVolumeID)
    throws VolumeNotFoundException, RemoteException, IOException
    {
        IndelibleFSVolumeIF localVolume = localConnection.retrieveVolume(retrieveVolumeID);
        return (new IndelibleFSVolumeRemoteImpl(localVolume, this));
    }
    
    public void startTransaction() throws RemoteException, IOException
    {
       localConnection.startTransaction();
    }

    @Override
	public boolean inTransaction() throws RemoteException, IOException
	{
		return localConnection.inTransaction();
	}


	public IndelibleVersion commit() throws IOException, RemoteException
    {
        return localConnection.commit();
    }
    
    @Override
	public IndelibleVersion commitAndSnapshot(HashMap<String, Serializable>snapshotMetadata) throws RemoteException,
			IOException, PermissionDeniedException
	{
		return localConnection.commitAndSnapshot(snapshotMetadata);
	}

	public void rollback() throws IOException, RemoteException
    {
        localConnection.rollback();
    }

    public void close() throws RemoteException, IOException
    {
        localConnection.close();
    }
    
    // This is for local use only - do not export to IndelibleFSServerConnection
    public NetworkDataDescriptor registerDataDescriptor(CASIDDataDescriptor descriptorToRegister)
    {
        return localConnection.registerDataDescriptor(descriptorToRegister);
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
	/**
	 * Add the session authentication from a client so that we can pull data from it
	 */
	public void addClientSessionAuthentication(SessionAuthentication sessionAuthenticationToAdd)
	{
		DataMoverReceiver.getDataMoverReceiver().addSessionAuthentication(sessionAuthenticationToAdd);
	}

	@Override
	public SessionAuthentication getSessionAuthentication() throws RemoteException
	{
		return localConnection.getSessionAuthentication();
	}

	/**
	 * Clean up if we get finalized
	 */
	@Override
	protected synchronized void finalize() throws Throwable
	{
		close();
		super.finalize();
	}
	
	synchronized void addReference(UnicastRemoteObject referenceObject)
	{
		references.add(referenceObject);
	}
	
	synchronized void removeReference(UnicastRemoteObject referenceObject)
	{
		references.remove(referenceObject);
	}
}
