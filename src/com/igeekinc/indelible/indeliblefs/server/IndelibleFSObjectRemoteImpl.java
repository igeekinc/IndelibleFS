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
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSObjectRemote;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;

public abstract class IndelibleFSObjectRemoteImpl extends UnicastRemoteObject implements
        IndelibleFSObjectRemote
{
    private static final long serialVersionUID = 4132685478462178149L;
    protected IndelibleFSObjectIF coreObject;
    protected IndelibleFSServerConnectionImpl connection;
    
    public IndelibleFSObjectRemoteImpl(IndelibleFSObjectIF coreObject, IndelibleFSServerConnectionImpl connection) throws RemoteException
    {
    	super(0, connection.getClientSocketFactory(), connection.getServerSocketFactory());
        if (coreObject == null)
            throw new IllegalArgumentException("Null coreObject not allowed");
        this.coreObject = coreObject;
        this.connection = connection;
        connection.addReference(this);
    }

    public IndelibleFSObjectRemoteImpl(int port) throws RemoteException
    {
        super(port);
        // TODO Auto-generated constructor stub
    }

    public IndelibleFSObjectRemoteImpl(int port, RMIClientSocketFactory csf,
            RMIServerSocketFactory ssf) throws RemoteException
    {
        super(port, csf, ssf);
        // TODO Auto-generated constructor stub
    }

    public Map<String, Object> getMetaDataResource(String mdResourceName)
            throws RemoteException, PermissionDeniedException, IOException
    {
        return coreObject.getMetaDataResource(mdResourceName);
    }
    
    public IndelibleFSObjectRemote setMetaDataResource(String mdResourceName, Map<String, Object> resource)
        throws RemoteException, PermissionDeniedException, IOException
    {
        IndelibleFSObjectIF newCoreObject = coreObject.setMetaDataResource(mdResourceName, resource);
        return newObject(newCoreObject, connection);
    }

    public IndelibleFSObjectID getObjectID() throws RemoteException
    {
        return coreObject.getObjectID();
    }

    public String[] listMetaDataResources() throws PermissionDeniedException, RemoteException,
            IOException
    {
        return coreObject.listMetaDataResources();
    }

	@Override
	public IndelibleVersionIterator listVersions() throws RemoteException,
			IOException
	{
		return coreObject.listVersions();
	}

	@Override
	public IndelibleFSObjectRemote getObjectForVersion(IndelibleVersion version,
			RetrieveVersionFlags flags) throws RemoteException, IOException
	{
		IndelibleFSObjectIF objectForVersion = coreObject.getVersion(version, flags);
		if (objectForVersion != null)
			return newObject(objectForVersion, connection);
		else
			return null;
	}

	@Override
	public IndelibleVersion getVersion() throws RemoteException
	{
		return coreObject.getCurrentVersion();
	}

	public abstract IndelibleFSObjectRemoteImpl newObject(IndelibleFSObjectIF object, IndelibleFSServerConnectionImpl connection) throws RemoteException;


	@Override
	public void release() throws RemoteException
	{
		connection.removeReference(this);
	}
}
