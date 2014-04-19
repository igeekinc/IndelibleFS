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

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFileNode;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSObjectRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFileNodeRemote;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;

public class IndelibleFileNodeRemoteImpl extends IndelibleFSObjectRemoteImpl
        implements IndelibleFileNodeRemote
{
    private static final long serialVersionUID = -482558858219794219L;

    public IndelibleFileNodeRemoteImpl(IndelibleFileNodeIF coreObject, IndelibleFSServerConnectionImpl connection)
            throws RemoteException
    {
        super(coreObject, connection);
    }

    public IndelibleFileNodeRemoteImpl(int port) throws RemoteException
    {
        super(port);
        // TODO Auto-generated constructor stub
    }

    public IndelibleFileNodeRemoteImpl(int port, RMIClientSocketFactory csf,
            RMIServerSocketFactory ssf) throws RemoteException
    {
        super(port, csf, ssf);
        // TODO Auto-generated constructor stub
    }

    public IndelibleFSForkRemote getFork(String name, boolean createIfNecessary)
            throws IOException, ForkNotFoundException
    {
        IndelibleFSForkIF localFork = ((IndelibleFileNode)coreObject).getFork(name, createIfNecessary);
        if (localFork != null)
            return new IndelibleFSForkRemoteImpl(localFork, connection);
        return null;
    }

    @Override
	public void deleteFork(String forkName) throws RemoteException, IOException, ForkNotFoundException, PermissionDeniedException
	{
    	((IndelibleFileNode)coreObject).deleteFork(forkName);
	}

	public int getReferenceCount()
    {
        return ((IndelibleFileNode)coreObject).getReferenceCount();
    }

    public boolean isDirectory()
    {
        return ((IndelibleFileNode)coreObject).isDirectory();
    }

    public boolean isFile()
    {
        return ((IndelibleFileNode)coreObject).isFile();
    }

    public long lastModified()
    {
        return ((IndelibleFileNode)coreObject).lastModified();
    }

    public long totalLength()
    {
        return ((IndelibleFileNode)coreObject).totalLength();
    }

    public long lengthWithChildren()
    {
        return ((IndelibleFileNode)coreObject).lengthWithChildren();
    }
    
    public String[] listForkNames() throws RemoteException
    {
        return ((IndelibleFileNode)coreObject).listForkNames();
    }

    public IndelibleFSObjectID getVolumeID() throws RemoteException
    {
        return ((IndelibleFileNode)coreObject).getVolume().getVolumeID();
    }
    
	@Override
	public IndelibleFSObjectRemote getObjectForVersion(IndelibleVersion version,
			RetrieveVersionFlags flags) throws RemoteException, IOException
	{
		IndelibleFileNode objectForVersion = ((IndelibleFileNode)coreObject).getVersion(version, flags);
		if (objectForVersion != null)
			return newObject(objectForVersion, connection);
		else
			return null;
	}

	@Override
	public IndelibleFileNodeRemoteImpl newObject(IndelibleFSObjectIF object,
			IndelibleFSServerConnectionImpl connection) throws RemoteException
	{
		return new IndelibleFileNodeRemoteImpl((IndelibleFileNodeIF)object, connection);
	}
}
