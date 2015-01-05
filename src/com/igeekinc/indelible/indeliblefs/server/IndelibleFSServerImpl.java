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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import sun.rmi.server.UnicastRef;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManager;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastServerRef2;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.RemoteCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.server.RemoteCASServerImpl;
import com.igeekinc.indelible.oid.EntityID;

public class IndelibleFSServerImpl extends SSLUnicastObject implements
        IndelibleFSServerRemote
{
	private static final long serialVersionUID = -1902873236322126174L;
	IndelibleFSManager localServer;
	Logger logger = Logger.getLogger(getClass());
    RemoteCASServerImpl casServer;
    public void setCASServer(RemoteCASServerImpl casServer)
    {
    	this.casServer = casServer;
    }

    public IndelibleFSServerImpl(IndelibleFSManager localServer, int port, IndelibleEntityAuthenticationClientRMIClientSocketFactory csf,
    		IndelibleEntityAuthenticationClientRMIServerSocketFactory ssf) throws RemoteException
    {
        super(port, csf, ssf);
        this.localServer = localServer;
    }

    
    public IndelibleFSServerConnectionRemote open() throws RemoteException
    {
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getClientEntityAuthenticationForThread();
        }
        IndelibleFSServerConnectionImpl returnConnection = new IndelibleFSServerConnectionImpl(localServer.open(authenticatedID),
        		0, getClientSocketFactory(), getServerSocketFactory());
        return returnConnection;
    }
    
    public EntityID getServerID()
    {
        return localServer.getServerID();
    }
    
    public EntityID getSecurityServerID()
    {
        return localServer.getSecurityServerID();
    }

	@Override
	public InetAddress getServerAddress() throws RemoteException 
	{
		try 
		{
			return InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new RemoteException("Could not get IP address");
		}
	}
	
	@Override
    public int getServerPort() throws RemoteException
    {
    	return ((UnicastRef)this.ref).getLiveRef().getPort();
    }

	@Override
	public RemoteCASServer getCASServer() throws RemoteException
	{
		return casServer;
	}

	
	@Override
	public void testReverseConnection(NetworkDataDescriptor testNetworkDescriptor) throws IOException
	{
		testNetworkDescriptor.getData();
	}

	@Override
	public InetSocketAddress [] getMoverAddresses(EntityID securityServerID) throws RemoteException
	{
		return DataMoverSource.getDataMoverSource().getListenNetworkAddresses(securityServerID);
	}
}
