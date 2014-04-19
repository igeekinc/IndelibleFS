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

import java.rmi.RemoteException;

import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastServerRef2;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerInternal;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.RemoteCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.remote.RemoteCASServerConnection;
import com.igeekinc.indelible.oid.EntityID;

public class RemoteCASServerImpl extends SSLUnicastObject implements RemoteCASServer
{
	private static final long serialVersionUID = 540793366744785657L;
	CASServerInternal localServer;

    public RemoteCASServerImpl(CASServerInternal localServer, int port, IndelibleEntityAuthenticationClientRMIClientSocketFactory csf,
    		IndelibleEntityAuthenticationClientRMIServerSocketFactory ssf) throws RemoteException
    {
        super(port, csf, ssf);
        this.localServer = localServer;
    }
	@Override
	public RemoteCASServerConnection open() throws RemoteException
	{
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getClientEntityAuthenticationForThread();
        }
        RemoteCASServerConnectionImpl returnConnection = new RemoteCASServerConnectionImpl(this, (CASServerConnection)localServer.open(authenticatedID));
        return returnConnection;
	}

	@Override
	public EntityID getServerID() throws RemoteException
	{
		return localServer.getServerID();
	}

	@Override
	public EntityID getSecurityServerID() throws RemoteException
	{
		return localServer.getSecurityServerID();
	}

}
