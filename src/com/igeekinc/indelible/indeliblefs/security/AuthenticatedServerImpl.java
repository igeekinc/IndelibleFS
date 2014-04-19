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
 
package com.igeekinc.indelible.indeliblefs.security;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import com.igeekinc.indelible.oid.EntityID;

public class AuthenticatedServerImpl<T extends SSLUnicastObject> extends UnicastRemoteObject implements
		AuthenticatedServer<T>
{
	private static final long serialVersionUID = 7220072475655519315L;
	private HashMap<EntityID, T>servers = new HashMap<EntityID, T>();
	public AuthenticatedServerImpl(int port, RMIClientSocketFactory csf, RMIServerSocketFactory ssf) throws RemoteException
	{
		super(port, csf, ssf);
	}

	@Override
	public EntityID[] listAuthenticationServers() throws RemoteException
	{
		EntityID [] returnKeys = new EntityID[servers.size()];
		returnKeys = servers.keySet().toArray(returnKeys);
		return returnKeys;
	}

	@Override
	public T getServerInstanceForAuthenticationServer(EntityID authenticationServerID) throws RemoteException
	{
		return servers.get(authenticationServerID);
	}

	public void addServer(EntityID authenticationServerID, T server)
	{
		servers.put(authenticationServerID, server);
	}
}
