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

import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.server.RemoteCASSegmentIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASSegmentIDIterator;
import com.igeekinc.indelible.oid.CASSegmentID;

public class RemoteCASSegmentIDIteratorImpl extends SSLUnicastObject implements
		RemoteCASSegmentIDIterator
{
	private static final long	serialVersionUID	= -2557057514212350011L;
	private CASSegmentIDIterator localIterator;
	public RemoteCASSegmentIDIteratorImpl(CASSegmentIDIterator localIterator,
			IndelibleEntityAuthenticationClientRMIClientSocketFactory csf,
    		IndelibleEntityAuthenticationClientRMIServerSocketFactory ssf) throws RemoteException
	{
		super(0, csf, ssf);

	}
	@Override
	public boolean hasNext() throws RemoteException
	{
		return localIterator.hasNext();
	}

	@Override
	public CASSegmentID next() throws RemoteException
	{
		return localIterator.next();
	}

	@Override
	public void remove() throws RemoteException
	{
		localIterator.remove();
	}

	@Override
	public void close() throws RemoteException
	{
		localIterator.close();
	}
}
