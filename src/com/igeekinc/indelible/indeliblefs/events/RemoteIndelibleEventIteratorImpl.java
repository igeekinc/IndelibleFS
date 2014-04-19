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
 
package com.igeekinc.indelible.indeliblefs.events;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteIndelibleEventIteratorImpl extends UnicastRemoteObject
		implements RemoteIndelibleEventIterator
{
	private static final long	serialVersionUID	= 5368538353103675783L;
	private IndelibleEventIterator localIterator;
	
	public RemoteIndelibleEventIteratorImpl(IndelibleEventIterator localIterator) throws RemoteException
	{
		this.localIterator = localIterator;
	}
	@Override
	public boolean hasNext() throws RemoteException
	{
		return localIterator.hasNext();
	}

	@Override
	public IndelibleEvent next() throws RemoteException
	{
		return localIterator.next();
	}

	@Override
	public void remove() throws RemoteException
	{
		localIterator.remove();	// Should always fail but maybe we'll change it in the future
	}

	@Override
	public void close() throws RemoteException
	{
		localIterator.close();
	}
}
