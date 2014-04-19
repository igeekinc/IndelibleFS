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

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotIterator;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleSnapshotIteratorRemote;

public class IndelibleSnapshotIteratorRemoteImpl extends UnicastRemoteObject implements
		IndelibleSnapshotIteratorRemote
{
	private static final long	serialVersionUID	= -5505938870428097424L;
	private IndelibleSnapshotIterator iterator;
	public IndelibleSnapshotIteratorRemoteImpl(IndelibleSnapshotIterator iterator) throws RemoteException
	{
		this.iterator = iterator;
	}
	
	@Override
	public IndelibleSnapshotInfo[] next(int maxToRetrieve)
			throws RemoteException
	{
		ArrayList<IndelibleSnapshotInfo>returnList = new ArrayList<IndelibleSnapshotInfo>();
		while (returnList.size() < maxToRetrieve && iterator.hasNext())
		{
			returnList.add(iterator.next());
		}
		IndelibleSnapshotInfo [] returnInfo = new IndelibleSnapshotInfo[returnList.size()];
		returnInfo = returnList.toArray(returnInfo);
		return returnInfo;
	}

}
