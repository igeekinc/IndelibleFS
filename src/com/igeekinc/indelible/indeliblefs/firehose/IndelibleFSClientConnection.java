/*
 * Copyright 2002-2014 iGeek, Inc.
 * All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.igeekinc.indelible.indeliblefs.firehose;

import java.io.IOException;
import java.util.HashMap;

import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleSymlinkNodeIF;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManagerConnection;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotIterator;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSObjectHandle.ObjectHandleType;

public class IndelibleFSClientConnection
{
	private long nextFSObjectHandle = 0x0200000000000000L |System.currentTimeMillis();
	private long nextFSForkHandle = 0x0300000000000000L|System.currentTimeMillis();
	private long nextVersionIteratorHandle = 0x0400000000000000L|System.currentTimeMillis();
	
	private HashMap<IndelibleFSObjectHandle, IndelibleFSObjectIF> fsObjectHandles = new HashMap<IndelibleFSObjectHandle, IndelibleFSObjectIF>();
	private HashMap<IndelibleFSForkHandle, IndelibleFSForkIF> fsForkHandles = new HashMap<IndelibleFSForkHandle, IndelibleFSForkIF>();
	private HashMap<IndelibleVersionIteratorHandle, IndelibleVersionIterator>versionIteratorHandles = new HashMap<IndelibleVersionIteratorHandle, IndelibleVersionIterator>();
	private HashMap<IndelibleSnapshotIteratorHandle, IndelibleSnapshotIterator>snapshotIteratorHandles = new HashMap<IndelibleSnapshotIteratorHandle, IndelibleSnapshotIterator>();
	private IndelibleFSServerConnectionHandle connectionHandle;
	private IndelibleFSManagerConnection managerConnection;
	
	public IndelibleFSClientConnection(IndelibleFSServerConnectionHandle connectionHandle, IndelibleFSManagerConnection managerConnection)
	{
		this.connectionHandle = connectionHandle;
		this.managerConnection = managerConnection;
	}

	public IndelibleFSServerConnectionHandle getConnectionHandle()
	{
		return connectionHandle;
	}

	public IndelibleFSManagerConnection getManagerConnection()
	{
		return managerConnection;
	}

	public IndelibleFSObjectHandle createHandle(IndelibleFSObjectIF object)
	{
		if (object instanceof IndelibleFSVolumeIF)
			return createHandle((IndelibleFSVolumeIF)object);
		if (object instanceof IndelibleFileNodeIF)
			return createHandle((IndelibleFileNodeIF)object);
		throw new InternalError("Can only call for volumes or files");
	}

	
	public IndelibleFSVolumeHandle createHandle(IndelibleFSVolumeIF volume)
	{
		synchronized(fsObjectHandles)
		{
			IndelibleFSVolumeHandle returnHandle = new IndelibleFSVolumeHandle(nextFSObjectHandle, volume.getObjectID());
			nextFSObjectHandle ++;
			fsObjectHandles.put(returnHandle, volume);
			return returnHandle;
		}
	}

	public IndelibleFSFileHandle createHandle(IndelibleFileNodeIF object)
	{
		synchronized(fsObjectHandles)
		{
			ObjectHandleType objectType = ObjectHandleType.kFile;
			if (object instanceof IndelibleDirectoryNodeIF)
				objectType = ObjectHandleType.kDirectory;
			if (object instanceof IndelibleSymlinkNodeIF)
				objectType = ObjectHandleType.kSymbolicLink;
			IndelibleFSFileHandle returnHandle = (IndelibleFSFileHandle) IndelibleFSObjectHandle.createObjectHandle(nextFSObjectHandle, object.getObjectID(), objectType);
			nextFSObjectHandle ++;
			fsObjectHandles.put(returnHandle, object);
			return returnHandle;
		}
	}
	
	public IndelibleVersionIteratorHandle createHandle(IndelibleVersionIterator iterator)
	{
		synchronized(versionIteratorHandles)
		{
			IndelibleVersionIteratorHandle returnHandle = new IndelibleVersionIteratorHandle(nextVersionIteratorHandle);
			nextVersionIteratorHandle ++;
			versionIteratorHandles.put(returnHandle, iterator);
			return returnHandle;
		}
	}
	
	public IndelibleVersionIterator getObjectForHandle(IndelibleVersionIteratorHandle handle)
	{
		synchronized(versionIteratorHandles)
		{
			return versionIteratorHandles.get(handle);
		}
	}
	
	public IndelibleSnapshotIteratorHandle createHandle(IndelibleSnapshotIterator iterator)
	{
		synchronized(snapshotIteratorHandles)
		{
			IndelibleSnapshotIteratorHandle returnHandle = new IndelibleSnapshotIteratorHandle(nextFSObjectHandle);
			nextFSObjectHandle ++;
			snapshotIteratorHandles.put(returnHandle, iterator);
			return returnHandle;
		}
	}
	
	public IndelibleSnapshotIterator getObjectForHandle(IndelibleSnapshotIteratorHandle handle)
	{
		synchronized(snapshotIteratorHandles)
		{
			return snapshotIteratorHandles.get(handle);
		}
	}
	
	public IndelibleVersionIterator removeObjectForHandle(IndelibleVersionIterator handle)
	{
		synchronized(versionIteratorHandles)
		{
			return versionIteratorHandles.remove(handle);
		}
	}

	
	private IndelibleFSObjectIF getObjectForHandleCore(IndelibleFSObjectHandle handle)
	{
		synchronized(fsObjectHandles)
		{
			return fsObjectHandles.get(handle);
		}
	}
	
	public IndelibleFSObjectIF getObjectForHandle(IndelibleFSObjectHandle handle)
	{
		return getObjectForHandleCore(handle);
	}
	
	public IndelibleFileNodeIF getObjectForHandle(IndelibleFSFileHandle handle)
	{
		return (IndelibleFileNodeIF)getObjectForHandleCore(handle);
	}
	
	public IndelibleDirectoryNodeIF getObjectForHandle(IndelibleFSDirectoryHandle handle)
	{
		return (IndelibleDirectoryNodeIF)getObjectForHandleCore(handle);
	}

	public IndelibleFSVolumeIF getObjectForHandle(IndelibleFSVolumeHandle handle)
	{
		return (IndelibleFSVolumeIF)getObjectForHandleCore(handle);
	}
	public IndelibleFSObjectIF removeObjectForHandle(IndelibleFSObjectHandle handle)
	{
		synchronized(fsObjectHandles)
		{
			return fsObjectHandles.remove(handle);
		}
	}

	public void close() throws IOException
	{
		managerConnection.close();
		fsObjectHandles.clear();
	}
	
	public IndelibleFSForkHandle createHandle(IndelibleFSForkIF fork)
	{
		synchronized(fsForkHandles)
		{
			IndelibleFSForkHandle returnHandle = new IndelibleFSForkHandle(nextFSForkHandle);
			nextFSForkHandle ++;
			fsForkHandles.put(returnHandle, fork);
			return returnHandle;
		}
	}
	
	public IndelibleFSForkIF getObjectForHandle(IndelibleFSForkHandle handle)
	{
		synchronized(fsForkHandles)
		{
			return fsForkHandles.get(handle);
		}
	}
	
	public IndelibleFSForkIF removeObjectForHandle(IndelibleFSForkHandle handle)
	{
		synchronized(fsForkHandles)
		{
			return fsForkHandles.remove(handle);
		}
	}
}
