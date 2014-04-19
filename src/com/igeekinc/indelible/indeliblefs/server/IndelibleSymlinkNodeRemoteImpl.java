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

import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.IndelibleSymlinkNodeIF;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSymlinkNode;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleSymlinkNodeRemote;

public class IndelibleSymlinkNodeRemoteImpl extends IndelibleFileNodeRemoteImpl
		implements IndelibleSymlinkNodeRemote
{
	private static final long serialVersionUID = -1318071319603823123L;

	public IndelibleSymlinkNodeRemoteImpl(IndelibleSymlinkNodeIF coreObject,
			IndelibleFSServerConnectionImpl connection) throws RemoteException
	{
		super(coreObject, connection);
	}

	@Override
	public String getTargetPath() throws IOException, RemoteException
	{
		return ((IndelibleSymlinkNode)coreObject).getTargetPath();
	}

	@Override
	public IndelibleSymlinkNodeRemoteImpl newObject(IndelibleFSObjectIF object,
			IndelibleFSServerConnectionImpl connection) throws RemoteException
	{
		return new IndelibleSymlinkNodeRemoteImpl((IndelibleSymlinkNodeIF)object, connection);
	}

}
