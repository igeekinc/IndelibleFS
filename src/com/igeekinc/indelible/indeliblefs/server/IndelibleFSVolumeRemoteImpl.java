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

import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.DeleteFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleSymlinkNodeIF;
import com.igeekinc.indelible.indeliblefs.MoveObjectInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNode;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolume;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFileNode;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.CreateFileInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.DeleteFileInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleDirectoryNodeRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSVolumeRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFileNodeRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleSnapshotIteratorRemote;
import com.igeekinc.indelible.indeliblefs.remote.MoveObjectInfoRemote;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;

public class IndelibleFSVolumeRemoteImpl extends UnicastRemoteObject implements
        IndelibleFSVolumeRemote
{
    private static final long serialVersionUID = -2241130063704140242L;
    protected IndelibleFSVolumeIF localVolume;
    protected IndelibleFSServerConnectionImpl connection;
    public IndelibleFSVolumeRemoteImpl(IndelibleFSVolumeIF localVolume, IndelibleFSServerConnectionImpl connection) throws RemoteException
    {
    	super(0, connection.getClientSocketFactory(), connection.getServerSocketFactory());
        this.localVolume = localVolume;
        this.connection = connection;
        connection.addReference(this);
    }

    public IndelibleFSVolumeRemoteImpl(int port) throws RemoteException
    {
        super(port);
        // TODO Auto-generated constructor stub
    }

    public IndelibleFSVolumeRemoteImpl(int port, RMIClientSocketFactory csf,
            RMIServerSocketFactory ssf) throws RemoteException
    {
        super(port, csf, ssf);
        // TODO Auto-generated constructor stub
    }

    protected IndelibleFileNodeRemoteImpl getRemoteImplForFileNode(IndelibleFileNodeIF localObject) throws IOException
    {
    	IndelibleFileNodeRemoteImpl returnObject = null;
    	if (localObject instanceof IndelibleDirectoryNodeIF)
    		returnObject = new IndelibleDirectoryNodeRemoteImpl((IndelibleDirectoryNodeIF)localObject, connection);
    	else
    		if (localObject instanceof IndelibleSymlinkNodeIF)
    			returnObject = new IndelibleSymlinkNodeRemoteImpl((IndelibleSymlinkNodeIF)localObject, connection);
    		else
    			if (localObject instanceof IndelibleFileNodeIF)
    				returnObject = new IndelibleFileNodeRemoteImpl((IndelibleFileNode)localObject, connection);
    	if (returnObject == null)
    		throw new IllegalArgumentException("Unrecognized file node type "+localObject.getClass());
    	return returnObject;
    }
    
    public IndelibleFileNodeRemote getObjectByPath(FilePath path)
            throws ObjectNotFoundException, PermissionDeniedException,
            IOException
    {
    	if (!path.isAbsolute())
    		throw new IllegalArgumentException("Path "+path+" is not absolute");
        IndelibleFileNodeIF localObject = localVolume.getObjectByPath(path);
        IndelibleFileNodeRemoteImpl returnObject = null;
        if (localObject != null)
        {
        	returnObject = getRemoteImplForFileNode(localObject);
        }
        return returnObject;
    }
    
    public IndelibleFileNodeRemote getObjectByID(IndelibleFSObjectID id)
	throws RemoteException, IOException, ObjectNotFoundException
	{
    	// TODO - should check the security of the path to make sure that the requestor has permissions to access this node
        IndelibleFileNodeIF localObject = localVolume.getObjectByID(id);
        IndelibleFileNodeRemoteImpl returnObject = null;
        if (localObject != null)
        {
        	returnObject = getRemoteImplForFileNode(localObject);
        }
        return returnObject;
	}
    public IndelibleDirectoryNodeRemote getRoot() throws PermissionDeniedException, IOException,
            RemoteException
    {
        IndelibleDirectoryNodeIF localRoot  = localVolume.getRoot();
        IndelibleDirectoryNodeRemoteImpl returnRoot = null;
        if (localRoot != null)
            returnRoot = new IndelibleDirectoryNodeRemoteImpl(localRoot, connection);
        return returnRoot;
    }

    public IndelibleFSObjectID getVolumeID() throws RemoteException
    {
        return localVolume.getVolumeID();
    }

    public String getVolumeName() throws RemoteException,
            PermissionDeniedException, IOException
    {
        return localVolume.getVolumeName();
    }

    public void setVolumeName(String volumeName) throws RemoteException,
            PermissionDeniedException, IOException
    {
        localVolume.setVolumeName(volumeName);
    }

    public HashMap<String, Object> getMetaDataResource(String mdResourceName)
            throws RemoteException, PermissionDeniedException, IOException
    {
        return localVolume.getMetaDataResource(mdResourceName);
    }

    public IndelibleFSObjectID getObjectID() throws RemoteException
    {
        return localVolume.getObjectID();
    }

    public String[] listMetaDataResources() throws PermissionDeniedException,
            RemoteException, IOException
    {
        return localVolume.listMetaDataResources();
    }

    public IndelibleFSVolumeRemote setMetaDataResource(String mdResourceName,
            HashMap<String, Object> resources) throws RemoteException,
            PermissionDeniedException, IOException
    {
        return new IndelibleFSVolumeRemoteImpl(localVolume.setMetaDataResource(mdResourceName, resources), connection);
    }

	@Override
	public IndelibleVersionIterator listVersions() throws RemoteException,
			IOException
	{
		return localVolume.listVersions();
	}

	@Override
	public IndelibleFSVolumeRemote getObjectForVersion(IndelibleVersion version,
			RetrieveVersionFlags flags) throws RemoteException, IOException
	{
		IndelibleFSVolumeIF returnObject = localVolume.getVersion(version, flags);
		IndelibleFSVolumeRemote returnRemoteObject = null;
		if (returnObject != null)
			returnRemoteObject = new IndelibleFSVolumeRemoteImpl((IndelibleFSVolume)returnObject, connection);
		return returnRemoteObject;
	}

	@Override
	public IndelibleVersion getVersion() throws RemoteException
	{
		return localVolume.getVersion();
	}

	@Override
	public void addSnapshot(IndelibleSnapshotInfo snapshotInfo)
			throws PermissionDeniedException, IOException, RemoteException
	{
		localVolume.addSnapshot(snapshotInfo);
	}

	@Override
	public boolean releaseSnapshot(IndelibleVersion removeSnapshotVersion)
			throws PermissionDeniedException, IOException, RemoteException
	{
		return localVolume.releaseSnapshot(removeSnapshotVersion);
	}

	@Override
	public IndelibleSnapshotIteratorRemote listSnapshots()
			throws PermissionDeniedException, IOException, RemoteException
	{
		return new IndelibleSnapshotIteratorRemoteImpl(localVolume.listSnapshots());
	}

	@Override
	public IndelibleSnapshotInfo getInfoForSnapshot(
			IndelibleVersion retrieveSnapshotVersion)
			throws PermissionDeniedException, IOException, RemoteException
	{
		return localVolume.getInfoForSnapshot(retrieveSnapshotVersion);
	}

	@Override
	public void release() throws RemoteException
	{
		connection.removeReference(this);
	}

	@Override
	public MoveObjectInfoRemote moveObject(FilePath sourcePath,
			FilePath destinationPath) throws RemoteException, IOException,
			ObjectNotFoundException, PermissionDeniedException,
			FileExistsException, NotDirectoryException
	{
		MoveObjectInfo localInfo = localVolume.moveObject(sourcePath, destinationPath);
		DeleteFileInfo sourceLocalInfo = localInfo.getSourceInfo();
		IndelibleDirectoryNodeRemoteImpl newSourceDirectory = new IndelibleDirectoryNodeRemoteImpl(sourceLocalInfo.getDirectoryNode(), connection);
		DeleteFileInfoRemote sourceInfo = new DeleteFileInfoRemote(newSourceDirectory, sourceLocalInfo.deleteSucceeded());
		
		CreateFileInfo destLocalInfo = localInfo.getDestInfo();
		IndelibleDirectoryNodeRemoteImpl newDestDirectory = new IndelibleDirectoryNodeRemoteImpl(destLocalInfo.getDirectoryNode(), connection);
		IndelibleFileNodeRemoteImpl newDestFile;
		if (destLocalInfo.getCreatedNode().isDirectory())
		{
			newDestFile = new IndelibleDirectoryNodeRemoteImpl((IndelibleDirectoryNodeIF)destLocalInfo.getCreatedNode(), connection);
		}
		else
		{
			newDestFile = new IndelibleFileNodeRemoteImpl(destLocalInfo.getCreatedNode(), connection);

		}
		CreateFileInfoRemote destInfo = new CreateFileInfoRemote(newDestDirectory, newDestFile);
		MoveObjectInfoRemote returnInfo = new MoveObjectInfoRemote(sourceInfo, destInfo);
		return returnInfo;
	}

	@Override
	public DeleteFileInfoRemote deleteObjectByPath(FilePath deletePath)
			throws RemoteException, IOException, ObjectNotFoundException,
			PermissionDeniedException, NotDirectoryException
	{
        DeleteFileInfo deleteFileInfo = localVolume.deleteObjectByPath(deletePath);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory;
        if (deleteFileInfo.deleteSucceeded())
        {
        	newRemoteDirectory = new IndelibleDirectoryNodeRemoteImpl(deleteFileInfo.getDirectoryNode(), connection);
        }
        else
        {
        	newRemoteDirectory = null;
        }
		return new DeleteFileInfoRemote(newRemoteDirectory, deleteFileInfo.deleteSucceeded());
	}
}
