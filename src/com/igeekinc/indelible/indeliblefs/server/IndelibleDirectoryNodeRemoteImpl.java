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
import java.util.HashMap;

import com.igeekinc.indelible.indeliblefs.CreateDirectoryInfo;
import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.CreateSymlinkInfo;
import com.igeekinc.indelible.indeliblefs.DeleteFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleNodeInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleSymlinkNodeIF;
import com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNode;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSObject;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolume;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFileNode;
import com.igeekinc.indelible.indeliblefs.exceptions.CannotDeleteDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotFileException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.CreateDirectoryInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.CreateFileInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.CreateSymlinkInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.DeleteFileInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleDirectoryNodeRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFileNodeRemote;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;

public class IndelibleDirectoryNodeRemoteImpl extends
        IndelibleFileNodeRemoteImpl implements IndelibleDirectoryNodeRemote
{
    private static final long serialVersionUID = 4772907644386896765L;

    public IndelibleDirectoryNodeRemoteImpl(IndelibleDirectoryNodeIF coreObject, IndelibleFSServerConnectionImpl connection)
            throws RemoteException
    {
        super(coreObject, connection);
    }

    @Override
    public CreateDirectoryInfoRemote createChildDirectory(String name)
            throws IOException, PermissionDeniedException, FileExistsException, RemoteException
    {
        CreateDirectoryInfo newDirectoryInfo = ((IndelibleDirectoryNode)coreObject).createChildDirectory(name);
        IndelibleDirectoryNodeIF newLocalFile = (IndelibleDirectoryNode)newDirectoryInfo.getCreatedNode();
        IndelibleDirectoryNodeIF newDirectory = newDirectoryInfo.getDirectoryNode();
        IndelibleDirectoryNodeRemoteImpl newRemoteFile = new IndelibleDirectoryNodeRemoteImpl(newLocalFile, connection);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory = new IndelibleDirectoryNodeRemoteImpl(newDirectory, connection);
        return new CreateDirectoryInfoRemote(newRemoteDirectory, newRemoteFile);
    }

    
    @Override
	public CreateSymlinkInfoRemote createChildSymlink(String name,
			String targetPath, boolean exclusive) throws IOException, PermissionDeniedException,
			FileExistsException, RemoteException, ObjectNotFoundException
	{
    	CreateSymlinkInfo newSymlinkInfo = ((IndelibleDirectoryNode)coreObject).createChildSymlink(name, targetPath, exclusive);
        IndelibleSymlinkNodeIF newLocalSymlink = newSymlinkInfo.getCreatedNode();
        IndelibleDirectoryNodeIF newDirectory = newSymlinkInfo.getDirectoryNode();
        IndelibleSymlinkNodeRemoteImpl newRemoteFile = new IndelibleSymlinkNodeRemoteImpl(newLocalSymlink, connection);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory = newObject(newDirectory, connection);
        return new CreateSymlinkInfoRemote(newRemoteDirectory, newRemoteFile);
	}

    @Override
	public CreateFileInfoRemote createChildLink(String name,
			IndelibleFSObjectID sourceFileID)
			throws IOException, PermissionDeniedException, FileExistsException,
			RemoteException, ObjectNotFoundException, NotFileException
	{
        IndelibleFSVolume ourVolume = ((IndelibleDirectoryNode)coreObject).getVolume();
        IndelibleFSObject sourceObject = ourVolume.getObjectByID(sourceFileID);
        if (sourceObject instanceof IndelibleFileNode)
        {
            if (sourceObject instanceof IndelibleDirectoryNode)
                throw new NotFileException();
        }
        else
        {
            throw new ObjectNotFoundException();
        }
        IndelibleFileNode sourceFileNode = (IndelibleFileNode)sourceObject;
        CreateFileInfo newFileInfo = ((IndelibleDirectoryNode)coreObject).createChildLink(name, sourceFileNode);
        IndelibleFileNodeIF newLocalFile = newFileInfo.getCreatedNode();
        IndelibleDirectoryNodeIF newDirectory = newFileInfo.getDirectoryNode();
        IndelibleFileNodeRemoteImpl newRemoteFile = super.newObject(newLocalFile, connection);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory = newObject(newDirectory, connection);
        return new CreateFileInfoRemote(newRemoteDirectory, newRemoteFile);
	}

	@Override
	public CreateFileInfoRemote createChildFile(String name, boolean exclusive)
            throws IOException, PermissionDeniedException, FileExistsException, RemoteException
    {
        CreateFileInfo newFileInfo = ((IndelibleDirectoryNode)coreObject).createChildFile(name, exclusive);
        IndelibleFileNodeIF newLocalFile = newFileInfo.getCreatedNode();
        IndelibleDirectoryNodeIF newDirectory = newFileInfo.getDirectoryNode();
        IndelibleFileNodeRemoteImpl newRemoteFile = super.newObject(newLocalFile, connection);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory = newObject(newDirectory, connection);
        return new CreateFileInfoRemote(newRemoteDirectory, newRemoteFile);
    }

    @Override
    public CreateFileInfoRemote createChildFile(String name, FilePath sourceFilePath, boolean exclusive)
            throws IOException, PermissionDeniedException, FileExistsException, RemoteException, ObjectNotFoundException, NotFileException, RemoteException
    {
        IndelibleFSVolume ourVolume = ((IndelibleDirectoryNode)coreObject).getVolume();
        IndelibleFSObject sourceObject = ourVolume.getObjectByPath(sourceFilePath);
        if (sourceObject instanceof IndelibleFileNode)
        {
            if (sourceObject instanceof IndelibleDirectoryNode)
                throw new NotFileException();
        }
        else
        {
            throw new ObjectNotFoundException();
        }
        IndelibleFileNode sourceFileNode = (IndelibleFileNode)sourceObject;
        return createChildFile(name, sourceFileNode, exclusive);
    }

    private CreateFileInfoRemote createChildFile(String name,
            IndelibleFileNode sourceFileNode, boolean exclusive) throws PermissionDeniedException, FileExistsException, NotFileException, IOException, RemoteException
    {
        CreateFileInfo newFileInfo = ((IndelibleDirectoryNode)coreObject).createChildFile(name, sourceFileNode, exclusive);
        IndelibleFileNodeIF newLocalFile = newFileInfo.getCreatedNode();
        IndelibleDirectoryNodeIF newDirectory = newFileInfo.getDirectoryNode();
        IndelibleFileNodeRemoteImpl newRemoteFile = super.newObject(newLocalFile, connection);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory = newObject(newDirectory, connection);
        return new CreateFileInfoRemote(newRemoteDirectory, newRemoteFile);
    }
    
    @Override
    public CreateFileInfoRemote createChildFile(String name, IndelibleFSObjectID sourceFileID, boolean exclusive)
            throws IOException, PermissionDeniedException, FileExistsException, RemoteException, NotFileException, ObjectNotFoundException
    {
        IndelibleFSVolume ourVolume = ((IndelibleDirectoryNode)coreObject).getVolume();
        IndelibleFSObject sourceObject = ourVolume.getObjectByID(sourceFileID);
        if (sourceObject instanceof IndelibleFileNode)
        {
            if (sourceObject instanceof IndelibleDirectoryNode)
                throw new NotFileException();
        }
        else
        {
            throw new ObjectNotFoundException();
        }
        IndelibleFileNode sourceFileNode = (IndelibleFileNode)sourceObject;
        return createChildFile(name, sourceFileNode, exclusive);
    }
    
    @Override
    public CreateFileInfoRemote createChildFile(String name, HashMap<String, CASIDDataDescriptor>initialForkData, boolean exclusive)
    throws IOException, PermissionDeniedException, FileExistsException, RemoteException
    {
        CreateFileInfo newFileInfo = ((IndelibleDirectoryNode)coreObject).createChildFile(name, initialForkData, exclusive);
        IndelibleFileNodeIF newLocalFile = newFileInfo.getCreatedNode();
        IndelibleDirectoryNodeIF newDirectory = newFileInfo.getDirectoryNode();
        IndelibleFileNodeRemoteImpl newRemoteFile = super.newObject(newLocalFile, connection);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory = newObject(newDirectory, connection);
        return new CreateFileInfoRemote(newRemoteDirectory, newRemoteFile);
    }
    
    @Override
    public DeleteFileInfoRemote deleteChild(String name) throws IOException,
            PermissionDeniedException, RemoteException, CannotDeleteDirectoryException
    {
        DeleteFileInfo deleteFileInfo = ((IndelibleDirectoryNode)coreObject).deleteChild(name);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory;
        if (deleteFileInfo.deleteSucceeded())
        {
        	newRemoteDirectory = newObject(deleteFileInfo.getDirectoryNode(), connection);
        }
        else
        {
        	newRemoteDirectory = null;
        }
		return new DeleteFileInfoRemote(newRemoteDirectory, deleteFileInfo.deleteSucceeded());
    }

    @Override
    public DeleteFileInfoRemote deleteChildDirectory(String name) throws IOException,
            PermissionDeniedException, NotDirectoryException
    {
        DeleteFileInfo deleteFileInfo = ((IndelibleDirectoryNode)coreObject).deleteChildDirectory(name);
        IndelibleDirectoryNodeRemoteImpl newRemoteDirectory;
        if (deleteFileInfo.deleteSucceeded())
        {
        	newRemoteDirectory = newObject(deleteFileInfo.getDirectoryNode(), connection);
        }
        else
        {
        	newRemoteDirectory = null;
        }
		return new DeleteFileInfoRemote(newRemoteDirectory, deleteFileInfo.deleteSucceeded());
    }

    @Override
    public IndelibleFileNodeRemote getChildNode(String name)
            throws IOException, RemoteException, ObjectNotFoundException
    {
        IndelibleFileNode localChild = ((IndelibleDirectoryNode)coreObject).getChildNode(name);
        IndelibleFileNodeRemote returnChild = null;
        if (localChild != null)
        {
            if (localChild instanceof IndelibleDirectoryNode)
                returnChild = newObject((IndelibleDirectoryNode)localChild, connection);
            else
                returnChild = super.newObject(localChild, connection);
        }
        return returnChild;
    }

    @Override
    public String[] list() throws IOException, RemoteException
    {
        return ((IndelibleDirectoryNode)coreObject).list();
    }

    @Override
    public boolean isDirectory()
    {
        return true;
    }

    @Override
    public boolean isFile()
    {
        return false;
    }
    
    public int getNumChildren() throws RemoteException
    {
    	return ((IndelibleDirectoryNode)coreObject).getNumChildren();
    }

	@Override
	public IndelibleNodeInfo[] getChildNodeInfo(String[] mdToRetrieve)
			throws IOException, PermissionDeniedException, RemoteException
	{
		return ((IndelibleDirectoryNode)coreObject).getChildNodeInfo(mdToRetrieve);
	}
	
	@Override
	public IndelibleDirectoryNodeRemoteImpl newObject(IndelibleFSObjectIF object,
			IndelibleFSServerConnectionImpl connection) throws RemoteException
	{
		return new IndelibleDirectoryNodeRemoteImpl((IndelibleDirectoryNodeIF)object, connection);
	}
}
