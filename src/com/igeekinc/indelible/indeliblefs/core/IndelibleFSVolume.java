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
 
package com.igeekinc.indelible.indeliblefs.core;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.DeleteFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.MoveObjectInfo;
import com.igeekinc.indelible.indeliblefs.exceptions.CannotDeleteDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotFileException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.logging.DebugLogMessage;
import com.igeekinc.util.logging.ErrorLogMessage;

class GetObjectByIDFuture extends ComboFutureBase<IndelibleFileNodeIF>
{

	public GetObjectByIDFuture()
	{
		super();
	}

	public <A> GetObjectByIDFuture(
			AsyncCompletion<IndelibleFileNodeIF, ? super A> completionHandler,
			A attachment)
	{
		super(completionHandler, attachment);
	}
}

class CreateFileFuture extends ComboFutureBase<IndelibleFileNodeIF>
{

	public CreateFileFuture()
	{
		super();
	}

	public <A> CreateFileFuture(
			AsyncCompletion<IndelibleFileNodeIF, ? super A> completionHandler,
			A attachment)
	{
		super(completionHandler, attachment);
	}
	
}

public abstract class IndelibleFSVolume extends IndelibleFSObject
    implements Serializable, CacheBackingStore, IndelibleFSVolumeIF 
{
    private static final int	kNumSpeedTestFilesToCreate	= 100;
	private static final long serialVersionUID = -8304131223165801502L;
    public static final String [] kUserSettableProperties = {IndelibleFSVolumeIF.kVolumeNamePropertyName};
    
    protected IndelibleFSObjectID rootOID;
    protected transient IndelibleDirectoryNode root;
    protected transient HashMap<IndelibleFSObjectID, IndelibleFileNode>dirtyList = new HashMap<IndelibleFSObjectID, IndelibleFileNode>();
    
	protected transient IndelibleFSManagerConnection connection;
	
    protected IndelibleFSVolume(IndelibleFSObjectID inOID, IndelibleVersion version, IndelibleFSManagerConnection connection)
    {
        super(inOID, version);
        this.connection = connection;
    }
    
    @Override
	public IndelibleFSVolume getVersion(IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException
	{
    	throw new UnsupportedOperationException();
	}

	protected void speedTest()
	{
		byte [] data = new byte[16*1024];
		for (int curByteNum = 0; curByteNum < data.length; curByteNum++)
		{
			data[curByteNum] = (byte) (Math.random() * 256);
		}
		long startTime = System.currentTimeMillis();
		CreateFileInfo dirInfo = null;
		try
		{
			dirInfo =  root.createChildDirectory(".speedtest"+startTime);
		} catch (PermissionDeniedException e1)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
		} catch (FileExistsException e1)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
		} catch (IOException e1)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
		}
		for (int curInsert = 0; curInsert < kNumSpeedTestFilesToCreate; curInsert++)
		{
			BitTwiddle.intToJavaByteArray(curInsert, data, 0);
			CASIDDataDescriptor storeDescriptor = new CASIDMemoryDataDescriptor(data);
			try
			{
				CreateFileInfo createFileInfo = ((IndelibleDirectoryNode)dirInfo.getCreatedNode()).createChildFile(Integer.toString(curInsert), true);
				/*
				IndelibleFSFork newFork = createFileInfo.getCreateNode().getFork("data", true);
				newFork.writeDataDescriptor(0, storeDescriptor);
				*/
				flush();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (PermissionDeniedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (FileExistsException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} /*catch (ForkNotFoundException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}*/
		}
		long endTime = System.currentTimeMillis();
		long elapsed = endTime - startTime;
		Logger.getLogger(getClass()).error("Start up file insert test - "+ kNumSpeedTestFilesToCreate+" files created in "+elapsed+" "+((double)kNumSpeedTestFilesToCreate/((double)elapsed/1000.0))+" records/sec");
	}
    public abstract IndelibleFSManagerConnection getConnection();
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeREmote#getRoot()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getRoot()
	 */
    @Override
	public IndelibleDirectoryNode getRoot()
    throws PermissionDeniedException
    {
        return root;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getObjectByPath(com.igeekinc.util.FilePath)
	 */
    @Override
	public IndelibleFileNode getObjectByPath(FilePath path)
    throws ObjectNotFoundException, PermissionDeniedException, IOException
    {
    	logger.debug(new DebugLogMessage("getObjectByPath called on {0} for path {1}", getObjectID(), path));
        IndelibleFileNode curNode = root;
        if (path.isAbsolute())
            path = path.removeLeadingComponent();
        while(path.getNumComponents() > 0 && curNode != null)
        {
            IndelibleFileNode nextNode = ((IndelibleDirectoryNode)curNode).getChildNode(path.getComponent(0));
            if (nextNode == null)
                throw new ObjectNotFoundException();
            curNode = nextNode;
            path = path.removeLeadingComponent();
        }
        if (logger.isDebugEnabled())
        {
        	if (curNode != null)
        	{
        		logger.debug(new DebugLogMessage("getObjectByPath found {0} for path {1}", curNode.getObjectID(), path));
        		if (logger.isDebugEnabled())
        		{
        			if (curNode.isDirectory())
        			{
        				String [] children = ((IndelibleDirectoryNode)curNode).list();
        				for (String curChild:children)
        					logger.debug(new DebugLogMessage("{0} - {1}", curNode.getObjectID(), curChild));
        			}
        		}
        	}
        	else
        	{
        		logger.debug(new DebugLogMessage("getObjectByPath did not find an object for path {0}, returning null", path));
        	}
        }
        return curNode;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#createNewFile()
	 */
    @Override
	public abstract IndelibleFileNodeIF createNewFile() throws IOException;

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#createNewFile(com.igeekinc.indelible.indeliblefs.core.IndelibleFileNode)
	 */
    @Override
	public abstract IndelibleFileNodeIF createNewFile(IndelibleFileNodeIF sourceFile) throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#createNewDirectory()
	 */
    @Override
	public abstract IndelibleDirectoryNodeIF createNewDirectory() throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#createNewSymlink(java.lang.String)
	 */
    @Override
	public abstract IndelibleSymlinkNode createNewSymlink(String targetPath) throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getObjectByID(com.igeekinc.indelible.oid.IndelibleFSObjectID)
	 */
    @Override
	public IndelibleFileNode getObjectByID(IndelibleFSObjectID id) throws IOException, ObjectNotFoundException
    {
    	return getObjectByID(id, connection.getDefaultVersion(), RetrieveVersionFlags.kNearest);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getObjectByID(com.igeekinc.indelible.oid.IndelibleFSObjectID, com.igeekinc.indelible.indeliblefs.core.IndelibleVersion, com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags)
	 */
    @Override
	public abstract IndelibleFileNode getObjectByID(IndelibleFSObjectID id, IndelibleVersion version, RetrieveVersionFlags flags) throws IOException, ObjectNotFoundException;
    
    
    @Override
	public <A> void getObjectByIDAsync(IndelibleFSObjectID id,
			IndelibleVersion version, RetrieveVersionFlags flags,
			AsyncCompletion<IndelibleFileNodeIF, A> completionHandler,
			A attachment) throws IOException, ObjectNotFoundException
	{
		GetObjectByIDFuture future = new GetObjectByIDFuture(completionHandler, attachment);
		getObjectByIDAsync(id, version, flags, future);
	}

	@Override
	public Future<IndelibleFileNodeIF> getObjectByIDAsync(
			IndelibleFSObjectID id, IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException,
			ObjectNotFoundException
	{
		GetObjectByIDFuture future = new GetObjectByIDFuture();
		getObjectByIDAsync(id, version, flags, future);
		return future;
	}

	protected abstract void getObjectByIDAsync(IndelibleFSObjectID id,
			IndelibleVersion version, RetrieveVersionFlags flags, GetObjectByIDFuture future) throws IOException,
			ObjectNotFoundException;
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#deleteObjectByID(com.igeekinc.indelible.oid.IndelibleFSObjectID)
	 */
    @Override
	public abstract void deleteObjectByID(IndelibleFSObjectID id)
    throws IOException;
    
    @Override
	public DeleteFileInfo deleteObjectByPath(FilePath deletePath)
			throws IOException, ObjectNotFoundException,
			PermissionDeniedException, NotDirectoryException
	{
    	boolean inTransaction = getConnection().inTransaction();
    	boolean exitTransaction = false;
    	if (!inTransaction)
    	{
    		getConnection().startTransaction();
    		exitTransaction = true;
    	}
    	try
    	{
    		if (!deletePath.isAbsolute())
    			throw new IOException("Cannot delete relative paths");
    		FilePath parentPath = deletePath.getParent();
    		if (parentPath == null || parentPath == deletePath)
    			throw new IOException("Cannot delete root");
    		IndelibleFileNode parentNode = getObjectByPath(parentPath);
    		if (!parentNode.isDirectory())
    			throw new NotDirectoryException(parentPath.toString() + " is not a directory");
    		DeleteFileInfo returnInfo;
    		try
    		{
    			returnInfo = ((IndelibleDirectoryNode)parentNode).deleteChild(deletePath.getName());
    		} catch (CannotDeleteDirectoryException e)
    		{
    			returnInfo = ((IndelibleDirectoryNode)parentNode).deleteChildDirectory(deletePath.getName());
    		}
    		if (exitTransaction)
    		{
    			getConnection().commit();
    			exitTransaction = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (exitTransaction)
    			getConnection().rollback();
    	}
	}

	@Override
	public MoveObjectInfo moveObject(FilePath sourcePath,
			FilePath destinationPath) throws IOException,
			ObjectNotFoundException, PermissionDeniedException,
			FileExistsException, NotDirectoryException
	{
    	boolean inTransaction = getConnection().inTransaction();
    	boolean exitTransaction = false;
		if (!sourcePath.isAbsolute() || !destinationPath.isAbsolute())
			throw new IOException("Cannot move between relative paths");
    	if (!inTransaction)
    	{
    		getConnection().startTransaction();
    		exitTransaction = true;
    	}
    	try
    	{
    		FilePath parentSourcePath = sourcePath.getParent();
    		if (parentSourcePath == null || parentSourcePath == sourcePath)
    			throw new IOException("Cannot move root");
    		IndelibleFileNode parentNode = getObjectByPath(parentSourcePath);
    		if (!parentNode.isDirectory())
    			throw new NotDirectoryException(parentSourcePath.toString() + " is not a directory");
    		IndelibleDirectoryNode parentDirectoryNode = (IndelibleDirectoryNode)parentNode;
    		IndelibleFileNode sourceNode = parentDirectoryNode.getChildNode(sourcePath.getName());
    		
    		IndelibleDirectoryNode destinationParentDirectoryNode = null;
    		String destinationName;
    		// Get the parent
    		FilePath destinationParentPath = destinationPath.getParent();
    		IndelibleFileNode destinationParentNode = getObjectByPath(destinationParentPath);	// If we get an ObjectNotFound exception here we exit and throw the exception
    		if (!destinationParentNode.isDirectory())
    			throw new NotDirectoryException(destinationParentPath+" is not a directory");
    		destinationName = destinationPath.getName();	// We were given a path to the destination file so we use that name
    		destinationParentDirectoryNode = (IndelibleDirectoryNode)destinationParentNode;
    		CreateFileInfo destInfo;
    		try
			{
				destInfo = destinationParentDirectoryNode.createChildLinkInternal(destinationName, sourceNode, true);
			} catch (NotFileException e)
			{
				// We shouldn't get here
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				throw new IOException("Could not move file");
			}
    		DeleteFileInfo sourceInfo;
    		try
			{
				sourceInfo = parentDirectoryNode.deleteChild(sourcePath.getName());
			} catch (CannotDeleteDirectoryException e)
			{
				sourceInfo = parentDirectoryNode.deleteChildDirectory(sourcePath.getName());
			}
    		
    		MoveObjectInfo returnInfo = new MoveObjectInfo(sourceInfo, destInfo);
    		if (exitTransaction)
    		{
    			getConnection().commit();
    			exitTransaction = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (exitTransaction)
    			getConnection().rollback();
    	}
	}

	@Override
	public IndelibleVersionIterator listVersions() throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}

	protected abstract IndelibleFSFork createFork(IndelibleFileNode parent, String name);
    
    protected abstract boolean deleteFork(IndelibleFSFork deleteFork) throws IOException;
    
    /**
     * Creates a fork with the contents of specified fork.  The new fork is a new object and
     * any changes to the content will only affect the new fork.
     * @param parent
     * @param name
     * @param sourceFork
     * @return
     * @throws IOException 
     */
    protected abstract IndelibleFSFork createFork(IndelibleFileNode parent, String name, IndelibleFSFork sourceFork) throws IOException;
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeREmote#getVolumeID()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getVolumeID()
	 */
    @Override
	public IndelibleFSObjectID getVolumeID()
    {
        return (IndelibleFSObjectID)getObjectID();
    }
    public IndelibleFSObject handleMiss(IndelibleFSObjectID key) throws IOException
    {
        try
		{
			return getObjectByID(key);
		} catch (ObjectNotFoundException e)
		{
			throw new IOException("Could not find object "+key);
		}
    }
    public abstract void storeObject(IndelibleFSObject storeObject) throws IOException;
    public abstract void updateObject(IndelibleFSObject updateObject) throws IOException;

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#setVolumeName(java.lang.String)
	 */
    @Override
	public void setVolumeName(String volumeName) throws PermissionDeniedException
    {
        try
        {
            HashMap<String, Object>volumeResources = getMetaDataResource(IndelibleFSVolumeIF.kVolumeResourcesName);
            if (volumeResources == null)
                volumeResources = new HashMap<String, Object>();
            volumeResources.put(IndelibleFSVolumeIF.kVolumeNamePropertyName, volumeName);
            super.setMetaDataResource(IndelibleFSVolumeIF.kVolumeResourcesName, volumeResources);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getVolumeName()
	 */
    @Override
	public String getVolumeName() throws PermissionDeniedException
    {
        String volumeName = null;
        
        try
        {
            HashMap<String, Object>volumeResources = getMetaDataResource(IndelibleFSVolumeIF.kVolumeResourcesName);
            if (volumeResources != null)
                volumeName = (String)volumeResources.get(IndelibleFSVolumeIF.kVolumeNamePropertyName);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        if (volumeName == null)
            volumeName = getObjectID().toString();
        return volumeName;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#setUserProperties(java.util.Properties)
	 */
    @Override
	public void setUserProperties(Properties propertiesToSet) throws PermissionDeniedException, IOException
    {
        if (propertiesToSet != null)
        {
        	boolean closeTransaction = false;
        	if (!connection.inTransaction())
        	{
        		connection.startTransaction();
        		closeTransaction = true;
        	}
            try
            {
                HashMap<String, Object>volumeResources = getMetaDataResource(IndelibleFSVolumeIF.kVolumeResourcesName);
                if (volumeResources == null)
                    volumeResources = new HashMap<String, Object>();
                for (String curUserPropertyName:kUserSettableProperties)
                {
                    String curPropertyVal = propertiesToSet.getProperty(curUserPropertyName);
                    volumeResources.put(curUserPropertyName, curPropertyVal);
                }

                super.setMetaDataResource(IndelibleFSVolumeIF.kVolumeResourcesName, volumeResources);
        		if (closeTransaction)
        		{
        			connection.commit();
        			closeTransaction = false;
        		}
            } catch (IOException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            }
            finally
            {
            	if (closeTransaction)
        		{
        			connection.rollback();
        		}
            }
        }
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#setMetaDataResource(java.lang.String, java.util.HashMap)
	 */
    @Override
    public IndelibleFSVolume setMetaDataResource(String mdResourceName,
            HashMap<String, Object> resources)
            throws PermissionDeniedException, IOException
    {
        if (mdResourceName.equals(IndelibleFSVolumeIF.kVolumeResourcesName))
            throw new PermissionDeniedException("Volume resources ("+IndelibleFSVolumeIF.kVolumeResourcesName+") cannot be changed via setMetaDataResouce");
    	boolean closeTransaction = false;
    	if (!connection.inTransaction())
    	{
    		connection.startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		IndelibleFSVolume returnVolume = (IndelibleFSVolume)super.setMetaDataResource(mdResourceName, resources);
    		return returnVolume;
    	}
    	finally
    	{
        	if (closeTransaction)
    		{
    			connection.rollback();
    		}
    	}
    }

	public void addDirtyNode(IndelibleFileNode dirtyNode)
	{
		synchronized(dirtyList)
		{
			if (!dirtyList.containsKey(dirtyNode.getObjectID()))
			{
				dirtyList.put(dirtyNode.getObjectID(), dirtyNode);
				if (dirtyList.size() == 1)
					getConnection().addVolumeToFlush(this);
			}
		}
	}
	
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException 
    {
    	in.defaultReadObject();
    	dirtyList = new HashMap<IndelibleFSObjectID, IndelibleFileNode>();
    }
    
	public void flush()
	{
		synchronized(dirtyList)
		{
			logger.debug(new DebugLogMessage("Flushing volume {0} - {1} nodes to flush", getObjectID(), dirtyList.size()));
			IndelibleFileNode [] dirtyNodes = new IndelibleFileNode[dirtyList.size()];
			dirtyNodes = dirtyList.values().toArray(dirtyNodes);
			for (IndelibleFileNode curNode:dirtyNodes)	// flush will remove the node from the dirty list so we need to use the array
			{
				try
				{
					logger.debug(new DebugLogMessage("Volume {0} flushing node {1}", getObjectID(), curNode.getObjectID()));
					curNode.flush();
				} catch (IOException e)
				{
					logger.error(new ErrorLogMessage("Caught exception"), e);
				}
			}
			if (dirtyList.size() != 0)
			{
				logger.error(new ErrorLogMessage("dirtyList not empty after flush! - clearing"));
				dirtyList.clear();
			}
		}
	}

	/**
	 * This should only be called by an abort/rollback method
	 */
	public void clearDirtyList()
	{
		synchronized(dirtyList)
		{
			dirtyList.clear();
		}
	}
	public void removeDirtyNode(IndelibleFileNode nodeToRemove)
	{
		synchronized(dirtyList)
		{
			dirtyList.remove(nodeToRemove.getObjectID());
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#listVersionsForObject(com.igeekinc.indelible.oid.IndelibleFSObjectID)
	 */
	@Override
	public abstract IndelibleVersionIterator listVersionsForObject(IndelibleFSObjectID id) throws IOException;
	
	/**
	 * Adds a snapshot for a particular version(time).  Snapshots can be added for any time and the
	 * system will retain the nearest version (before) the snapshot.  
	 * @param snapshotVersion
	 * @throws PermissionDeniedException
	 * @throws IOException
	 */
    public void addSnapshot(IndelibleSnapshotInfo snapshotInfo) throws PermissionDeniedException, IOException
    {
    	HashMap<String, Object>snapshotMap = getMetaDataResource(IndelibleFSVolumeIF.kVolumeSnapshotsPropertyName);
    	boolean closeTransaction = false;
    	if (!connection.inTransaction())
    	{
    		connection.startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		if (snapshotMap == null)
    			snapshotMap = new HashMap<String, Object>();
    		String snapshotName = getSnapshotName(snapshotInfo.getVersion());
    		snapshotMap.put(snapshotName, snapshotInfo);
    		super.setMetaDataResource(IndelibleFSVolumeIF.kVolumeSnapshotsPropertyName, snapshotMap);
    		if (closeTransaction)
    		{
    			connection.commit();
    			closeTransaction = false;
    		}
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			connection.rollback();
    		}
    	}
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#releaseSnapshot(com.igeekinc.indelible.indeliblefs.core.IndelibleVersion)
	 */
    @Override
	public boolean releaseSnapshot(IndelibleVersion removeSnapshotVersion) throws PermissionDeniedException, IOException
    {
    	HashMap<String, Object>snapshotMap = getMetaDataResource(IndelibleFSVolumeIF.kVolumeSnapshotsPropertyName);
    	boolean closeTransaction = false;
    	if (!connection.inTransaction())
    	{
    		connection.startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		IndelibleSnapshotInfo removedVersion = (IndelibleSnapshotInfo) snapshotMap.remove(getSnapshotName(removeSnapshotVersion));
    		if (removedVersion != null)
    		{
    			super.setMetaDataResource(IndelibleFSVolumeIF.kVolumeSnapshotsPropertyName, snapshotMap);
        		if (closeTransaction)
        		{
        			connection.commit();
        			closeTransaction = false;
        		}
    			return true;
    		}
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			connection.rollback();
    		}
    	}
    	return false;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#getInfoForSnapshot(com.igeekinc.indelible.indeliblefs.core.IndelibleVersion)
	 */
    @Override
	public IndelibleSnapshotInfo getInfoForSnapshot(IndelibleVersion retrieveSnapshotVersion) throws PermissionDeniedException, IOException
    {
    	HashMap<String, Object>snapshotMap = getMetaDataResource(IndelibleFSVolumeIF.kVolumeSnapshotsPropertyName);
    	IndelibleSnapshotInfo returnInfo = (IndelibleSnapshotInfo)snapshotMap.get(getSnapshotName(retrieveSnapshotVersion));
    	return returnInfo;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSVolumeIF#listSnapshots()
	 */
    @Override
	public IndelibleSnapshotIterator listSnapshots() throws PermissionDeniedException, IOException
    {
    	HashMap<String, Object>snapshotMap = getMetaDataResource(IndelibleFSVolumeIF.kVolumeSnapshotsPropertyName);
    	ArrayList<IndelibleSnapshotInfo>snapshotList = new ArrayList<IndelibleSnapshotInfo>();

    	if (snapshotMap != null)
    	{
    		Collection<Object> versionsCollection = snapshotMap.values();
    		for (Object curObject:versionsCollection)
    		{
    			if (curObject instanceof IndelibleSnapshotInfo)
    				snapshotList.add((IndelibleSnapshotInfo)curObject);
    		}
    		Collections.sort(snapshotList, new Comparator<IndelibleSnapshotInfo>(){

				@Override
				public int compare(IndelibleSnapshotInfo paramT1,
						IndelibleSnapshotInfo paramT2)
				{
					long compare = paramT1.getVersion().getVersionTime() - paramT2.getVersion().getVersionTime();
					if (compare == 0)
						compare = paramT1.getVersion().getUniquifier() - paramT2.getVersion().getUniquifier();
					int returnValue = 0;
					if (compare < 0)
						returnValue = -1;
					if (compare >0)
						returnValue = 1;
					return returnValue;
				}
    			
    		});
    	}
    	IndelibleSnapshotListIterator returnIterator = new IndelibleSnapshotListIterator(snapshotList);
    	return returnIterator;
    }
    

	private String getSnapshotName(IndelibleVersion snapshotVersion)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'@'HH:mm:ss.SSSZ");
    	String snapshotName=formatter.format(new Date(snapshotVersion.getVersionTime()))+"-"+Integer.toString(snapshotVersion.getUniquifier());
		return snapshotName;
	}
}
