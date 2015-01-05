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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerInternal;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentExists;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.logging.DebugLogMessage;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSCASVolume extends IndelibleFSVolume 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -521993014929695630L;
	private transient CASCollectionConnection	collectionConnection;
	private transient ObjectIDFactory oidFactory;

    /**
     * @param inOID
     * @throws SQLException 
     */
    public IndelibleFSCASVolume(IndelibleFSObjectID inOID, IndelibleVersion version, CASCollectionConnection inCollection, IndelibleFSManagerConnection connection) 
    throws IOException, SQLException
    {
        super(inOID, version, connection);
        oidFactory = ((CASServerInternal)inCollection.getCASServer()).getOIDFactory();
        collectionConnection = inCollection;
        setConnection(connection);
        if (rootOID == null)
        {
            root = createNewDirectory();
            rootOID = root.objectID;
        }
        else
        {
            try
			{
				root = (IndelibleDirectoryNode) getObjectByID(rootOID);
			} catch (ObjectNotFoundException e)
			{
				throw new IOException("Could not retrieve rootOID "+rootOID);
			}
        }
        //speedTest();
    }

    protected void setConnection(IndelibleFSManagerConnection connection)
            throws SQLException
    {
        this.connection = connection;
    }

    @Override
    public IndelibleFSManagerConnection getConnection()
    {
        return connection;
    }

    /*
     * To be called after deserializing this object from the CASCollection.  We will
     * know what the CASCollection is at that point since we have just retrieved this object from
     * there.
     */
    protected void setCollection(CASCollectionConnection inCollection) throws IOException
    {
        collectionConnection = inCollection;
        oidFactory = ((CASServerInternal)inCollection.getCASServer()).getOIDFactory();
        try
		{
			root = (IndelibleDirectoryNode) getObjectByID(rootOID);
		} catch (ObjectNotFoundException e)
		{
			throw new IOException("Could not retrieve root node "+rootOID);
		}
    }
    
    public CASCollectionConnection getCollectionConnection()
    {
        return collectionConnection;
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSVolume#createNewFile()
     */
    public IndelibleFileNodeIF createNewFile() throws IOException 
    {
        IndelibleFileNode newFileNode = new IndelibleFileNode(this, (IndelibleFSObjectID)oidFactory.getNewOID(IndelibleFileNode.class),
                connection.getVersion());
        newFileNode.setDirty();
        newFileNode.modified();
        return newFileNode;
    }
    
    public IndelibleFileNodeIF createNewFile(IndelibleFileNodeIF sourceNode) throws IOException
    {
        IndelibleFileNode newFileNode = new IndelibleFileNode(this, (IndelibleFSObjectID)oidFactory.getNewOID(IndelibleFileNode.class),
                connection.getVersion());
        IndelibleFSForkIF [] sourceForks = ((IndelibleFileNode)sourceNode).getForks();
        for (IndelibleFSForkIF curSourceFork:sourceForks)
        {
        	newFileNode.createOrReplaceFork(curSourceFork.getName(), curSourceFork);
        }
        return newFileNode;
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSVolume#createNewDirectory()
     */
    public IndelibleDirectoryNode createNewDirectory() throws IOException 
    {
        IndelibleDirectoryNode newDirectoryNode = new IndelibleDirectoryNode(this, (IndelibleFSObjectID)oidFactory.getNewOID(IndelibleFileNode.class),
                connection.getVersion());
        storeObject(newDirectoryNode);
        return newDirectoryNode;
    }

    
    @Override
	public IndelibleSymlinkNode createNewSymlink(String targetPath)
			throws IOException
	{
    	IndelibleSymlinkNode newSymlinkNode = new IndelibleSymlinkNode(this, (IndelibleFSObjectID)oidFactory.getNewOID(IndelibleFileNode.class),
                connection.getVersion());
        storeObject(newSymlinkNode);
        newSymlinkNode.setTargetPath(targetPath);
        updateObject(newSymlinkNode);
        return newSymlinkNode;
	}

    
    @Override
	public IndelibleFileNode getObjectByID(IndelibleFSObjectID id, IndelibleVersion version, RetrieveVersionFlags flags) throws IOException, ObjectNotFoundException
	{
        IndelibleFileNode returnNode = null;
        synchronized(dirtyList)
        {
        	returnNode = dirtyList.get(id);
        }
        if (returnNode == null)
        {
        	DataVersionInfo nodeDescriptor = null;
			try
			{
				nodeDescriptor = collectionConnection.retrieveSegment(id, version, flags);
			} catch (SegmentNotFound e1)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
			}
        	if (nodeDescriptor != null)
        	{
        		ObjectInputStream readStream = new ObjectInputStream(nodeDescriptor.getDataDescriptor().getInputStream());

        		try
        		{
        			returnNode = (IndelibleFileNode)readStream.readObject();
        			readStream.close();
        			returnNode.version = nodeDescriptor.getVersion();
        		}
        		catch (ClassNotFoundException e)
        		{
        			throw new IOException("ClassNotFound exception caught");
        		}
        		// volume is transient so set it to point to us
        		returnNode.setVolume(this);
        		for (IndelibleFSFork curFork:returnNode.getForks())
        		{
        			((IndelibleFSCASFork)curFork).setCASCollection(collectionConnection);
        			curFork.parent = returnNode;
        		}
        	}
        	else
        		throw new ObjectNotFoundException();
        }
        return returnNode;
    }

    class GetObjectByIDAsyncCompletionHandler implements AsyncCompletion<DataVersionInfo, Void>
    {
    	IndelibleFSCASVolume volume;
    	GetObjectByIDFuture future;
    	GetObjectByIDAsyncCompletionHandler(IndelibleFSCASVolume volume, GetObjectByIDFuture future)
    	{
    		this.volume = volume;
    		this.future = future;
    	}
    	
    	@Override
		public void completed(DataVersionInfo nodeDescriptor, Void attachment)
		{
    		try
			{
				ObjectInputStream readStream = new ObjectInputStream(nodeDescriptor.getDataDescriptor().getInputStream());
				IndelibleFileNode returnNode;
				
				try
				{
					returnNode = (IndelibleFileNode)readStream.readObject();
					readStream.close();
					returnNode.version = nodeDescriptor.getVersion();
				}
				catch (ClassNotFoundException e)
				{
					throw new IOException("ClassNotFound exception caught");
				}
				// volume is transient so set it to point to us
				returnNode.setVolume(volume);
				for (IndelibleFSFork curFork:returnNode.getForks())
				{
					((IndelibleFSCASFork)curFork).setCASCollection(collectionConnection);
					curFork.parent = returnNode;
				}
				future.completed(returnNode, null);
			} catch (IOException e)
			{
				future.failed(e, null);
			}
		}

		@Override
		public void failed(Throwable exc, Void attachment)
		{
			future.failed(exc, null);
		}
    }
	@Override
	protected void getObjectByIDAsync(IndelibleFSObjectID id,
			IndelibleVersion version, RetrieveVersionFlags flags,
			GetObjectByIDFuture future) throws IOException,
			ObjectNotFoundException
	{
        IndelibleFileNode returnNode = null;
        synchronized(dirtyList)
        {
        	returnNode = dirtyList.get(id);
        }
        if (returnNode == null)
        {
        	collectionConnection.retrieveSegmentAsync(id, new GetObjectByIDAsyncCompletionHandler(this, future), null);
        }
        else
        {
        	future.completed(returnNode, null);
        }
	}

	public IndelibleVersionIterator listVersionsForObject(IndelibleFSObjectID id) throws IOException
	{
		return collectionConnection.listVersionsForSegment(id);
	}
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSVolume#deleteObjectByID(com.igeekinc.indelible.oid.IndelibleFSObjectID)
     */
    public void deleteObjectByID(IndelibleFSObjectID id) throws IOException 
    {
        // TODO Auto-generated method stub
        // Be sure to clear out directory entries if it's a directory
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSVolume#createStream()
     */
    protected IndelibleFSFork createFork(IndelibleFileNode parent, String name) 
    {
        IndelibleFSCASFork returnFork = new IndelibleFSCASFork(parent, name);
        returnFork.setCASCollection(collectionConnection);
        return returnFork;
    }

    
    @Override
    protected IndelibleFSFork createFork(IndelibleFileNode parent, String name,
            IndelibleFSFork sourceFork) throws IOException
    {
        IndelibleFSCASFork returnFork;
        
        if (sourceFork instanceof IndelibleFSCASFork)
        {
            IndelibleFSCASFork sourceFSCASFork = (IndelibleFSCASFork)sourceFork;
            returnFork = new IndelibleFSCASFork(parent, name, sourceFSCASFork);
            returnFork.setCASCollection(collectionConnection);
        }
        else
        {
            returnFork =  new IndelibleFSCASFork(parent, name);
            returnFork.setCASCollection(collectionConnection);
            long offset = 0;
            
            while (offset < sourceFork.length())
            {
                CASIDDataDescriptor curDescriptor = sourceFork.getDataDescriptor(offset, 1024*1024);
                returnFork.appendDataDescriptor(curDescriptor);
                offset += curDescriptor.getLength();
            }
        }
        return returnFork;
    }

    
    @Override
	protected boolean deleteFork(IndelibleFSFork deleteFork) throws IOException
	{
		deleteFork.truncate(0L);
		return false;
	}

	public void storeObject(IndelibleFSObject storeObject) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream storeStream = new ObjectOutputStream(baos);
        storeStream.writeObject(storeObject);
        storeStream.close();
        CASIDDataDescriptor storeDescriptor = new CASIDMemoryDataDescriptor(baos.toByteArray());
        try
		{
			collectionConnection.storeVersionedSegment(storeObject.getObjectID(), storeDescriptor);
		} catch (SegmentExists e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IOException("Segment already exists");	// segment exists is low-level so we'll just make it into an IOException here
		}
    }
    
    public void updateObject(IndelibleFSObject updateObject) throws IOException
    {
    	logger.debug(new DebugLogMessage("Updating object {0}", updateObject.getObjectID()));
    	if (updateObject instanceof IndelibleFileNode)
    	{
    		IndelibleFileNode updateFile = (IndelibleFileNode)updateObject;
    		if (updateFile.parents.length == 0)
    		{
    			// No more parents means we need to delete this
    			
    		}
    		if (logger.isDebugEnabled() && updateFile.isDirectory())
    		{
    			String [] children = ((IndelibleDirectoryNode)updateFile).list();
    			for (String curChild:children)
    			{
    				logger.debug(new DebugLogMessage("{0} - {1}", updateFile.getObjectID(), curChild));
    			}
    		}
    	}
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream storeStream = new ObjectOutputStream(baos);
        storeStream.writeObject(updateObject);
        storeStream.close();
        CASIDDataDescriptor storeDescriptor = new CASIDMemoryDataDescriptor(baos.toByteArray());
        try
		{
			collectionConnection.storeVersionedSegment(updateObject.getObjectID(), storeDescriptor);
		} catch (SegmentExists e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IOException("Segment already exists");	// segment exists is low-level so we'll just make it into an IOException here
		}
    }
    
    @Override
    protected void setDirty()
    {
        try
        {
            writeVolumeInfo();
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
    }

    public static final String kVolumeMetaDataPropertyName = "com.igeekinc.indelible.indeliblefs.core.IndelibleFSCASVolumeInfo";
    public static final String	kVolumeInfoPropertyName	= "VolumeInfo";
    protected synchronized void writeVolumeInfo() throws IOException, PermissionDeniedException
    {
        CASCollectionConnection volumeCollectionConnection = getCollectionConnection();
        HashMap<String, Serializable> volumeMD = new HashMap<String, Serializable>();
        volumeMD.put(kVolumeInfoPropertyName, this);
        
        volumeCollectionConnection.setMetaDataResource(kVolumeMetaDataPropertyName, volumeMD);
    }

    public String toString()
    {
    	StringBuffer returnBuffer = new StringBuffer();
    	returnBuffer.append(objectID.toString()+"\n");
    	try
		{
			String [] mdKeys = listMetaDataResources();
			for (String curKey:mdKeys)
			{
				returnBuffer.append(curKey+"\n");
				Map<String, Object>resourceData = getMetaDataResource(curKey);
				for (String curResourceKey:resourceData.keySet())
				{
					returnBuffer.append(curResourceKey+" = "+resourceData.get(curResourceKey)+"\n");
				}
			}
		} catch (PermissionDeniedException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
    	return returnBuffer.toString();
    }

	@Override
	public IndelibleVersionIterator listVersions() throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}
}
