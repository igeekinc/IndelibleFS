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
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.logging.ErrorLogMessage;


public class IndelibleFileNode extends IndelibleFSObject implements Serializable, IndelibleFileNodeIF
{
    private static final long serialVersionUID = 3257285833592616244L;
    protected transient IndelibleFSVolume volume;
    protected IndelibleParentEntry primaryParent;   // This is the parent that will get updated
    protected transient WeakReference<IndelibleDirectoryNode>primaryParentRef;
    protected IndelibleParentEntry [] parents;
    protected IndelibleFSFork [] forks;
    protected String forkNames[];
    protected long lastModifiedTime;
    protected int referenceCount;
    protected long totalLength, lengthWithChildren;
    
    public static final String kIndelibleFSMetaDataPropertyName = "com.igeekinc.indelible.indeliblefs.md";
    
    public static final String kFileSizePropertyName = "fileSize";
    protected IndelibleFileNode()
    {
        super();
        forks = new IndelibleFSFork[0];
        forkNames = new String[0];
        parents = new IndelibleParentEntry[0];
        primaryParent = null;
    }
    
    protected IndelibleFileNode(IndelibleFSVolume volume, IndelibleFSObjectID objectID, IndelibleVersion version)
    {
        super(objectID, version);
        forks = new IndelibleFSFork[0];
        forkNames = new String[0];
        this.volume = volume;
        parents = new IndelibleParentEntry[0];
        primaryParent = null;
    }

    protected IndelibleFileNode(IndelibleFileNode source, IndelibleVersion newVersion)
    {
    	super(source.getObjectID(), newVersion);
    	if (newVersion.isFinal())
    		throw new IllegalArgumentException("newVersion cannot be a final version");
    	forks = Arrays.copyOf(source.forks, source.forks.length);
    	forkNames = Arrays.copyOf(source.forkNames, source.forkNames.length);
    	volume = source.volume;
    	parents = source.parents;
    	primaryParent = source.primaryParent;
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.util.FileLike#lastModified()
     */
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#lastModified()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#lastModified()
	 */
    @Override
	public long lastModified() 
    {
        return lastModifiedTime;
    }

    /* (non-Javadoc)
     * @see com.igeekinc.util.FileLike#isDirectory()
     */
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#isDirectory()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#isDirectory()
	 */
    @Override
	public boolean isDirectory() 
    {
        return false;
    }

    /* (non-Javadoc)
     * @see com.igeekinc.util.FileLike#isFile()
     */
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#isFile()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#isFile()
	 */
    @Override
	public boolean isFile() 
    {
        return true;
    }

    /* (non-Javadoc)
     * @see com.igeekinc.util.FileLike#length()
     */
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#length()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#length()
	 */
    @Override
	public long length() 
    {
        return totalLength();
    }

    /* (non-Javadoc)
     * @see com.igeekinc.util.FileLike#totalLength()
     */
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#totalLength()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#totalLength()
	 */
    @Override
	public long totalLength() 
    {
        long returnLength = 0;
        for (int curStreamNum = 0; curStreamNum < forks.length; curStreamNum++)
            returnLength+= forks[curStreamNum].length();
        /*if (returnLength != totalLength)
            throw new InternalError("totalLength does not match sum of fork lengths");*/
        return returnLength;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#lengthWithChildren()
	 */
    @Override
	public long lengthWithChildren()
    {
        return totalLength + lengthWithChildren;
    }
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#getReferenceCount()
     */
    public int getReferenceCount()
    {
        return referenceCount;
    }
    
    protected synchronized void incrementReferenceCount()
    throws IOException
    {
        referenceCount++;
        setDirty();
    }
    
    protected synchronized void decrementReferenceCount()
    throws IOException
    {
        referenceCount--;
        setDirty();
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeRemote#getFork(java.lang.String, boolean)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#getFork(java.lang.String, boolean)
	 */
    @Override
	public synchronized IndelibleFSForkIF getFork(String name, boolean createIfNecessary)
    throws IOException, ForkNotFoundException
    {
    	for (int curForkNum = 0; curForkNum < forks.length; curForkNum++)
    	{
    		if (forkNames[curForkNum].equals(name))
    		{
    			IndelibleFSForkIF returnFork = forks[curForkNum];
    			return returnFork;
    		}
    	}
    	if (!createIfNecessary)
    		throw new ForkNotFoundException();
    	boolean closeTransaction = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		modified();
    		if (!version.equals(IndelibleVersion.kLatestVersion))
    		{
    			throw new ForkNotFoundException();
    		}
    		IndelibleFSFork newFork = volume.createFork(this, name);
    		IndelibleFSFork [] newForks = new IndelibleFSFork[forks.length+1];
    		String [] newForkNames = new String[forks.length + 1];
    		System.arraycopy(forks, 0, newForks, 0, forks.length);
    		System.arraycopy(forkNames, 0, newForkNames, 0, forks.length);
    		newForks[forks.length] = newFork;
    		newForkNames[forks.length] = name;
    		forks = newForks;
    		forkNames = newForkNames;
    		if (closeTransaction)
    		{
    			volume.getConnection().commit();
    			closeTransaction = false;
    		}
    		return newFork;
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#createOrReplaceFork(java.lang.String, com.igeekinc.indelible.indeliblefs.core.IndelibleFSFork)
	 */

	@Override
	public void deleteFork(String forkName) throws IOException,
			ForkNotFoundException, PermissionDeniedException
	{
		IndelibleFSFork [] newForks = new IndelibleFSFork[forks.length - 1];
		String [] newForkNames = new String[forks.length - 1];
		int newForkNum = 0;
		IndelibleFSFork deleteFork = null;
		for (int curForkNum = 0; curForkNum < forks.length; curForkNum++)
    	{
    		if (forkNames[curForkNum].equals(forkName))
    		{
    			deleteFork = forks[curForkNum];
    		}
    		else
    		{
    			newForkNames[newForkNum] = forkNames[curForkNum];
    			newForks[newForkNum] = forks[curForkNum];
    		}
    	}
    	if (deleteFork == null)
    		throw new ForkNotFoundException();
    	boolean closeTransaction = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		forks = newForks;
    		forkNames = newForkNames;
    		modified();
    		volume.deleteFork(deleteFork);
    		if (closeTransaction)
    		{
    			volume.getConnection().commit();
    			closeTransaction = false;
    		}
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			volume.getConnection().rollback();
    		}
    	}		
	}

	public synchronized IndelibleFSForkIF createOrReplaceFork(String name, IndelibleFSForkIF sourceFork) throws IOException
    {
    	boolean replace = false;

    	boolean closeTransaction = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		IndelibleFSFork newFork = volume.createFork(this, name, (IndelibleFSFork)sourceFork);
        	for (int curForkNum = 0; curForkNum < forks.length; curForkNum++)
        	{
        		if (forkNames[curForkNum].equals(name))
        		{
        			replace = true;
        			volume.deleteFork(forks[curForkNum]);
        			forks[curForkNum] = newFork;
        			break;
        		}
        	}
    		if (!replace)
    		{
    			IndelibleFSFork [] newForks = new IndelibleFSFork[forks.length+1];
    			String [] newForkNames = new String[forks.length + 1];
    			System.arraycopy(forks, 0, newForks, 0, forks.length);
    			System.arraycopy(forkNames, 0, newForkNames, 0, forks.length);
    			newForks[forks.length] = newFork;
    			newForkNames[forks.length] = name;
    			forks = newForks;
    			forkNames = newForkNames;
    		}
    		modified();
    		if (closeTransaction)
    		{
    			volume.getConnection().commit();
    			closeTransaction = false;
    		}
    		return newFork;
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#listForkNames()
	 */
    @Override
	public synchronized String [] listForkNames()
    {
        String [] returnForkNames = new String[forkNames.length];
        System.arraycopy(forkNames, 0, returnForkNames, 0, forkNames.length);
        return returnForkNames;
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#getForks()
	 */

	public synchronized IndelibleFSFork [] getForks()
    {
        IndelibleFSFork [] returnForks = new IndelibleFSFork[forks.length];
        System.arraycopy(forks, 0, returnForks, 0, forks.length);
        return returnForks;
    }
    
    protected void setVolume(IndelibleFSVolume volume)
    {
        this.volume = volume;
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#getVolume()
	 */
    @Override
	public IndelibleFSVolume getVolume()
    {
        return volume;
    }
    

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#setMetaDataResource(java.lang.String, java.util.HashMap)
	 */
    @Override
    public IndelibleFileNode setMetaDataResource(String mdResourceName,
            Map<String, Object> resources)
            throws PermissionDeniedException, IOException
    {
        if (mdResourceName.equals(kIndelibleFSMetaDataPropertyName))
            throw new PermissionDeniedException("Indelible FS resources ("+kIndelibleFSMetaDataPropertyName+") cannot be changed via setMetaDataResouce");
        return (IndelibleFileNode)super.setMetaDataResource(mdResourceName, resources);
    }

    /**
     * Called by IndelibleFSFork when a fork length changes
     * @param indelibleFSFork
     * @param oldLength
     * @param newLength
     */
    protected void updateLength(IndelibleFSFork indelibleFSFork, long oldLength, long newLength)
    throws IOException
    {
        long delta = newLength - oldLength;
        if (delta != 0)
        {
            //long oldTotalLength = totalLength;
            totalLength += delta;
            setDirty();
            IndelibleDirectoryNode parent;
			try
			{
				parent = getPrimaryParent();
	            if (parent != null)
	            {
	                //parent.updateLengthWithChildren(this, oldTotalLength, totalLength);
	            }
			} catch (ObjectNotFoundException e)
			{
				logger.error(new ErrorLogMessage("Could not find object to update"));
			}

        }
    }
    
    protected void updateLengthWithChildren(IndelibleFileNode source, long oldLength, long newLength) 
    throws IOException
    {
        /*
        long delta = newLength - oldLength;
        if (delta != 0)
        {
            long oldLengthWithChildren = lengthWithChildren; 
            lengthWithChildren += delta;
            volume.updateObject(this);
            IndelibleDirectoryNode parent = getPrimaryParent();
            if (parent != null)
            {
                try
                {
                    volume.updateObject(this);
                } catch (IOException e)
                {
                    Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
                }
                parent.updateLengthWithChildren(this, oldLengthWithChildren, lengthWithChildren);
            }
        }
        */
    }
    
    protected synchronized void addParent(IndelibleDirectoryNode parent, String nameInParent)
    {
        IndelibleParentEntry newEntry = new IndelibleParentEntry(parent, nameInParent);
        if (primaryParent == null)
        {
            primaryParent = newEntry;
            parents = new IndelibleParentEntry[1];
            parents[0] = newEntry;
            primaryParentRef = new WeakReference<IndelibleDirectoryNode>(parent);
        }
        else
        {
            IndelibleParentEntry [] newParents = new IndelibleParentEntry[parents.length + 1];
            System.arraycopy(parents, 0, newParents, 0, parents.length);
            newParents[parents.length] = newEntry;
            parents = newParents;
        }
    }
    
    protected synchronized void removeParent(IndelibleDirectoryNode removeParent, String nameInParent)
    {
        IndelibleFSObjectID removeParentID = removeParent.getObjectID();
        int removeParentNum = 0;
        for (; removeParentNum < parents.length; removeParentNum++)
        {
            if (parents[removeParentNum].getParentID().equals(removeParentID))
                break;
        }
        if (removeParentNum < parents.length)
        {
            if (parents.length > 1)
            {
                IndelibleParentEntry [] newParents = new IndelibleParentEntry[parents.length - 1];
                if (removeParentNum > 0)
                {
                    System.arraycopy(parents, 0, newParents, 0, removeParentNum);
                }
                if (removeParentNum < parents.length - 1)
                {
                    System.arraycopy(parents, removeParentNum + 1, newParents, removeParentNum, parents.length - 1 - removeParentNum);
                }
                parents = newParents;
                primaryParent = parents[0];
            }
            else
            {
                primaryParent = null;
                parents = new IndelibleParentEntry[0];
            }
        }
    }
    protected synchronized IndelibleDirectoryNode getPrimaryParent() throws ObjectNotFoundException
    {
        IndelibleDirectoryNode returnParent = null;
        if (primaryParentRef != null)
        {
             returnParent = primaryParentRef.get();
        }
        if (returnParent == null)
        {
            if (primaryParent != null)
            {
                try
                {
                    IndelibleFileNode parentObj = volume.getObjectByID(primaryParent.getParentID());
                    if (parentObj instanceof IndelibleDirectoryNode)
                    {
                        returnParent = (IndelibleDirectoryNode)parentObj;
                        primaryParentRef = new WeakReference<IndelibleDirectoryNode>(returnParent);
                    }
                } catch (IOException e)
                {
                    Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
                }
                
            }
        }
        return returnParent;
    }
    
    public void flush() throws IOException
    {
    	volume.updateObject(this);

    	clearDirty();
    }
    
    @Override
    public void setDirty()
    {
    	if (!isDirty())
    	{
    		super.setDirty();
    		volume.addDirtyNode(this);
    	}
    }
    
    @Override
    public void clearDirty()
    {
    	volume.removeDirtyNode(this);
    	super.clearDirty();
    }

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#listVersions()
	 */
	@Override
	public IndelibleVersionIterator listVersions() throws IOException
	{
		return volume.listVersionsForObject(this.objectID);
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFileNodeIF#getVersion(com.igeekinc.indelible.indeliblefs.core.IndelibleVersion, com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags)
	 */
	@Override
	public IndelibleFileNode getVersion(IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException
	{
		if (version.equals(this.version))
			return this;
		try
		{
			return volume.getObjectByID(this.objectID, version, flags);
		} catch (ObjectNotFoundException e)
		{
			throw new IOException("Could not retrieve object "+this.objectID+" for version "+version);
		}
	}

	public void modified()
	{
		if (version.isFinal())
		{
			version = volume.getConnection().getVersion();
		}
		lastModifiedTime = System.currentTimeMillis();
		setDirty();
	}
}
