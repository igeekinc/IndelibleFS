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
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.CreateDirectoryInfo;
import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.CreateSymlinkInfo;
import com.igeekinc.indelible.indeliblefs.DeleteFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleNodeInfo;
import com.igeekinc.indelible.indeliblefs.exceptions.CannotDeleteDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotFileException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.logging.DebugLogMessage;
import com.igeekinc.util.logging.ErrorLogMessage;

class CreateChildFileAsyncFuture extends ComboFutureBase<CreateFileInfo>
{	
	@SuppressWarnings("unchecked")
	public <A> CreateChildFileAsyncFuture(AsyncCompletion<CreateFileInfo, ? super A>completionHandler, A attachment)
	{
		super(completionHandler, attachment);
	}
	public CreateChildFileAsyncFuture()
	{
	}	
}

public class IndelibleDirectoryNode extends IndelibleFileNode implements IndelibleDirectoryNodeIF
{
    private static final long serialVersionUID = 3256443603474658359L;
    private Hashtable<String, IndelibleFSObjectID> children;
    private transient ArrayList<IndelibleDirectoryChange> changes;
    protected IndelibleDirectoryNode(IndelibleFSVolume volume, IndelibleFSObjectID objectID, IndelibleVersion version)
    {
        super(volume, objectID, version);
        children = new Hashtable<String, IndelibleFSObjectID>();
        changes = new ArrayList<IndelibleDirectoryChange>();
    }
    
    @SuppressWarnings("unchecked")
	protected IndelibleDirectoryNode(IndelibleDirectoryNode source, IndelibleVersion version)
    {
    	super(source, version);
    	this.children = (Hashtable<String, IndelibleFSObjectID>) source.children.clone();
    	changes = (ArrayList<IndelibleDirectoryChange>) source.changes.clone();
    }
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeRemote#createChildFile(java.lang.String)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#createChildFile(java.lang.String, boolean)
	 */
    @Override
	public CreateFileInfo createChildFile(String name, boolean exclusive)
    throws IOException, PermissionDeniedException, FileExistsException
    {
    	logger.debug(new DebugLogMessage("createChildFile called on {0} to create child ''{1}''", getObjectID(), name));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		IndelibleFileNode newFile = createChildCore(name, exclusive);
    		modified();
    		CreateFileInfo returnInfo = new CreateFileInfo(this, newFile);
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#createChildFile(java.lang.String, java.util.HashMap, boolean)
	 */
    @Override
	public CreateFileInfo createChildFile(String name, HashMap<String, CASIDDataDescriptor>initialForkData, boolean exclusive)
    throws IOException, PermissionDeniedException, FileExistsException, RemoteException
    {
    	/*
    	logger.debug(new DebugLogMessage("createChildFile called on {0} to create child ''{1}'' with initialForkData", getObjectID(), name));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		IndelibleFileNode newFile = createChildCore(name, exclusive);
    		// TODO - figure out if these semantics are reasonable.  Perhaps createChild should simply always be
    		// exclusive (fail if file already exists) and we should add a new retrieve + create call
    		for (String existingFork:newFile.listForkNames())
    		{
    			try
				{
					newFile.deleteFork(existingFork);
				} catch (ForkNotFoundException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
    		}
    		for (String curForkName:initialForkData.keySet())
    		{
    			IndelibleFSForkIF curFork;
    			try
    			{
    				curFork = newFile.getFork(curForkName, true);
    			} catch (ForkNotFoundException e)
    			{
    				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    				throw new IOException("Could not open fork "+curForkName);
    			}
    			curFork.writeDataDescriptor(0, initialForkData.get(curForkName));
    			curFork.flush();
    		}
    		modified();;
    		CreateFileInfo returnInfo = new CreateFileInfo(this, newFile);
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    	*/
    	Future<CreateFileInfo>waitFuture = createChildFileAsync(name, initialForkData, exclusive);
    	try
		{
			return waitFuture.get();
		} 
    	catch (InterruptedException e)
		{
			throw new IOException("Interrupted");
		} catch (ExecutionException e)
		{
			if (e.getCause() instanceof PermissionDeniedException)
				throw (PermissionDeniedException)e.getCause();
			if (e.getCause() instanceof FileExistsException)
				throw (FileExistsException)e.getCause();
			if (e.getCause() instanceof RemoteException)
				throw (RemoteException)e.getCause();
			if (e.getCause() instanceof IOException)
				throw (IOException)e.getCause();
			throw new IOException("Got unexpected exception "+e.getCause().getMessage());
		}
    }
    
    @Override
	public Future<CreateFileInfo> createChildFileAsync(String name,
			HashMap<String, CASIDDataDescriptor> initialForkData,
			boolean exclusive) throws IOException, PermissionDeniedException,
			FileExistsException, RemoteException
	{
    	CreateChildFileAsyncFuture returnFuture = new CreateChildFileAsyncFuture();
    	createChildFileAsync(name, initialForkData, exclusive, returnFuture);
		return returnFuture;
	}

	@Override
	public <A> void createChildFileAsync(String name,
			HashMap<String, CASIDDataDescriptor> initialForkData,
			boolean exclusive,
			AsyncCompletion<CreateFileInfo, ? super A> completionHandler,
			A attachment) throws IOException, PermissionDeniedException,
			FileExistsException, RemoteException
	{
		CreateChildFileAsyncFuture future = new CreateChildFileAsyncFuture(completionHandler, attachment);
		createChildFileAsync(name, initialForkData, exclusive, future);
	}

	class CreateChildFileCompletionHandler implements AsyncCompletion<IndelibleFileNode, Void>
	{
		private IndelibleDirectoryNode parent;
		private boolean tempTransactionOpen;
		private HashMap<String, CASIDDataDescriptor> initialForkData;
		private CreateChildFileAsyncFuture future;
		int finishedForks = 0;
		private IndelibleFileNode newFile;
		private Throwable writeException = null;
		
		public CreateChildFileCompletionHandler(IndelibleDirectoryNode parent, HashMap<String, CASIDDataDescriptor> initialForkData, boolean tempTransactionOpen, CreateChildFileAsyncFuture future)
		{
			this.parent = parent;
			this.initialForkData = initialForkData;
			this.tempTransactionOpen = tempTransactionOpen;
			this.future = future;
		}
		
		@Override
		public void completed(IndelibleFileNode newFile, Void attachment)
		{
			try
			{
				this.newFile = newFile;
				// TODO - figure out if these semantics are reasonable.  Perhaps createChild should simply always be
				// exclusive (fail if file already exists) and we should add a new retrieve + create call
				for (String existingFork:newFile.listForkNames())
				{
					try
					{
						newFile.deleteFork(existingFork);
					} catch (ForkNotFoundException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}
				}
				for (String curForkName:initialForkData.keySet())
				{
					IndelibleFSForkIF curFork;
					try
					{
						curFork = newFile.getFork(curForkName, true);
					} catch (ForkNotFoundException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						throw new IOException("Could not open fork "+curForkName);
					}
					curFork.writeDataDescriptorAsync(0, initialForkData.get(curForkName), new CreateChildFileCompletionHandler2(this, curFork, curForkName), null);
				}
				
			} catch (PermissionDeniedException e)
			{
				failed(e, null);
			} catch (IOException e)
			{
				failed(e, null);
			}
		}

		public void forkWriteFinished(String forkName)
		{
			finishedForks ++;
			if (finishedForks == initialForkData.size())
			{
				allForksWritten();
			}
		}

		private void allForksWritten()
		{
			if (writeException == null)
			{
				modified();
				CreateFileInfo returnInfo = new CreateFileInfo(parent, newFile);
				if (tempTransactionOpen)
				{
					try
					{
						volume.getConnection().commit();
					} catch (IOException e)
					{
						failed(e, null);
					}
					tempTransactionOpen = false;
				}
				future.completed(returnInfo, null);
			}
			else
			{
				future.failed(writeException, null);
			}
		}
		@Override
		public void failed(Throwable exc, Void attachment)
		{
			future.failed(exc, null);
		}

		public void forkWriteFailed(Throwable e, String forkName)
		{
			finishedForks++;
			writeException = e;	// If we get multiple exceptions writing, we'll only get the last exception.  Oh well
			if (finishedForks == initialForkData.size())
			{
				allForksWritten();
			}
		}
		
	}
	
	class CreateChildFileCompletionHandler2 implements AsyncCompletion<Void, Void>
	{
		private CreateChildFileCompletionHandler parent;
		private IndelibleFSForkIF writeFork;
		private String forkName;
		
		public CreateChildFileCompletionHandler2(CreateChildFileCompletionHandler parent, IndelibleFSForkIF writeFork, String forkName)
		{
			this.parent = parent;
			this.writeFork = writeFork;
			this.forkName = forkName;
		}
		@Override
		public void completed(Void result, Void attachment)
		{
			try
			{
				writeFork.flush();
				parent.forkWriteFinished(forkName);
			} catch (IOException e)
			{
				parent.forkWriteFailed(e, forkName);
			}

		}

		@Override
		public void failed(Throwable exc, Void attachment)
		{
			parent.forkWriteFailed(exc, forkName);
		}
		
	}
	private void createChildFileAsync(String name,
			HashMap<String, CASIDDataDescriptor> initialForkData,
			boolean exclusive, CreateChildFileAsyncFuture future) throws IOException, PermissionDeniedException,
			FileExistsException, RemoteException
	{
    	logger.debug(new DebugLogMessage("createChildFile called on {0} to create child ''{1}'' with initialForkData", getObjectID(), name));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	createChildCoreAsync(name, exclusive, new CreateChildFileCompletionHandler(this, initialForkData, tempTransactionOpen, future), null);
    	// After creatChildCoreAsync finishes the remainer of the logic is in CreateChildFileCompletionHandler.completed
	}
	
	private IndelibleFileNode createChildCore(String name, boolean exclusive)
            throws FileExistsException, IOException
    {
        IndelibleFSObjectID childObjectID = children.get(name);
		if (childObjectID != null)
        {
        	if (exclusive)
        		throw new FileExistsException(name);
        	try
			{
				return volume.getObjectByID(childObjectID);
			} catch (ObjectNotFoundException e)
			{
				throw new IOException("Could not retrieve existing file "+name+" oid = "+childObjectID);
			}
        }
        IndelibleFileNode newFile = (IndelibleFileNode)volume.createNewFile();
        newFile.addParent(this, name);
        children.put(name, newFile.objectID);
        changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryAdd, name, newFile.objectID, volume.getConnection().getVersion()));
        return newFile;
    }
    
	class CreateChildCoreCompletionHandler implements AsyncCompletion<IndelibleFileNodeIF, Void>
	{
		private IndelibleDirectoryNode parent;
		private String name;
		private AsyncCompletion<IndelibleFileNode, Object>completionHandler;
		private Object attachment;
		
		@SuppressWarnings("unchecked")
		public <A>CreateChildCoreCompletionHandler(IndelibleDirectoryNode parent, String name, AsyncCompletion<IndelibleFileNode, A>completionHandler,
				A attachment)
		{
			this.parent = parent;
			this.name = name;
			this.completionHandler = (AsyncCompletion<IndelibleFileNode, Object>) completionHandler;
			this.attachment = attachment;
		}
		
		@Override
		public void completed(IndelibleFileNodeIF result, Void attachment)
		{
			try
			{
				IndelibleFileNode newFile = (IndelibleFileNode)volume.createNewFile();
				newFile.addParent(parent, name);
				children.put(name, newFile.objectID);
				changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryAdd, name, newFile.objectID, volume.getConnection().getVersion()));
				completionHandler.completed(newFile, this.attachment);
			} catch (IOException e)
			{
				failed(e, attachment);
			}
		}

		@Override
		public void failed(Throwable exc, Void attachment)
		{
			completionHandler.failed(exc, this.attachment);
		}
		
	}
	
	private <A>void createChildCoreAsync(String name, boolean exclusive, AsyncCompletion<IndelibleFileNode, A>completionHandler, A attachment)
            throws FileExistsException, IOException
    {
        IndelibleFSObjectID childObjectID = children.get(name);
		CreateChildCoreCompletionHandler completionHandler2 = new CreateChildCoreCompletionHandler(this, name, completionHandler, attachment);
		if (childObjectID != null)
        {
        	if (exclusive)
        		throw new FileExistsException(name);
        	try
			{
				volume.getObjectByIDAsync(childObjectID, volume.getConnection().getDefaultVersion(), RetrieveVersionFlags.kNearest,
						completionHandler2, null);
			} catch (ObjectNotFoundException e)
			{
				throw new IOException("Could not retrieve existing file "+name+" oid = "+childObjectID);
			}
        }    
		else
		{
			IndelibleFileNode newFile = (IndelibleFileNode)volume.createNewFile();
			completionHandler2.completed(newFile, null);
		}
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#createChildFile(java.lang.String, com.igeekinc.indelible.indeliblefs.core.IndelibleFileNode, boolean)
	 */
    @Override
	public CreateFileInfo createChildFile(String name, IndelibleFileNodeIF sourceFile, boolean exclusive) 
    throws PermissionDeniedException, FileExistsException, IOException, NotFileException
    {
    	logger.debug(new DebugLogMessage("createChildFile called on {0} to create child ''{1}'' with sourceFile {2}", getObjectID(), name, sourceFile.getObjectID()));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		if (sourceFile.isDirectory())
    			throw new NotFileException();
    		IndelibleFileNode newFile = (IndelibleFileNode)createChildCore(name, exclusive);
    		IndelibleFSForkIF [] sourceForks = ((IndelibleFileNode)sourceFile).getForks();
    		for (IndelibleFSForkIF curFork:sourceForks)
    		{
    			newFile.createOrReplaceFork(curFork.getName(), curFork);
    		}
    		String [] mdResourceNames = sourceFile.listMetaDataResources();
    		for (String curMDResourceName:mdResourceNames)
    		{
    			HashMap<String, Object>curMD = sourceFile.getMetaDataResource(curMDResourceName);
    			newFile.setMetaDataResource(curMDResourceName, curMD);
    		}
    		modified();;
    		CreateFileInfo returnInfo = new CreateFileInfo(this, newFile);
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#createChildSymlink(java.lang.String, java.lang.String, boolean)
	 */
    @Override
	public synchronized CreateSymlinkInfo createChildSymlink(String name, String targetPath, boolean exclusive)
    throws PermissionDeniedException, FileExistsException, IOException
    {
    	logger.debug(new DebugLogMessage("createChildSymlink called on {0} to create child ''{1}'' with targetPath {2}", getObjectID(), name, targetPath));

    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		IndelibleFSObjectID existingOID = children.get(name);
			if (existingOID != null)
    		{
    			if (exclusive)
    				throw new FileExistsException(name);
    			volume.deleteObjectByID(existingOID);
    			children.remove(name);
    		}
    		IndelibleSymlinkNode newLink = volume.createNewSymlink(targetPath);
    		newLink.addParent(this, name);
    		children.put(name, newLink.objectID);
    		changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryAdd, name, newLink.objectID, volume.getConnection().getVersion()));
    		modified();;
    		CreateSymlinkInfo returnInfo = new CreateSymlinkInfo(this, newLink);
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }
    
    
    
    @Override
	public CreateFileInfo createChildLink(String name,
			IndelibleFileNodeIF sourceFile)
			throws PermissionDeniedException, FileExistsException, IOException,
			NotFileException, ObjectNotFoundException
	{
    	return createChildLinkInternal(name, sourceFile, false);
	}
    
	protected CreateFileInfo createChildLinkInternal(String name,
					IndelibleFileNodeIF sourceFile, boolean allowDirectoryLinks)
							throws PermissionDeniedException, FileExistsException, IOException,
							NotFileException, ObjectNotFoundException
	{
    	logger.debug(new DebugLogMessage("createChildLink called on {0} to create child ''{1}'' with original object {2}", getObjectID(), name, sourceFile.getObjectID()));
    	if (!sourceFile.getVolume().equals(getVolume()))
    		throw new ObjectNotFoundException(sourceFile.getObjectID() +" does not exist in volume");
    	if (!allowDirectoryLinks && sourceFile.isDirectory())
    		throw new NotFileException();
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		IndelibleFSObjectID existingOID = children.get(name);
			if (existingOID != null)
    		{
				throw new FileExistsException(name);
    		}
			((IndelibleFileNode)sourceFile).addParent(this, name);
    		children.put(name, sourceFile.getObjectID());
    		changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryAdd, name, sourceFile.getObjectID(), volume.getConnection().getVersion()));
    		modified();;
    		CreateFileInfo returnInfo = new CreateFileInfo(this, sourceFile);
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
	}

	/* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeRemote#createChildDirectory(java.lang.String)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#createChildDirectory(java.lang.String)
	 */
    @Override
	public synchronized CreateDirectoryInfo createChildDirectory(String name)
    throws IOException, PermissionDeniedException, FileExistsException
    {
    	logger.debug(new DebugLogMessage("createChildDirectory called on {0} to create child ''{1}''", getObjectID(), name));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		if (children.get(name) != null)
    		{
    			logger.error(new DebugLogMessage("Child ''{0}'' already exists in {1}", name, getObjectID()));
    			throw new FileExistsException(name);
    		}
    		IndelibleDirectoryNode newDirectory = (IndelibleDirectoryNode)volume.createNewDirectory();
    		newDirectory.addParent(this, name);
    		children.put(name, newDirectory.objectID);
    		changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryAdd, name, newDirectory.objectID, version));
    		modified();
    		CreateDirectoryInfo returnInfo = new CreateDirectoryInfo(this, newDirectory);
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return returnInfo;
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeRemote#deleteChild(java.lang.String)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#deleteChild(java.lang.String)
	 */
    @Override
	public synchronized DeleteFileInfo deleteChild(String name)
    throws IOException, PermissionDeniedException, CannotDeleteDirectoryException
    {
    	logger.debug(new DebugLogMessage("deleteChild called on {0} to delete child ''{1}''", getObjectID(), name));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		IndelibleFSObjectID childID = children.get(name);
    		if (children.remove(name) == null)
    		{
    			return new DeleteFileInfo(this, false);
    		}
    		try
			{
				IndelibleFileNode child = volume.getObjectByID(childID);
				if (child.isDirectory())
					throw new CannotDeleteDirectoryException();
				child.removeParent(this, name);
				changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryDelete, name, childID, version));
				child.setDirty();	// We set the child dirty but NOT modified because the modified date should not be changed
			} catch (ObjectNotFoundException e)
			{
				logger.error(new ErrorLogMessage("Could not find node to be deleted name = {0}, oid = {1}",
						new Serializable[]{name, childID}));
			}
    		modified();
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return new DeleteFileInfo(this, true);
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#deleteChildDirectory(java.lang.String)
	 */
    @Override
	public synchronized DeleteFileInfo deleteChildDirectory(String name)
    throws IOException, PermissionDeniedException, NotDirectoryException
    {
    	logger.debug(new DebugLogMessage("deleteChildDirectory called on {0} to delete child ''{1}''", getObjectID(), name));
    	boolean tempTransactionOpen = false;
    	if (!volume.getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		volume.getConnection().startTransaction();
    		tempTransactionOpen = true;
    	}
    	try
    	{
    		IndelibleFSObjectID childID = children.get(name);
    		if (children.remove(name) == null)
    			return new DeleteFileInfo(this, false);
    		try
    		{
    			IndelibleFileNode child = volume.getObjectByID(childID);
    			if (!child.isDirectory())
    				throw new NotDirectoryException();
    			child.removeParent(this, name);
    			changes.add(new IndelibleDirectoryChange(ChangeType.kDirectoryEntryDelete, name, childID, version));
    			child.setDirty();	// Set the child dirty so it gets flushed, but NOT modified
    		} catch (ObjectNotFoundException e)
    		{
    			logger.error(new ErrorLogMessage("Could not find node to be deleted name = {0}, oid = {1}",
    					new Serializable[]{name, childID}));
    		}
    		modified();
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().commit();
    			tempTransactionOpen = false;
    		}
    		return new DeleteFileInfo(this, true);
    	}
    	finally
    	{
    		if (tempTransactionOpen)
    		{
    			volume.getConnection().rollback();
    		}
    	}
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeRemote#list()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#list()
	 */
    @Override
	public synchronized String [] list()
    throws IOException
    {
        String [] returnList = new String[children.size()];
        returnList = (String [])children.keySet().toArray(returnList);
        return(returnList);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#getNumChildren()
	 */
    @Override
	public int getNumChildren()
    {
    	return children.size();
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeRemote#getChildNode(java.lang.String)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#getChildNode(java.lang.String)
	 */
    @Override
	public synchronized IndelibleFileNode getChildNode(String name)
    throws IOException, ObjectNotFoundException
    {
        IndelibleFSObjectID childID = children.get(name);
        if (childID == null)
        	throw new ObjectNotFoundException();
        return(volume.getObjectByID(childID));
    }

    protected synchronized IndelibleDirectoryChange [] getChanges()
    {
        return changes.toArray(new IndelibleDirectoryChange[changes.size()]);
    }
    
    protected synchronized void clearChanges()
    {
        changes.clear();
    }
    
    protected synchronized IndelibleDirectoryChange [] getChangesAndClear()
    {
        IndelibleDirectoryChange [] returnChanges = changes.toArray(new IndelibleDirectoryChange[changes.size()]);
        changes.clear();
        return returnChanges;
    }
    
    private void readObject(ObjectInputStream ois)
    throws ClassNotFoundException, IOException
    {
    	ois.defaultReadObject();
    	changes = new ArrayList<IndelibleDirectoryChange>();
    }

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#isDirectory()
	 */
	@Override
	public boolean isDirectory() 
	{
		return true;
	}
    
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNodeIF#getChildNodeInfo(java.lang.String[])
	 */
	@Override
	public IndelibleNodeInfo [] getChildNodeInfo(String [] mdToRetrieve) throws IOException, PermissionDeniedException, RemoteException
	{
		ArrayList<IndelibleNodeInfo> returnInfoList = new ArrayList<IndelibleNodeInfo>();
		String [] childNames = list();
		for (String curChildName:childNames)
		{
			IndelibleFSObjectID childID = children.get(curChildName);
			if (childID != null)
			{
				HashMap<String, HashMap<String, Object>> curChildMetaData = new HashMap<String, HashMap<String,Object>>();
				if (mdToRetrieve != null)
				{
					try
					{
						IndelibleFileNode curChildNode = getChildNode(curChildName);
						if (curChildNode != null)
						{
							for (String curMDName:mdToRetrieve)
							{
								HashMap<String,Object> curMD = curChildNode.getMetaDataResource(curMDName);
								if (curMD != null)
									curChildMetaData.put(curMDName, curMD);
							}
						}
					} catch (Exception e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						continue;	// Skip this node
					}
				}
				IndelibleNodeInfo curChildNodeInfo = new IndelibleNodeInfo(childID, curChildName, curChildMetaData);
				returnInfoList.add(curChildNodeInfo);
			}
		}
		IndelibleNodeInfo [] returnInfo = new IndelibleNodeInfo[returnInfoList.size()];
		returnInfo = returnInfoList.toArray(returnInfo);
		return returnInfo;
	}
	
	public IndelibleDirectoryNode getVersion(IndelibleVersion version,
			RetrieveVersionFlags flags) throws IOException
	{
		return (IndelibleDirectoryNode)super.getVersion(version, flags);
	}
}
