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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.exceptions.InTransactionException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSManagerConnectionIF;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSManagerConnection implements IndelibleFSManagerConnectionIF
{
    private IndelibleFSManager manager;
    private CASServerConnection casServerConnection;
    private Hashtable<IndelibleFSObjectID, CASCollectionID> volumesForTransaction;
    private boolean inTransaction;
    private SessionAuthentication sessionAuthentication;
    private ArrayList<IndelibleFSVolume> volumesToFlush = new ArrayList<IndelibleFSVolume>();
    private IndelibleVersion defaultVersion;	// Version to use outside of a transaction
    private ArrayList<IndelibleFSObject>openObjects = new ArrayList<IndelibleFSObject>();
    
    protected IndelibleFSManagerConnection(IndelibleFSManager manager, CASServerConnection casServerConnection)
    {
        this.manager = manager;
        this.casServerConnection = casServerConnection;
        defaultVersion = IndelibleVersion.kLatestVersion;
    }
    
    protected CASServerConnectionIF getCASConnection()
    {
        return casServerConnection;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#createVolume(java.util.Properties)
	 */
	public IndelibleFSVolumeIF createVolume(Properties volumeProperties)
    throws IOException, PermissionDeniedException
    {
        return manager.createVolume(this, volumeProperties);
    }
	
	public void deleteVolume(IndelibleFSObjectID deleteVolumeID)
		    throws VolumeNotFoundException, PermissionDeniedException, IOException
	{
		manager.deleteVolume(this, deleteVolumeID);
	}
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#retrieveVolume(com.igeekinc.indelible.oid.IndelibleFSObjectID)
	 */

	public synchronized IndelibleFSVolumeIF retrieveVolume(IndelibleFSObjectID retrieveVolumeID)
    throws VolumeNotFoundException, IOException
    {
        return manager.retrieveVolume(this, retrieveVolumeID);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#listVolumes()
	 */

	public synchronized IndelibleFSObjectID [] listVolumes() throws IOException
    {
        return manager.listVolumes(this);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#startTransaction()
	 */
	public synchronized void startTransaction() throws IOException
    {
        manager.startTransaction(this);
        inTransaction = true;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#commit()
	 */
	public synchronized IndelibleVersion commit() throws IOException
    {
        if (!inTransaction)
            throw new InternalError("Not in a transaction");
        IndelibleVersion returnVersion = manager.commit(this);
        inTransaction = false;
        return returnVersion;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#commitAndSnapshot(java.util.HashMap)
	 */
	public synchronized IndelibleVersion commitAndSnapshot(HashMap<String, Serializable>snapshotMetadata) throws IOException, PermissionDeniedException
    {
        if (!inTransaction)
            throw new InternalError("Not in a transaction");
        IndelibleVersion returnVersion = manager.commitAndSnapshot(this, snapshotMetadata);
        inTransaction = false;
        return returnVersion;
    }
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#rollback()
	 */
	public void rollback() throws IOException
    {
        if (!inTransaction)
            throw new InternalError("Not in a transaction");
        manager.rollback(this);
        inTransaction = false;
    }

    protected Hashtable<IndelibleFSObjectID, CASCollectionID> getVolumesForTransaction()
    {
        return volumesForTransaction;
    }

    protected void setVolumesForTransaction(Hashtable<IndelibleFSObjectID, CASCollectionID> volumesForTransaction)
    {
        this.volumesForTransaction = volumesForTransaction;
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#close()
	 */
	public void close() throws IOException
    {
    	if (inTransaction)
    	{
    		try
			{
				rollback();
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
    	}
        manager.close(this);
    }

    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#getVersion()
	 */
	public IndelibleVersion getVersion()
    {
        return casServerConnection.getVersionForTransaction();
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#inTransaction()
	 */
	public synchronized boolean inTransaction()
    {
        return inTransaction;
    }

    public NetworkDataDescriptor registerDataDescriptor(CASIDDataDescriptor descriptorToRegister)
    {
        return casServerConnection.getDataMoverSession().registerDataDescriptor(descriptorToRegister);
    }
    
    public SessionAuthentication getSessionAuthentication()
    {
    	return sessionAuthentication;
    }

    /**
     * Should only be set by IndelibleFSManager
     * @param sessionAuthentication
     */
	protected void setSessionAuthentication(SessionAuthentication sessionAuthentication)
	{
		this.sessionAuthentication = sessionAuthentication;
	}

	public void addVolumeToFlush(IndelibleFSVolume flushVolume)
	{
		synchronized(volumesToFlush)
		{
			if (!volumesToFlush.contains(flushVolume))
				volumesToFlush.add(flushVolume);
		}
	}
	
	public IndelibleFSVolume [] getVolumesToFlushAndClear()
	{
		synchronized(volumesToFlush)
		{
			IndelibleFSVolume [] returnVolumes = new IndelibleFSVolume[volumesToFlush.size()];
			returnVolumes = volumesToFlush.toArray(returnVolumes);
			volumesToFlush.clear();
			return returnVolumes;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#getDefaultVersion()
	 */
	public IndelibleVersion getDefaultVersion()
	{
		return defaultVersion;
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleServerConnectionIF#setDefaultVersion(com.igeekinc.indelible.indeliblefs.core.IndelibleVersion)
	 */
	public synchronized void setDefaultVersion(IndelibleVersion defaultVersion) throws InTransactionException
	{
		if (inTransaction)
			throw new InTransactionException("Can't set default version during a transaction");
		this.defaultVersion = defaultVersion;
	}
	
	public synchronized IndelibleFSObject reference(IndelibleFSObject objectToReference)
	{
		openObjects.add(objectToReference);
		return objectToReference;
	}
}
