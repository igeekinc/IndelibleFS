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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore;

import java.io.IOException;
import java.util.concurrent.Future;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.util.async.AsyncCompletion;

/**
 * A CASStore stores data segments by CASID.  Segments may be deleted but cannot be changed.
 * The CASStore is the basic unit of storage management for Indelible.  All CASStore operations
 * are atomic and are completed when the method call returns.  The CASStore does not support any
 * transaction semantics.
 * 
 * Each CASStore is identified by an ObjectID
 *
 */
public interface CASStore
{
	/**
	 * Returns the identifier for the CAStore
	 * @return
	 */
	public CASStoreID getCASStoreID();
    /**
     * Retrieves the segment identified by this CASIdentifier (checksum)
     * @param segmentID
     * @return
     * @throws IOException
     * @throws SegmentNotFound 
     */
    public CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID) throws IOException, SegmentNotFound;
    public Future<CASIDDataDescriptor> retrieveSegmentAsync(CASIdentifier segmentID) throws IOException, SegmentNotFound;
    public <A> void retrieveSegmentAsync(CASIdentifier segmentID, AsyncCompletion<CASIDDataDescriptor, A> completionHandler, A attachment) throws IOException, SegmentNotFound;
    /**
     * Stores a segment and returns the CASIdentifier that identifies it it.
     * @param segmentDescriptor
     * @return
     * @throws IOException
     */
    public void storeSegment(CASIDDataDescriptor segmentDescriptor) throws IOException;
    public Future<Void> storeSegmentAsync(CASIDDataDescriptor segmentDescriptor) throws IOException;
    public <A> void storeSegmentAsync(CASIDDataDescriptor segmentDescriptor, AsyncCompletion<Void, A> completionHandler, A attachment) throws IOException;
    /**
     * Deletes the segment identified by the segmentID
     * @param segmentID
     * @return 
     * @throws IOException
     */
    public boolean deleteSegment(CASIdentifier segmentID) throws IOException;
    
    /**
     * Returns the number of segments stored
     * @return
     */
    public long getNumSegments();
    
    /**
     * Returns the total amount of space allocated to this store
     * @return bytes allocated
     */
    public long getTotalSpace();
    
    /**
     * Returns the amount of space currently in use
     * @return bytes in use
     */
    public long getUsedSpace();
	public long getFreeSpace();
	
	/*
	 * Syncs to disk
	 */
	public void sync() throws IOException;
	
	/*
	 * Syncs all data structures.  After calling shutdown() the CASStore is no longer usable
	 */
	public void shutdown() throws IOException;
	/**
	 * Rebuilds the store
	 * @throws IOException
	 */
	public void rebuild() throws IOException;
	
	/**
	 * Reloads objects that were stored but are missing from the index (usually
	 * this just new objects added after the last sync)
	 * @throws IOException
	 */
	public void reloadNewObjects() throws IOException;
	/**
	 * Gets the current status of the store 
	 * @return
	 */
	public CASStoreStatus getStatus();
	
	public void addStatusChangeListener(CASStoreStatusChangedListener listener);
	
	public void removeStatusChangeListener(CASStoreStatusChangedListener listener);

	/**
	 * Checks an existing segment and makes sure it exists on disk and is correct
	 * @param identifierToVerify
	 * @return
	 */
	public boolean verifySegment(CASIdentifier identifierToVerify);
	public void repairSegment(CASIdentifier casID, CASIDDataDescriptor dataDescriptor) throws IOException;
} 
