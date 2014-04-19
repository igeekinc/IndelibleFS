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
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;

public abstract class IndelibleFSObject implements Serializable, IndelibleFSObjectIF
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -3935288815800598255L;
	public static void initMapping()
	{
		ObjectIDFactory.addMapping(IndelibleFSObject.class, IndelibleFSObjectID.class);
	}
	protected IndelibleFSObjectID objectID;
	protected IndelibleVersion version;
    private transient boolean dirty; // Object has been modified and needs to be written out
    private HashMap<String, HashMap<String, Object>> metadataHashmap;
    protected transient Logger logger = Logger.getLogger(getClass());
    
    protected IndelibleFSObject()
    {
        metadataHashmap = new HashMap<String, HashMap<String, Object>>();
    }
    
    protected IndelibleFSObject(IndelibleFSObjectID objectID, IndelibleVersion version)
    {
        this.objectID = objectID;
        this.version = version;
        metadataHashmap = new HashMap<String, HashMap<String, Object>>();
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectRemote#getObjectID()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#getObjectID()
	 */
    @Override
	public IndelibleFSObjectID getObjectID()
    {
        return objectID;
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectRemote#listMetaDataResources()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#listMetaDataResources()
	 */
    @Override
	public String [] listMetaDataResources()
    throws PermissionDeniedException
    {
        Set<String> keySet = metadataHashmap.keySet();
        return keySet.toArray(new String[keySet.size()]);
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectRemote#getMetaDataResource(java.lang.String)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#getMetaDataResource(java.lang.String)
	 */
    @Override
	@SuppressWarnings("unchecked")
    public HashMap<String, Object> getMetaDataResource(String mdResourceName)
    throws PermissionDeniedException, IOException
    {
        HashMap<String, Object>retrievedMap = metadataHashmap.get(mdResourceName);
        if (retrievedMap != null)
            return (HashMap<String, Object>)retrievedMap.clone();
        return null;
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#setMetaDataResource(java.lang.String, java.util.HashMap)
	 */
    @Override
	public IndelibleFSObject setMetaDataResource(String mdResourceName, HashMap<String, Object> resources)
    throws PermissionDeniedException, IOException
    {
        metadataHashmap.put(mdResourceName, resources);
        setDirty();
        return this;    // Eventually we need to return a new object
    }
    
    public boolean isDirty()
    {
        return dirty;
    }
    
    protected void setDirty()
    {
        dirty = true;
    }
    
    protected void clearDirty()
    {
        dirty = false;
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException 
    {
    	in.defaultReadObject();
    	logger = Logger.getLogger(getClass());
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#listVersions()
	 */
    @Override
	public abstract IndelibleVersionIterator listVersions() throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#getVersion(com.igeekinc.indelible.indeliblefs.core.IndelibleVersion, com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags)
	 */
    @Override
	public abstract IndelibleFSObject getVersion(IndelibleVersion version, RetrieveVersionFlags flags) throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSObjectIF#getVersion()
	 */
    @Override
	public IndelibleVersion getVersion()
    {
    	return version;
    }
}
