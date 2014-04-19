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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.oid.IndelibleFSObjectID;


/**
 * Ensures that only one copy of an object will be present in memory at 
 * any time.  
 */
public class ObjectCache implements Runnable
{
    private HashMap<IndelibleFSObjectID, WeakReference<IndelibleFSObject>> cacheObjs;
    private HashMap<WeakReference<IndelibleFSObject>, IndelibleFSObjectID> reverseMap;
    private ReferenceQueue<IndelibleFSObject> refQueue;
    private Thread queueClearThread;
    private Logger logger;
    private CacheBackingStore missHandler;
    
    public ObjectCache()
    {
        this(null);
    }
    /**
     * 
     */
    public ObjectCache(CacheBackingStore inMissHandler)
    {
        cacheObjs = new HashMap<IndelibleFSObjectID, WeakReference<IndelibleFSObject>>();
        reverseMap = new HashMap<WeakReference<IndelibleFSObject>, IndelibleFSObjectID>();
        refQueue = new ReferenceQueue<IndelibleFSObject>();
        logger = Logger.getLogger(getClass());
        missHandler = inMissHandler;
        queueClearThread = new Thread(this, "ObjectCache clear thread");
        queueClearThread.setDaemon(true);
        queueClearThread.start();
    }
    
    /*
     * Objects should all get loaded via the get() routine except
     * for new objects.
     */
    protected synchronized void put(IndelibleFSObjectID key, IndelibleFSObject objectToCache)
    {
        WeakReference<IndelibleFSObject> cacheRef = new WeakReference<IndelibleFSObject>(objectToCache, refQueue);
        cacheObjs.put(key, cacheRef);
        reverseMap.put(cacheRef, key);
    }
    
    public synchronized IndelibleFSObject get(IndelibleFSObjectID key)
    throws IOException
    {
        WeakReference<IndelibleFSObject> cacheRef = cacheObjs.get(key);
        IndelibleFSObject returnObject = null;
        if (cacheRef != null)
        {
            returnObject = (IndelibleFSObject)cacheRef.get();
        }
        if (returnObject == null)
        {
            returnObject = (IndelibleFSObject)handleMiss(key);
            if (returnObject != null)
                put(key, returnObject);
        }
        return(returnObject);
    }

    public synchronized Object remove(IndelibleFSObjectID key)
    {
        WeakReference<IndelibleFSObject> removeRef = cacheObjs.remove(key);
        Object returnObject = null;
        if (removeRef != null)
        {
            returnObject = removeRef.get();
            reverseMap.remove(removeRef);
        }
        return returnObject;
    }
    
    @SuppressWarnings("unchecked")
    public void run()
    {
        while (true)
        {
            try
            {
                WeakReference<IndelibleFSObject> removeRef = (WeakReference<IndelibleFSObject>) refQueue.remove();
                synchronized (this)
                {
                    Object key = reverseMap.get(removeRef);
                    if (key != null)
                        cacheObjs.remove(key);
                }
            } catch (Throwable t)
            {
                logger.error("Caught exception in ObjectCache cleanup loop", t);
            }
        }
    }
    
    /**
     * In the event of a cache miss, this routine is called to load the object
     * from the database, or where ever as necessary
     * @param key
     * @return
     */
    protected Object handleMiss(IndelibleFSObjectID key)
    throws IOException
    {
        if (missHandler != null)
            return missHandler.handleMiss(key);
        return null;
    }
}
