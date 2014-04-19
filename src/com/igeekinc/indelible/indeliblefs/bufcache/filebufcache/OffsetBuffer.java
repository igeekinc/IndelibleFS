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
 
package com.igeekinc.indelible.indeliblefs.bufcache.filebufcache;

import com.igeekinc.indelible.indeliblefs.bufcache.Buffer;
import com.igeekinc.indelible.indeliblefs.bufcache.BufferCache;
import com.igeekinc.util.rwlock.LockNotHeld;

/**
 * This is a buffer that holds a byte array that is at some offset within
 * the containing structure
 * @author David Smith-Uchida
 */
public class OffsetBuffer extends Buffer
{
    private Object id;
    OffsetBuffer(long offset, int size, BufferCache cache)
    {
    	super(size, cache);
        setOffset(offset);
    }

	public long getOffset()
    {
        return ((Long)id).longValue();
    }
    
    public void setOffset(long offset)
    {
        if (!lock.holdsWriteLock())
            throw new LockNotHeld();
        this.id = offset;
        dirty = true;
    }
}
