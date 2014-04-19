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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.TreeMap;

import com.igeekinc.indelible.indeliblefs.bufcache.AbstractBufferCache;
import com.igeekinc.indelible.indeliblefs.bufcache.Buffer;

public class FileBufferCache extends AbstractBufferCache<Long>
{
    File cachedFile;
    RandomAccessFile io;
    TreeMap<Long, WeakReference<Buffer>> buffers;
    Allocator allocator;
    /**
     * 
     */
    public FileBufferCache(File inCachedFile, Allocator inAllocator, boolean readOnly)
    throws IOException
    {
        cachedFile = inCachedFile;
        String mode;
        if (readOnly)
            mode="r";
        else
            mode="rw";
        io = new RandomAccessFile(inCachedFile, mode);
        buffers = new TreeMap<Long, WeakReference<Buffer>>();
        allocator = inAllocator;
        allocator.setBufferCache(this);
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.bufcache.BufferCache#getBuffer(int, long)
     */
    public synchronized Buffer getBuffer(Long offset, int size) throws IOException
    {
        Long start = offset;
        Long end = new Long(offset + size);
        Buffer returnBuffer = null;
        WeakReference<Buffer> bufferRef = buffers.get(start);
        if (bufferRef != null)
            returnBuffer = (Buffer)bufferRef.get();

        if (returnBuffer != null)
        {
            if (returnBuffer.getSize() != size)
                throw new IOException("Size requested does not match existing buffer");
        }
        else
        {
            if (!buffers.subMap(start, end).isEmpty())
                throw new IOException("Requested buffer overlaps");
            returnBuffer = new OffsetBuffer(offset, size, this);
            if (offset+size <= io.length())
            {
                io.seek(offset);
                io.read(returnBuffer.getBytes());
            }
            bufferRef = new WeakReference<Buffer>(returnBuffer);
            buffers.put(start, bufferRef);
            buffers.put(end, bufferRef);
        }
        return returnBuffer;
    }

    public synchronized Buffer getBuffer(int size)
    throws IOException
    {
        long offset = allocator.allocateSpace(size);
        return getBuffer(offset, size);
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.bufcache.BufferCache#flush()
     */
    public synchronized void flush() throws IOException
    {
        Iterator<WeakReference<Buffer>> bufferIterator = buffers.values().iterator();
        while (bufferIterator.hasNext())
        {
            Buffer flushBuffer = null;
            WeakReference<Buffer> bufferRef = bufferIterator.next();
            if (bufferRef != null)
                flushBuffer = (Buffer)bufferRef.get();

            if (flushBuffer != null && flushBuffer.isDirty())
                flushBuffer(flushBuffer);
        }
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.bufcache.BufferCache#flushBuffer(com.igeekinc.indelible.indeliblefs.bufcache.Buffer)
     */
    public synchronized void flushBuffer(Buffer bufferToFlush) throws IOException
    {
        if (bufferToFlush.getCache() != this)
            throw new IllegalArgumentException("Asking to flush a buffer from a different cache");
        bufferToFlush.forWriting();
        if (bufferToFlush.isDirty())
        {
            io.seek(((OffsetBuffer)bufferToFlush).getOffset());
            io.write(bufferToFlush.getBytes());
            bufferToFlush.setDirty(false);
        }
        bufferToFlush.release();
    }

}
