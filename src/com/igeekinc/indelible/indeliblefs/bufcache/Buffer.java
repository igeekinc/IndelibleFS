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
 
package com.igeekinc.indelible.indeliblefs.bufcache;

import java.io.IOException;

import com.igeekinc.util.rwlock.InvalidWaitTime;
import com.igeekinc.util.rwlock.LockNotHeld;
import com.igeekinc.util.rwlock.ReadWriteLock;
import com.igeekinc.util.rwlock.ReadWriteLockImplementation;
import com.igeekinc.util.rwlock.UpgradeNotAllowed;

public abstract class Buffer<I> implements ReadWriteLock
{

	private int	size;
	protected byte []	bytes;
	protected boolean	dirty;
	protected ReadWriteLockImplementation	lock;
	protected BufferCache<I>	cache;

	protected Buffer(int size, BufferCache<I> cache)
	{
		setSize(size);
	    lock = new ReadWriteLockImplementation(true);
	    bytes = new byte[getSize()];
	    dirty = false;
	    this.cache = cache;
	}
	public boolean isDirty()
	{
	    return dirty;
	}

	public void setDirty(boolean dirty)
	{
	    this.dirty = dirty;
	}

	public byte [] getBytes()
	{
	    if (!lock.holdsWriteLock())
	        throw new LockNotHeld();
	    return bytes;
	}

	public void setByte(int offset, byte value)
	{
	    if (!lock.holdsWriteLock())
	        throw new LockNotHeld();
	    bytes[offset] = value;
	    dirty = true;
	}

	public void setBytes(int offset, byte [] values)
	{
	    setBytes(offset, values, 0, values.length);
	}

	public void setBytes(int offset, byte [] values, int valuesPos, int valuesLength)
	{
	    if (!lock.holdsWriteLock())
	        throw new LockNotHeld();
	    System.arraycopy(values, valuesPos, bytes, offset, valuesLength);
	    dirty = true;
	}

	public byte getByte(int offset)
	{
	    if (!holdsReadLock())
	        throw new LockNotHeld();
	    return bytes[offset];
	}

	public void getBytes(int bufOffset, byte [] output, int outputOffset, int length)
	{
	    if (!holdsReadLock())
	        throw new LockNotHeld();
	    System.arraycopy(bytes, bufOffset, output, outputOffset, length);
	}

	public int getSize()
	{
	    return size;
	}

	public boolean downgrade() throws LockNotHeld
	{
	    return lock.downgrade();
	}

	public boolean forReading()
	{
	    return lock.forReading();
	}

	public boolean forReading(int waitTime) throws InvalidWaitTime
	{
	    return lock.forReading(waitTime);
	}

	public boolean forWriting() throws UpgradeNotAllowed
	{
	    return lock.forWriting();
	}

	public boolean forWriting(int waitTime) throws InvalidWaitTime,
			UpgradeNotAllowed
	{
	    return lock.forWriting(waitTime);
	}

	public void release() throws LockNotHeld
	{
	    lock.release();
	}

	public boolean upgrade() throws UpgradeNotAllowed, LockNotHeld
	{
	    return lock.upgrade();
	}

	public boolean upgrade(int waitTime) throws InvalidWaitTime,
			UpgradeNotAllowed, LockNotHeld
	{
	    return lock.upgrade(waitTime);
	}

	public boolean holdsReadLock()
	{
	    return lock.holdsReadLock();
	}

	public boolean holdsWriteLock()
	{
	    return lock.holdsWriteLock();
	}

	protected void finalize() throws Throwable
	{
		flush();
	}
	
	public void flush() throws IOException
	{
	    if (dirty)
	        cache.flushBuffer(this);
	}
	public BufferCache<I> getCache()
	{
	    return cache;
	}

	protected void setSize(int size)
	{
		this.size = size;
	}
}
