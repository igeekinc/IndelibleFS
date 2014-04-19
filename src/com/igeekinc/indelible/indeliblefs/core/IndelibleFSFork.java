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
import java.util.ArrayList;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;

public abstract class IndelibleFSFork implements Serializable, IndelibleFSForkIF
{
    private static final long serialVersionUID = -2353465236479687111L;
    protected String name;
    protected long length;
    protected transient IndelibleFileNode parent;   // May be null
    protected transient ArrayList<IndelibleForkChange> changes;
    
    public IndelibleFSFork(String name)
    {
        this.name = name;
        changes = new ArrayList<IndelibleForkChange>();
    }
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#getDataDescriptor(long, long)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#getDataDescriptor(long, long)
	 */
    @Override
	public abstract CASIDDataDescriptor getDataDescriptor(long offset, long length)
    	throws IOException;
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#read(long, byte[])
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#read(long, byte[])
	 */
    @Override
	public abstract int read(long offset, byte [] bytesToRead)
    	throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#read(long, byte[], int, int)
	 */
    @Override
	public abstract int read(long offset, byte[] destBuffer, int destBufferOffset, int len)
        throws IOException;
    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#writeDataDescriptor(long, com.igeekinc.indelible.indeliblefs.datamover.DataDescriptor)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#writeDataDescriptor(long, com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor)
	 */
    @Override
	public abstract void writeDataDescriptor(long offset, CASIDDataDescriptor source)
    	throws IOException;
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#write(long, byte[])
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#write(long, byte[])
	 */
    @Override
	public abstract void write(long offset, byte [] source) throws IOException;
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#write(long, byte[], int, int)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#write(long, byte[], int, int)
	 */
    @Override
	public abstract void write(long offset, byte []source, int sourceOffset, int length) throws IOException;
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#appendDataDescriptor(com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor)
	 */
    @Override
	public abstract void appendDataDescriptor(CASIDDataDescriptor source) throws IOException;
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#append(byte[])
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#append(byte[])
	 */
    @Override
	public abstract void append(byte [] source) throws IOException;
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#append(byte[], int, int)
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#append(byte[], int, int)
	 */
    @Override
	public abstract void append(byte [] source, int sourceOffset, int length) throws IOException;

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkRemote#flush()
     */
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#flush()
	 */
    @Override
	public abstract void flush() throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#truncate(long)
	 */
    @Override
	public abstract long truncate(long truncateLength) throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#extend(long)
	 */
    @Override
	public abstract long extend(long extendLength) throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#length()
	 */
    @Override
	public long length()
    {
        return length;
    }
    
    protected void setLength(long newLength)
    throws IOException
    {
        long oldLength = length;
        length = newLength;
        if (parent != null)
        	parent.updateLength(this, oldLength, newLength);
    }
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#getSegmentIDs()
	 */
    @Override
	public abstract CASIdentifier [] getSegmentIDs() throws IOException;
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleFSForkIF#getName()
	 */
    @Override
	public String getName()
    {
        return name;
    }
    
    protected synchronized IndelibleForkChange [] getChanges()
    {
        return changes.toArray(new IndelibleForkChange[changes.size()]);
    }
    
    protected synchronized void clearChanges()
    {
        changes.clear();
    }
    
    protected synchronized IndelibleForkChange [] getChangesAndClear()
    {
        IndelibleForkChange [] returnChanges = changes.toArray(new IndelibleForkChange[changes.size()]);
        changes.clear();
        return returnChanges;
    }
    
    private void readObject(ObjectInputStream ois)
    throws ClassNotFoundException, IOException
    {
        ois.defaultReadObject();
        changes = new ArrayList<IndelibleForkChange>();
    }
}
