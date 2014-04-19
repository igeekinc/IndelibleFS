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

import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Future;

import org.perf4j.LoggingStopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.util.DirectMemoryUtils;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.objectcache.LRUQueue;
import com.igeekinc.util.perf.MBPerSecondLog4jStopWatch;

public class IndelibleFSCASFork extends IndelibleFSFork
{
    private static final String	kStopWatchName	= IndelibleFSCASFork.class.getName();
	private static final long serialVersionUID = 3257286928859674166L;
    private ArrayList<CASSegmentID> segments;
    private transient CASCollectionConnection casCollection;

    private transient long bufferStart;
    private transient LRUQueue<Long, CASIDDataDescriptor>cache = new LRUQueue<Long, CASIDDataDescriptor>(8);
    
    private transient WeakReference<ByteBuffer> cleanBuffer;
    private transient ByteBuffer dirtyBuffer;
    private transient long dirtyBufferStart;
    private transient CASSegmentID cleanBufferSegmentID;
    private transient CASIdentifier cleanBufferCASID;
    long cleanBufferOffset = -1;
    int cleanBufferSize = 0;
    private static final int kPreferredBlockSize = 1024*1024;
    // TODO - we will max out at 1024*1024 * 2GB - probably OK for the moment.  Need to look into variable size blocking for
    // very large files
    
    public IndelibleFSCASFork(IndelibleFileNode parent, String name)
    {
        super(name);
        segments = new ArrayList<CASSegmentID>();
        length = 0;
        this.parent = parent;
    }
    
    protected IndelibleFSCASFork(IndelibleFileNode parent, String name, IndelibleFSCASFork sourceFork)
    {
        super(name);
        synchronized(sourceFork)
        {
            ArrayList<CASSegmentID> sourceSegments = sourceFork.segments;
            segments = new ArrayList<CASSegmentID>(sourceSegments.size());
            for (int curSegmentNum = 0; curSegmentNum < sourceSegments.size(); curSegmentNum++)
            {
                segments.add(sourceSegments.get(curSegmentNum));
                changes.add(new IndelibleForkChange(IndelibleForkChange.ChangeType.kForkEntryAdd, curSegmentNum, sourceSegments.get(curSegmentNum), parent.version));
            }
            length = sourceFork.length;
        }
        parent.setDirty();
        clearBuffer();
        this.parent = parent;
    }
    public synchronized void setCASCollection(CASCollectionConnection inCASCollection)
    {
        casCollection = inCASCollection;
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSStream#getDataDescriptor(long, long)
     */
    public synchronized CASIDDataDescriptor getDataDescriptor(long offset, long readLength)
    throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".getDataDescriptor");
        if (offset > length)
            throw new EOFException((offset)+" > stream length "+length);
        if (offset + readLength > length)
            readLength = length - offset;
        //byte [] data = new byte[(int)readLength];
        ByteBuffer data = ByteBuffer.allocate((int)readLength);
        Log4JStopWatch readWatch = new Log4JStopWatch(kStopWatchName+".getDataDescriptor.getData");
        read(offset, data);
        data.position(0);
        readWatch.stop();
        Log4JStopWatch dataDescriptorCreateWatch = new Log4JStopWatch(kStopWatchName+".getDataDescriptor.dataDescriptorCreate");
        CASIDMemoryDataDescriptor returnDescriptor = null;
        if (offset % kPreferredBlockSize == 0 && readLength == getBuffer().limit() && cleanBuffer.get() != null && cleanBufferSegmentID != null)
        {
        	// TODO - we should really be getting a memory mapped buffer all the way up from CASStore
        	//ByteBuffer copyBuffer = ByteBuffer.allocateDirect(kPreferredBlockSize);
        	ByteBuffer copyBuffer = DirectMemoryUtils.allocateDirect(kPreferredBlockSize);
        	copyBuffer.put(getBuffer());
        	copyBuffer.flip();
        	// Skip the CASID generation
        	returnDescriptor = new CASIDMemoryDataDescriptor(cleanBufferCASID, copyBuffer);
        }
        else
        {
        	returnDescriptor = new CASIDMemoryDataDescriptor(data);
        }
        dataDescriptorCreateWatch.stop();
        wholeWatch.bytesProcessed(readLength);
        wholeWatch.stop();
        return returnDescriptor;
    }


	@Override
	public Future<CASIDDataDescriptor> getDataDescriptorAsync(long offset,
			long length) throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <A> void getDataDescriptorAsync(long offset, long length,
			AsyncCompletion<Void, A> completionHandler, A attachment)
			throws IOException
	{
		// TODO Auto-generated method stub
		
	}
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSStream#read(long, byte[])
     */
    public synchronized int read(long offset, byte[] destBuffer)
    throws IOException
    {
        return read(offset, destBuffer, 0, destBuffer.length);
    }
    


	public synchronized int read(long offset, byte[] destBuffer, int destBufferOffset, int len)
    throws IOException
    {
		ByteBuffer readBuffer = ByteBuffer.wrap(destBuffer, destBufferOffset, len).slice();	// We slice the buffer so that position() will be 0 when destBufferOffset is not 0
		return read(offset, readBuffer);
    }
    
    @Override
	public int read(long offset, ByteBuffer readBuffer) throws IOException
	{
    	if (offset < 0)
    		throw new IllegalArgumentException("Offset cannot be negative");
        if (offset > length)
            return -1;

        int bytesToCopy = readBuffer.limit() - readBuffer.position();

        if (offset + bytesToCopy > length)
            bytesToCopy = (int)(length - offset);

        int bytesRead = 0;
        while (bytesRead < bytesToCopy)
        {
            loadBuffer(offset);
            int startOffset = (int)(offset - bufferStart);  // The offset to start at in the buffer
            int bytesAvailable = getBuffer().limit() - startOffset;
            int bytesThisPass = bytesToCopy - bytesRead;
            if (bytesThisPass > bytesAvailable)
                bytesThisPass = bytesAvailable;
            ByteBuffer thisReadBuffer = getBuffer().duplicate();
            thisReadBuffer.position(startOffset);
            thisReadBuffer.limit(startOffset + bytesThisPass);
            readBuffer.put(thisReadBuffer);
            
            bytesRead += bytesThisPass;
            offset += bytesThisPass;
        }
        if (getBuffer() != null && offset >= bufferStart + getBuffer().limit())
            clearBuffer();  // No point in keeping it around
        return bytesRead;
    }

    
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSStream#writeDataDescriptor(long, com.igeekinc.indelible.indeliblefs.datamover.DataDescriptor)
     */
    public synchronized void writeDataDescriptor(long offset, CASIDDataDescriptor source, boolean releaseSource)
    throws IOException
    {
        writeDataDescriptor(offset, source, 0, (int)source.getLength(), releaseSource);
    }

    
    @Override
	public void writeDataDescriptor(long offset, CASIDDataDescriptor source)
			throws IOException
	{
    	 writeDataDescriptor(offset, source, 0, (int)source.getLength(), false);
	}

	/* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSStream#writeDataDescriptor(long, com.igeekinc.indelible.indeliblefs.datamover.DataDescriptor, long, long)
     */
    public synchronized void writeDataDescriptor(long offset, CASIDDataDescriptor source,
            long sourceOffset, long writeLength, boolean releaseSource)
    throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".writeDataDescriptor");
    	if (offset < 0)
    		throw new IllegalArgumentException("Offset cannot be negative");
    	if (sourceOffset < 0)
    		throw new IllegalArgumentException("sourceOffset cannot be negative");
    	if (writeLength < 0)
    		throw new IllegalArgumentException("writeLength cannot be negative");
    	boolean closeTransaction = false;
    	if (!parent.getVolume().getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		parent.getVolume().getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		long startLength = length;
    		long bytesToWrite = writeLength;
    		loadBuffer(offset);
    		while (bytesToWrite > 0)
    		{
    			int destOffset = (int)(offset % kPreferredBlockSize);
    			int bytesThisPass = kPreferredBlockSize - destOffset;
    			if (bytesThisPass > bytesToWrite)
    				bytesThisPass = (int)bytesToWrite;
    			setBufferDirty(true);	// Set the buffer dirty first so that we will not lose the reference to the buffer to the GC
    			ByteBuffer buffer = getBuffer().slice();
    			buffer.position(destOffset);
    			buffer.limit(destOffset + bytesThisPass);

				int bytesRead = source.getData(buffer, sourceOffset, bytesThisPass, releaseSource);

    			offset += bytesRead;
    			if (offset > length)
    				length = offset;
    			if (bytesToWrite > 0)
    				loadBuffer(offset);	// Should make a check here to see if we are going to write a partial segment
    			else
    				bufferFlush();	// This will leave the buffer empty

    			sourceOffset += bytesRead;
    			bytesToWrite -= bytesRead;
    		}
    		if (parent != null && length > startLength)
    			parent.updateLength(this, startLength, length);
    		parent.setDirty();
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().commit();
    			closeTransaction = false;
    		}
    		wholeWatch.bytesProcessed(writeLength);
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().rollback();
    		}
    		wholeWatch.stop();
    	}
    }
    
	class WriteDataDescriptorFuture extends ComboFutureBase<Void>
	{

		public WriteDataDescriptorFuture()
		{
			super();
			// TODO Auto-generated constructor stub
		}

		public <A> WriteDataDescriptorFuture(
				AsyncCompletion<Void, ? super A> completionHandler, A attachment)
		{
			super(completionHandler, attachment);
		}
	}
	
	@Override
	public Future<Void> writeDataDescriptorAsync(long offset,
			CASIDDataDescriptor source) throws IOException
	{
		WriteDataDescriptorFuture returnFuture = new WriteDataDescriptorFuture();
		writeDataDescriptorAsync(offset, source, returnFuture);
		return returnFuture;
	}

	@Override
	public <A> void writeDataDescriptorAsync(long offset,
			CASIDDataDescriptor source,
			AsyncCompletion<Void, A> completionHandler, A attachment)
			throws IOException
	{
		WriteDataDescriptorFuture future = new WriteDataDescriptorFuture(completionHandler, attachment);
		writeDataDescriptorAsync(offset, source, future);
	}

	protected void writeDataDescriptorAsync(long offset,
			CASIDDataDescriptor source, WriteDataDescriptorFuture future)
	{
		try
		{
			writeDataDescriptor(offset, source);
			future.completed(null, null);
		} catch (Throwable e)
		{
			future.failed(e, null);
		}
	}
	
    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSStream#write(long, byte[])
     */
    public synchronized void write(long offset, byte[] source)
    throws IOException
    {
        write(offset, source, 0, source.length);
    }

    /* (non-Javadoc)
     * @see com.igeekinc.indelible.indeliblefs.IndelibleFSStream#write(long, byte[], int, int)
     */
    public synchronized void write(long offset, byte[] source, int sourceOffset, int writeLength)
    throws IOException
    {
    	ByteBuffer writeBuffer = ByteBuffer.wrap(source, sourceOffset, writeLength).slice();
    	write(offset, writeBuffer);
    }
    public synchronized void write(long offset, ByteBuffer writeBuffer)
    throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".write");
    	boolean closeTransaction = false;
    	ByteBuffer workBuffer = writeBuffer.slice();
    	if (!parent.getVolume().getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		parent.getVolume().getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		long startLength = length;

    		while (workBuffer.remaining() > 0)
    		{
    			int destOffset = (int)(offset % kPreferredBlockSize);
    			int bytesThisPass = kPreferredBlockSize - destOffset;
    			if (bytesThisPass > workBuffer.remaining())
    				bytesThisPass = workBuffer.remaining();
    			ByteBuffer writeFromBuffer;
    			ByteBuffer curBuffer = workBuffer.slice();
    			if (destOffset == 0 && bytesThisPass == kPreferredBlockSize)
    			{
    				// We're writing a whole block so we'll just swap the buffer in
    				writeFromBuffer = curBuffer;
    				writeFromBuffer.limit(bytesThisPass);
        			setBuffer(writeFromBuffer, true);
    			}
    			else
    			{
    				writeFromBuffer = loadBuffer(offset);
    				setBufferDirty(true);	// Set the buffer dirty first so that we will not lose the reference to the buffer to the GC
    				writeFromBuffer.position(destOffset);

    				workBuffer.limit(bytesThisPass);
    				writeFromBuffer.put(workBuffer);
    			}

    			offset += bytesThisPass;
    			if (offset > length)
    				length = offset;
    			bufferFlush();
    		}
    		if (parent != null && length > startLength)
    			parent.updateLength(this, startLength, length);
    		parent.modified();
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().commit();
    			closeTransaction = false;
    		}
    		wholeWatch.bytesProcessed(workBuffer.limit());
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().rollback();
    		}
    		wholeWatch.stop();
    	}
    }

    public synchronized void append(byte[] source, int sourceOffset, int appendLength)
            throws IOException
    {
        write(length, source, sourceOffset, appendLength);
    }
    
    public synchronized void append(byte[] source) throws IOException
    {
        write(length, source, 0, source.length);
    }
    
    public synchronized void appendDataDescriptor(CASIDDataDescriptor source, long sourceOffset,
            long appendLength, boolean releaseSource) throws IOException
    {
        writeDataDescriptor(length, source, sourceOffset, appendLength, releaseSource);
    }
    
    public synchronized void appendDataDescriptor(CASIDDataDescriptor source, long sourceOffset,
            long appendLength) throws IOException
    {
        writeDataDescriptor(length, source, sourceOffset, appendLength, false);
    }
    
    public synchronized void appendDataDescriptor(CASIDDataDescriptor source) throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".appendDataDescriptor");
    	boolean closeTransaction = false;
    	if (!parent.getVolume().getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		parent.getVolume().getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		if (source.getLength() == kPreferredBlockSize && length % kPreferredBlockSize == 0)
    		{
    			appendBlock(source, length, true, false);
    		}
    		else
    		{
    			writeDataDescriptor(length, source);
    		}
    		wholeWatch.bytesProcessed(source.getLength());
    		parent.modified();
    		if (closeTransaction)
    		{
    			Log4JStopWatch commitTransactionWatch = new Log4JStopWatch(kStopWatchName+".appendDataDescriptor.commitTransaction");
    			parent.getVolume().getConnection().commit();
    			closeTransaction = false;
    			commitTransactionWatch.stop();
    		}
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().rollback();
    		}
    		wholeWatch.stop();
    	}
    }

	private void appendBlock(CASIDDataDescriptor source, long startLength, boolean updateLength, boolean segmentExists)
			throws IOException
	{
		MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".appendBlock");
		try
		{
			LoggingStopWatch storeSegmentStopWatch = new Log4JStopWatch(kStopWatchName+".appendBlock.storeSegment");
			CASSegmentID appendSegmentID = casCollection.storeSegment(source).getSegmentID();
			storeSegmentStopWatch.stop();
			segments.add(appendSegmentID);
			int index = (int)(length/kPreferredBlockSize);
			changes.add(new IndelibleForkChange(IndelibleForkChange.ChangeType.kForkEntryAdd, index, appendSegmentID, parent.version));
			length += source.getLength();
			if (updateLength && parent != null && length > startLength)
				parent.updateLength(this, startLength, length);
			parent.setDirty();
		}
		finally
		{
			wholeWatch.bytesProcessed(source.getLength());
			wholeWatch.stop();
		}
	}
    
	/*private CASIDDataDescriptor getData(long offset)
	{
		
	}*/
    private ByteBuffer loadBuffer(long offset)
    throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".loadBuffer");
    	try
    	{
    		boolean bufferNeedsCleaning = true;
    		bufferFlush();
    		ByteBuffer returnBuffer = null;
    		if (cleanBuffer != null)
    			returnBuffer = cleanBuffer.get();
    		if (returnBuffer == null)
    		{
    			returnBuffer = DirectMemoryUtils.allocateDirect(kPreferredBlockSize);
    			setBuffer(returnBuffer, false);
    			bufferStart = -1;
    			bufferNeedsCleaning = false;	// No need to zero it
    		}
    		if (bufferStart > -1 && offset >= bufferStart && offset < bufferStart+kPreferredBlockSize)
    			return returnBuffer;	// The offset requested is within the current buffer


    		int index = (int)(offset/kPreferredBlockSize);
    		CASSegmentID segmentID = null;
    		cleanBufferSegmentID = null;
    		if (index < segments.size())
    		{
    			segmentID = (CASSegmentID)segments.get(index);	
    		}
    		if (segmentID == null)
    		{
    			if (bufferNeedsCleaning)
    			{
    				// No segmentID means a sparse section of the file so we need to return a buffer of all nulls
    				// Fastest way to clear a direct buffer is to just discard and allocate a new one
    				cleanBuffer = null;
    				returnBuffer = DirectMemoryUtils.allocateDirect(kPreferredBlockSize);
    				setBuffer(returnBuffer, false);
    			}
    		}
    		else
    		{
    			DataVersionInfo segmentInfo = casCollection.retrieveSegment(segmentID);
    			CASIDDataDescriptor loadDescriptor = segmentInfo.getDataDescriptor();	// We can ignore the version here
    			if (loadDescriptor == null)
    				throw new IOException("Failed to retrieve segment "+segmentID.toString());
    			ByteBuffer readBuffer = getBufferNoLoad();
    			if (readBuffer == null)
    				readBuffer = DirectMemoryUtils.allocateDirect(kPreferredBlockSize);
    			readBuffer.position(0);
    			loadDescriptor.getData(readBuffer, 0, (int)loadDescriptor.getLength(), true);
    			setBuffer(readBuffer, false);
    			returnBuffer = readBuffer;
    			cleanBufferSegmentID = segmentID;
    			cleanBufferCASID = loadDescriptor.getCASIdentifier();
    			cleanBufferOffset = index*kPreferredBlockSize;

    		}
    		bufferStart = ((long)index * (long)kPreferredBlockSize);
    		return returnBuffer;
    	}
    	finally
    	{
    		wholeWatch.bytesProcessed(kPreferredBlockSize);
    		wholeWatch.stop();
    	}
    }
    
    private CASSegmentID getCleanBufferID()
    {
    	if (dirtyBuffer == null && cleanBuffer.get() != null)
    		return cleanBufferSegmentID;
    	return null;
    }
    
    private void bufferFlush()
    throws IOException
    {
        if (!isBufferDirty())
            return;
        MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".loadBuffer");
    	try
    	{
    		ByteBuffer buffer = getBuffer();
    		if (bufferStart + kPreferredBlockSize > length) // Partial buffer at end
    		{
    			buffer.limit((int)(length - bufferStart));
    		}
    		else
    		{
    			buffer.limit(kPreferredBlockSize);

    		}
    		buffer.position(0);
    		LoggingStopWatch genDescriptorStopWatch = new Log4JStopWatch(kStopWatchName+".loadBuffer.genDescriptor");
    		CASIDDataDescriptor writeDescriptor = new CASIDMemoryDataDescriptor(buffer);
    		CASSegmentID addSegmentID;
    		genDescriptorStopWatch.stop();
    		
    		LoggingStopWatch storeSegmentStopWatch = new Log4JStopWatch(kStopWatchName+".loadBuffer.storeSegment");
    		addSegmentID = casCollection.storeSegment(writeDescriptor).getSegmentID();
    		storeSegmentStopWatch.stop();
    		int index = (int)(bufferStart/kPreferredBlockSize);
    		IndelibleVersion version = null;
    		if (parent != null)
    			version = parent.version;

    		if (index < segments.size())
    		{
    			CASSegmentID removeSegmentID = segments.get(index);
    			changes.add(new IndelibleForkChange(IndelibleForkChange.ChangeType.kForkEntryDelete, index, removeSegmentID, version));
    			segments.set(index, addSegmentID);
    			changes.add(new IndelibleForkChange(IndelibleForkChange.ChangeType.kForkEntryAdd, index, addSegmentID, version));
    		}
    		else
    		{
    			if (index == segments.size())
    			{
    				segments.add(addSegmentID);
    				changes.add(new IndelibleForkChange(IndelibleForkChange.ChangeType.kForkEntryAdd, index, addSegmentID, version));
    			}
    			else
    			{
    				throw new InternalError("Can't handle sparse files yet");
    			}
    		}

    		parent.setDirty();
    		clearBuffer();
    		setBufferDirty(false);
    	}
    	finally
    	{
    		wholeWatch.bytesProcessed(kPreferredBlockSize);
    		wholeWatch.stop();
    	}
    }
    
    public synchronized void flush()
    throws IOException
    {
    	boolean closeTransaction = false;
    	if (!parent.getVolume().getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		parent.getVolume().getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		bufferFlush();
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().commit();
    			closeTransaction = false;
    		}
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().rollback();
    		}
    	}
    }
    
    public synchronized long extend(long extendLength) throws IOException
    {
    	boolean closeTransaction = false;
    	if (!parent.getVolume().getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		parent.getVolume().getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		if (extendLength > length)
    		{
    			long startLength = length;
    			long bytesToExtend = extendLength - length;
    			if (length % kPreferredBlockSize != 0)
    			{
    				int alignBytes = (int)(kPreferredBlockSize - (length % kPreferredBlockSize));
    				if (alignBytes > bytesToExtend)
    					alignBytes = (int)bytesToExtend;
    				byte [] zeroFill = new byte[alignBytes];
    				CASIDDataDescriptor fillDescriptor = new CASIDMemoryDataDescriptor(zeroFill);
    				appendDataDescriptor(fillDescriptor);
    				bytesToExtend -= alignBytes;
    			}

    			if (bytesToExtend >= kPreferredBlockSize)
    			{
    				byte [] zeroBlock = new byte[kPreferredBlockSize];
    				CASIDDataDescriptor zeroBlockDescriptor = new CASIDMemoryDataDescriptor(zeroBlock);
    				casCollection.storeSegment(zeroBlockDescriptor);	// Make sure it's stored at least once
    				while (bytesToExtend >= kPreferredBlockSize)
    				{
    					appendBlock(zeroBlockDescriptor, length, false, true);
    					bytesToExtend -= kPreferredBlockSize;
    				}
    			}

    			if (bytesToExtend > 0)
    			{
    				byte [] finalZeros = new byte[(int)bytesToExtend];
    				CASIDDataDescriptor finalDescriptor = new CASIDMemoryDataDescriptor(finalZeros);
    				appendDataDescriptor(finalDescriptor);
    			}
    			if (parent != null && length > startLength)
    				parent.updateLength(this, startLength, length);
    		}
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().commit();
    			closeTransaction = false;
    		}
    		return length;
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().rollback();
    		}
    	}
    }
    public synchronized long truncate(long truncateLength)
    throws IOException
    {
    	if (truncateLength != 0)
    		throw new IllegalArgumentException("Can only truncate to zero length currently");
    	boolean closeTransaction = false;
    	if (!parent.getVolume().getConnection().inTransaction())
    	{
    		// Create a transaction for this operation and be sure to commit/rollback at exit
    		parent.getVolume().getConnection().startTransaction();
    		closeTransaction = true;
    	}
    	try
    	{
    		// TODO _ fix this!
    		IndelibleVersion version = null;
    		if (parent != null)
    			version = parent.version;
    		long startLength = length;
    		synchronized(segments)
    		{
    			for (int index = 0; index< segments.size(); index++)  
    			{
    				CASSegmentID removeSegmentID = segments.get(index);
    				changes.add(new IndelibleForkChange(IndelibleForkChange.ChangeType.kForkEntryDelete, index, removeSegmentID, version));
    			}
    			segments.clear();
    		}
    		length = 0;
    		if (parent != null)
    			parent.updateLength(this, startLength, length);
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().commit();
    			closeTransaction = false;
    		}
    		return length;
    	}
    	finally
    	{
    		if (closeTransaction)
    		{
    			parent.getVolume().getConnection().rollback();
    		}
    	}
    }
    
    public synchronized CASIdentifier [] getSegmentIDs() throws IOException
    {
    	CASIdentifier [] returnIDs = new CASIdentifier[segments.size()];
    	for (int curSegmentNum = 0; curSegmentNum < segments.size(); curSegmentNum++)
    	{
    		returnIDs[curSegmentNum] = casCollection.retrieveCASIdentifier(segments.get(curSegmentNum));
    	}
    	
        return returnIDs;
    }

	private synchronized ByteBuffer getBuffer() throws IOException
	{
		ByteBuffer returnBuffer = getBufferNoLoad();
		if (returnBuffer == null)	// Must have gotten whacked by the GC
		{
			loadBuffer(bufferStart);
			returnBuffer = getBufferNoLoad();
		}
		returnBuffer.position(0);
		return returnBuffer;
	}

	protected ByteBuffer getBufferNoLoad()
	{
		ByteBuffer returnBuffer = null;
		if (dirtyBuffer != null)
		{
			returnBuffer = dirtyBuffer;
		}
		else
		{
			if (cleanBuffer != null)
				returnBuffer = cleanBuffer.get();
		}
		if (returnBuffer != null)
			returnBuffer.position(0);
		return returnBuffer;
	}

	private synchronized void setBuffer(ByteBuffer buffer, boolean dirty)
	{
		if (dirtyBuffer != null)
			throw new InternalError("Trying to set buffer while buffer is dirty");

		if (dirty)
		{
			dirtyBuffer = buffer;
			cleanBuffer = null;
		}
		else
		{
			dirtyBuffer = null;
			cleanBuffer = new WeakReference<ByteBuffer>(buffer);
		}
	}
	
	private synchronized void clearBuffer()
	{
		cleanBuffer = null;
		dirtyBuffer = null;
	}

	private synchronized boolean isBufferDirty()
	{
		return dirtyBuffer != null;
	}

	private synchronized void setBufferDirty(boolean bufferDirty) throws IOException
	{
		if (bufferDirty)
			dirtyBuffer = getBuffer();
		else
			dirtyBuffer = null;
	}


}
