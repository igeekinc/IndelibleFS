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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.indelible.indeliblefs.uniblock.BasicDataDescriptorFactory;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.perf.MBPerSecondLog4jStopWatch;

/**
 * FSCASSerialStorageFuture acts both as a Future object and as CompletionHandler.  In its
 * CompletionHandler role, it is called by the DataMover when the data for the segmentDescriptor
 * is available and will then write the data.
 * @author David L. Smith-Uchida
 *
 */
class FSCASSerialStorageFuture extends ComboFutureBase<Integer>
{
	private FSCASSerialStorage storage;
	private CASIDDataDescriptor segmentDescriptor;
	private long dataID;
	private ByteBuffer writeBuffer;
	public FSCASSerialStorageFuture(FSCASSerialStorage storage, CASIDDataDescriptor segmentDescriptor, long dataID)
	{
		this.storage = storage;
		this.segmentDescriptor = segmentDescriptor;
		this.dataID = dataID;
	}
	
	public <A>FSCASSerialStorageFuture(FSCASSerialStorage storage, CASIDDataDescriptor segmentDescriptor, long dataID, AsyncCompletion<Integer, ? super A>completionHandler, A attachment)
	{
		super(completionHandler, attachment);
		this.storage = storage;
		this.segmentDescriptor = segmentDescriptor;
		this.dataID = dataID;
	}
	
	/*
	 * Set the buffer that data will be retrieved into.  This buffer will not be valid until completed is called.
	 */
	public void setWriteBuffer(ByteBuffer writeBuffer)
	{
		this.writeBuffer = writeBuffer;
	}
	/**
	 * This will be called by the DataMover
	 */
	@Override
	public synchronized void completed(Integer result, Object attachment)
	{
		try
		{
			writeBuffer.position(0);
			storage.storeDataSegment(writeBuffer, segmentDescriptor.getCASIdentifier(), dataID, false);
		} catch (Throwable t)
		{
			super.failed(t, attachment);
		}
		super.completed(result, attachment);
	}
	
	
}
public class FSCASSerialStorage extends BasicDataDescriptorFactory
{
    File casStorageRoot;
    FSCASSerialStorageDirectory rootNode;
    long lastDataID;
    Logger logger;
    
    public static final int kDigitsPerDirLevel = 3;    // Just fix it at this for ease of use
    public static final int kFileNameRadix = 10;
    public static final int kFilesPerDirLevel = (int)Math.pow(kFileNameRadix, kDigitsPerDirLevel);
    
    private static final String	kStopWatchName	= FSCASSerialStorage.class.getName();
    public FSCASSerialStorage(File casStorageRoot)
    {
    	logger = Logger.getLogger(getClass());
        this.casStorageRoot = casStorageRoot;
        if (!casStorageRoot.exists())
            casStorageRoot.mkdirs();
        if (!casStorageRoot.exists() || !casStorageRoot.isDirectory())
        {
            throw new IllegalArgumentException(casStorageRoot.getAbsolutePath()+" is not a directory");
        }
        rootNode = new FSCASSerialStorageDirectory(casStorageRoot, 0);
    }
    
    public void storeDataSegment(CASIDDataDescriptor segmentDescriptor, long dataID) throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".storeDataSegment");
    	ByteBuffer writeBuffer = ByteBuffer.allocate((int)segmentDescriptor.getLength());
    	segmentDescriptor.getData(writeBuffer, 0, writeBuffer.capacity(), true);
    	writeBuffer.position(0);
    	storeDataSegment(writeBuffer, segmentDescriptor.getCASIdentifier(), dataID, false);
        wholeWatch.bytesProcessed(segmentDescriptor.getLength());
        wholeWatch.stop();
    }
    
    public void replaceDataSegment(CASIDDataDescriptor segmentDescriptor, long dataID) throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".storeDataSegment");
    	ByteBuffer writeBuffer = ByteBuffer.allocate((int)segmentDescriptor.getLength());
    	segmentDescriptor.getData(writeBuffer, 0, writeBuffer.capacity(), true);
    	writeBuffer.position(0);
    	storeDataSegment(writeBuffer, segmentDescriptor.getCASIdentifier(), dataID, true);
        wholeWatch.bytesProcessed(segmentDescriptor.getLength());
        wholeWatch.stop();
    }
    
    public Future<Integer>storeDataSegmentAsync(CASIDDataDescriptor segmentDescriptor, long dataID) throws IOException
    {
    	FSCASSerialStorageFuture future = new FSCASSerialStorageFuture(this, segmentDescriptor, dataID);
    	storeDataSegmentAsyncCommon(segmentDescriptor, dataID, future);
    	return future;
    }
    
    public <A>void storeDataSegmentAsync(CASIDDataDescriptor segmentDescriptor, long dataID, AsyncCompletion<Integer, A>completionHandler,
    		A attachment) throws IOException
    {
    	FSCASSerialStorageFuture future = new FSCASSerialStorageFuture(this, segmentDescriptor, dataID, completionHandler, attachment);
    	storeDataSegmentAsyncCommon(segmentDescriptor, dataID, future);
    }
    
    private void storeDataSegmentAsyncCommon(CASIDDataDescriptor segmentDescriptor, long dataID, FSCASSerialStorageFuture future) throws IOException
    {
    	ByteBuffer writeBuffer = ByteBuffer.allocate((int)segmentDescriptor.getLength());
    	future.setWriteBuffer(writeBuffer);
    	segmentDescriptor.getDataAsync(writeBuffer, 0, writeBuffer.capacity(), true, null, future);
    	CASIdentifier checkIdentifier = new CASIdentifier(writeBuffer.array());
    	if (!checkIdentifier.equals(segmentDescriptor.getCASIdentifier()))
    		throw new IOException("checkIdentifier does not match CASIDDataDescriptor cas identifier");
    }
    
    /**
     * Writes a dataBuffer to the specified segment.  The data buffer should contain the entire segment and it should
     * be positioned at 0 and the amount to be written will be the capacity of the buffer
     * @param dataBuffer
     * @param casID
     * @param dataID
     * @throws IOException
     */
    protected synchronized void storeDataSegment(ByteBuffer dataBuffer, CASIdentifier casID, long dataID, boolean replace) throws IOException
    {
    	if (dataBuffer.position() != 0)
    		throw new IllegalArgumentException("dataBuffer must be positioned at 0");

        boolean stored = false;
        FSCASSerialStorageDirectory curStoreNode = null;
        while (!stored)
        {
            if (curStoreNode == null || dataID - lastDataID > 1)
            {
                curStoreNode = findOrCreateStoreNodeForID(dataID);
            }
            if (curStoreNode != null)
            {
                File storageDirFile = curStoreNode.getDirectory();
                if (storageDirFile.exists() && !storageDirFile.isDirectory())
                {
                    throw new InternalError(storageDirFile.getAbsolutePath()+" is not a directory");
                }
                String segFileName = getSegFileName(dataID);
                File casSegmentFile = new File(storageDirFile, segFileName);
                if (!replace && casSegmentFile.exists())
                	throw new IOException("Trying to store segment "+casID+" as dataID = "+dataID+" path = "+casSegmentFile.getAbsolutePath());
                MBPerSecondLog4jStopWatch writeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".storeDataSegment.write");
                FileOutputStream outputStream = new FileOutputStream(casSegmentFile);
                FileChannel writeChannel = outputStream.getChannel();
                if (writeChannel.write(dataBuffer) != dataBuffer.capacity())
                {
                	outputStream.close();
                	throw new IOException("Did not get complete buffer");
                }
                outputStream.close();
                writeWatch.bytesProcessed(dataBuffer.capacity());
                writeWatch.stop();
                curStoreNode.addFile(casSegmentFile);
                stored = true;
                lastDataID = dataID;
            }
        }
    }

	private FSCASSerialStorageDirectory findOrCreateStoreNodeForID(long dataID)
			throws InternalError
	{
		FSCASSerialStorageDirectory curStoreNode;
		ArrayList<String> storeNodePath = null;
		while(storeNodePath == null)
		{
		    storeNodePath = getStorePathForDataID(dataID);
		    if (storeNodePath == null)  // Need to split the root.  Oh frabjous day!
		    {
		        String firstNodeName = "";
		        for (int digitNum = 0; digitNum < kDigitsPerDirLevel; digitNum++)
		            firstNodeName += "0";
		        String casStoragePath = casStorageRoot.getAbsolutePath();
		        File tempFirstNode = new File(new File(casStorageRoot.getParent()), firstNodeName);
		        if (!casStorageRoot.renameTo(tempFirstNode)) // Move out of the way
		        	throw new InternalError("Oh noes - fell down trying to move root "+casStorageRoot+" to temporary location "+tempFirstNode);
		        File casStorageRoot = new File(casStoragePath);
		        if (!casStorageRoot.mkdir())
		            throw new InternalError("Oh noes - fell down trying to create new casStorageRoot "+casStoragePath);
		        File firstNode = new File(casStorageRoot, firstNodeName);
		        tempFirstNode.renameTo(firstNode);
		        rootNode = new FSCASSerialStorageDirectory(casStorageRoot, 0);  // Reload everyone
		        curStoreNode = null;
		    }
		}
		curStoreNode = getStoreNodeForPath(storeNodePath);
		if (curStoreNode == null)
		{
		    ArrayList<String>parentPath = new ArrayList<String>();
		    FSCASSerialStorageDirectory curAncestor = rootNode;
		    for (int nodeDepth = 0; nodeDepth < storeNodePath.size(); nodeDepth++)
		    {
		        parentPath.add(storeNodePath.get(nodeDepth));
		        FSCASSerialStorageDirectory nextAncestor = getStoreNodeForPath(parentPath);
		        if (nextAncestor == null)
		        {
		            String nextDirName = parentPath.get(parentPath.size() - 1);
		            File nextDir = new File(curAncestor.getDirectory(), nextDirName);
		            nextDir.mkdir();
		            nextAncestor = curAncestor.addChildDir(nextDirName);
		        }
		        curAncestor = nextAncestor;
		    }
		}
		return curStoreNode;
	}

    private String getSegFileName(long dataID)
    {
        String dataIDStr = Long.toString(dataID);
        String segFileName = dataIDStr+".casseg";
        return segFileName;
    }
    
    public synchronized FSCASSerialStorageDirectory getStoreNodeForPath(ArrayList<String>storePathForDataID)
    {
        FSCASSerialStorageDirectory curCheckNode = rootNode;
        for (String curDirName:storePathForDataID)
        {
            curCheckNode = curCheckNode.getChildDir(curDirName);
            if (curCheckNode == null)
                break;
        }
        return curCheckNode;
    }
    
    public ArrayList<String>getStorePathForDataID(long dataID)
    {
        ArrayList<String>storePathForSerial = new ArrayList<String>();
        FSCASSerialStorageDirectory curCheckNode = rootNode;
        int curHeight = rootNode.height();
        int serialLenWithPadding = (curHeight + 1)* kDigitsPerDirLevel;
        StringBuffer serialStrBuf = new StringBuffer(serialLenWithPadding);
        String serialOnly = Long.toString(dataID);
        if (serialOnly.length() > serialLenWithPadding)
        {
            if (serialOnly.length() > serialLenWithPadding + kDigitsPerDirLevel)
                throw new InternalError("Skipping too far ahead with serial numbers - looking for path for "+dataID);
            return null;
        }
        for (int curCharNum = 0; curCharNum < serialLenWithPadding - serialOnly.length(); curCharNum++)
            serialStrBuf.append('0');
        serialStrBuf.append(serialOnly);
        int curDepth = 0;
        while(curDepth < curHeight && curCheckNode != null)
        {
            String curDirName = serialStrBuf.substring(curDepth * kDigitsPerDirLevel, (curDepth + 1) * kDigitsPerDirLevel);
            storePathForSerial.add(curDirName);
            curDepth++;
        }
        return storePathForSerial;
    }
    
    public synchronized CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID, long dataID)
    throws IOException
    {
    	MBPerSecondLog4jStopWatch wholeWatch = new MBPerSecondLog4jStopWatch(kStopWatchName+".retrieveSegment");
        File casFile = getSegmentFileForDataID(dataID);
        if (casFile == null)
        	return null;
        CASIDDataDescriptor returnDescriptor = null;

        if (casFile.exists() && casFile.isFile())
        {
            FileInputStream dataStream = new FileInputStream(casFile);
            returnDescriptor = createBDD(dataStream, segmentID);
            dataStream.close();
        }
        wholeWatch.bytesProcessed(returnDescriptor.getLength());
        wholeWatch.stop();
        return returnDescriptor;
    }

	public File getSegmentFileForDataID(long dataID)
	{
		Log4JStopWatch wholeWatch = new Log4JStopWatch(kStopWatchName+".getSegmentFileForDataID");
		try
		{
			ArrayList<String> storeNodePath = getStorePathForDataID(dataID);
			if (storeNodePath == null)
			{
				return null;
			}
			FSCASSerialStorageDirectory retrieveNode = getStoreNodeForPath(storeNodePath);
			if (retrieveNode == null)
				return null;
			String segFileName = getSegFileName(dataID);

			File casFile = new File(retrieveNode.getDirectory(), segFileName);
			if (casFile.exists())
				return casFile;
			else
				return null;	// Doesn't really exist
		}
		finally
		{
			wholeWatch.stop();
		}
	}

	public boolean removeSegment(long dataID) throws IOException
	{
		ArrayList<String> storeNodePath = getStorePathForDataID(dataID);
		if (storeNodePath != null)
		{
			FSCASSerialStorageDirectory retrieveNode = getStoreNodeForPath(storeNodePath);
			String segFileName = getSegFileName(dataID);

			File casFile = new File(retrieveNode.getDirectory(), segFileName);

			if (casFile.exists() && casFile.isFile())
			{
				return casFile.delete();
			}
		}
		return false;
	}
	
	public long getFirstDataID() throws IOException
	{
		FSCASSerialStorageDirectory curNode = rootNode;
		while (curNode.height() > 0)
		{
			curNode = curNode.firstDir();
			if (curNode == null)
				throw new IOException("Got to bottom of tree too soon");
		}
		File firstFile = curNode.firstFile();
		long returnID;
		if (firstFile != null)
		{
			String firstNumStr = firstFile.getName().substring(0, firstFile.getName().length() - 7);
			returnID = Long.parseLong(firstNumStr);
		}
		else
		{
			returnID = -1;
		}
		return returnID;
	}
	
	public long getLastDataID() throws IOException
	{
		FSCASSerialStorageDirectory curNode = rootNode;
		while (curNode.height() > 0)
		{
			curNode = curNode.lastDir();
			if (curNode == null)
				throw new IOException("Got to bottom of tree too soon");
		}
		File lastFile = curNode.lastFile();
		long returnID;
		if (lastFile != null)
		{
			String lastNumStr = lastFile.getName().substring(0, lastFile.getName().length() - 7);
			returnID = Long.parseLong(lastNumStr);
		}
		else
		{
			returnID = -1;
		}
		return returnID;
	}
}
