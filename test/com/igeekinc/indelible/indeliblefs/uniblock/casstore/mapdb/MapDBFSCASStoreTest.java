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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.mapdb;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.bouncycastle.util.Arrays;

import com.igeekinc.indelible.indeliblefs.server.IndelibleFSServerOIDs;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreStatus;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.logging.ErrorLogMessage;

class BlockInfo
{
	long blockNum;
	int blockSize;
	int passNum;
	long offset;
}

class GenerateDataRunnable implements Runnable
{
	LinkedBlockingQueue<CASIDDataDescriptor> writeQueue;
	HashMap<CASIdentifier, BlockInfo> checkInfo;
	int startBlockNum, numBlocks, pass, blockSize;
	MapDBFSCASStoreTest test;
	public GenerateDataRunnable(LinkedBlockingQueue<CASIDDataDescriptor> writeQueue, HashMap<CASIdentifier, 
			BlockInfo> checkInfo, int startBlockNum, int numBlocks, int pass, int blockSize, MapDBFSCASStoreTest runningThreads)
	{
		this.writeQueue = writeQueue;
		this.checkInfo = checkInfo;
		this.startBlockNum = startBlockNum;
		this.numBlocks = numBlocks;
		this.pass = pass;
		this.blockSize = blockSize;
		this.test = runningThreads;
	}
	@Override
	public void run() 
	{
		try
		{
			long writeStartTime = System.currentTimeMillis();
			long generateTime = 0, casIDTime = 0, writeTime = 0;
			for (int blockNum = startBlockNum; blockNum < startBlockNum + numBlocks; blockNum++)
			{
				long offset = (long)blockNum * blockSize;
				long generateStartTime = System.currentTimeMillis();
				ByteBuffer testBytes = TestFilesTool.generateTestPatternByteBuffer(blockNum, blockSize, pass, offset);
				generateTime += (System.currentTimeMillis() - generateStartTime);
				long casIDStartTime = System.currentTimeMillis();			
				testBytes.position(0);
				CASIDDataDescriptor addBlock = new CASIDMemoryDataDescriptor(testBytes);
				casIDTime += (System.currentTimeMillis() - casIDStartTime);
				BlockInfo info = new BlockInfo();
				info.blockNum = blockNum;
				info.blockSize = (int)addBlock.getLength();
				info.offset = offset;
				info.passNum = 0;
				synchronized(checkInfo)
				{
					checkInfo.put(addBlock.getCASIdentifier(), info);
				}
				long writeBeginTime = System.currentTimeMillis();
				try {
					writeQueue.put(addBlock);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				writeTime += (System.currentTimeMillis() - writeBeginTime);
			}
		}
		finally
		{
			test.decrementRunningThreads();
		}
	}
	
}

class test1000Inserter implements Runnable
{
	boolean finished = false;
	LinkedBlockingQueue<CASIDDataDescriptor>insertQueue;
	MapDBFSCASStore insertStore;
	
	public test1000Inserter(LinkedBlockingQueue<CASIDDataDescriptor>insertQueue, MapDBFSCASStore insertStore)
	{
		this.insertQueue = insertQueue;
		this.insertStore = insertStore;
	}
	
	synchronized void setFinished()
	{
		finished = true;
	}
	@Override
	public void run()
	{
		while (!finished || insertQueue.size() > 0)
		{
			try
			{
				CASIDDataDescriptor insertDescriptor = insertQueue.poll(10, TimeUnit.MILLISECONDS);
				if (insertDescriptor != null)
				{
					try
					{
						insertStore.storeSegment(insertDescriptor);
					} catch (IOException e)
					{
						// TODO Auto-generated catch block
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}
				}
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
	}
	
}
public class MapDBFSCASStoreTest extends TestCase
{
	private static final int	kSmallBlockSize	= 1024;
	int runningThreads;
	static
	{
		IndelibleFSServerOIDs.initMapping();
	}
	ObjectIDFactory oidFactory;
	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
		oidFactory = new ObjectIDFactory(genIDFactory.createGeneratorID());
	}

	public void testBasic() throws Exception
	{
		CASIDMemoryDataDescriptor.setMaxGeneratorThreads(8);
		MapDBFSCASStore testStore = createTempCASStore();
		byte [] dataA = new byte[1024];
		for (int index = 0; index < dataA.length; index++)
			dataA[index] = 'A';
		CASIDMemoryDataDescriptor storeDescriptor = new CASIDMemoryDataDescriptor(dataA);
		testStore.storeSegment(storeDescriptor);
		CASIDDataDescriptor restoreDescriptor = testStore.retrieveSegment(storeDescriptor.getCASIdentifier());
		byte [] retrievedData = restoreDescriptor.getData();
		assertTrue(Arrays.areEqual(dataA, retrievedData));
		testStore.shutdown();
		TestFilesTool.deleteTree(testStore.getStoreRoot());
	}

	public void test1000() throws Exception
	{
		CASIDMemoryDataDescriptor.setMaxGeneratorThreads(0);
		HashMap<CASIdentifier, BlockInfo> checkInfo = new HashMap<CASIdentifier, BlockInfo>();
		MapDBFSCASStore testStore = createTempCASStore();
		long insertStartTime = System.currentTimeMillis();
		for (int blockNum = 0; blockNum < 1000; blockNum++)
		{
			int offset = blockNum * kSmallBlockSize;
			CASIDDataDescriptor addBlock = generateTestPatternDataDescriptor(blockNum, kSmallBlockSize, 0, offset);
			BlockInfo info = new BlockInfo();
			info.blockNum = blockNum;
			info.blockSize = kSmallBlockSize;
			info.offset = blockNum * kSmallBlockSize;
			info.passNum = 0;
			checkInfo.put(addBlock.getCASIdentifier(), info);
			testStore.storeSegment(addBlock);
		}
		testStore.shutdown();
		System.out.println("test1000 - insert took "+(System.currentTimeMillis() - insertStartTime));
		File storeRoot = testStore.getStoreRoot();
		MapDBFSCASStore checkStore = new MapDBFSCASStore(storeRoot);
		for (CASIdentifier checkID:checkInfo.keySet())
		{
			BlockInfo verifyInfo = checkInfo.get(checkID);
			DataDescriptor verifyDescriptor = checkStore.retrieveSegment(checkID);
			assertTrue(verifyTestPatternDataDescriptor(verifyDescriptor, verifyInfo.blockNum, verifyInfo.passNum, verifyInfo.offset));
		}
		checkStore.shutdown();
		TestFilesTool.deleteTree(storeRoot);
	}
	
	public void test1000Threaded() throws Exception
	{
		CASIDMemoryDataDescriptor.setMaxGeneratorThreads(Runtime.getRuntime().availableProcessors());
		HashMap<CASIdentifier, BlockInfo> checkInfo = new HashMap<CASIdentifier, BlockInfo>();
		MapDBFSCASStore testStore = createTempCASStore();
		long insertStartTime = System.currentTimeMillis();
		boolean finished = false;
		LinkedBlockingQueue<CASIDDataDescriptor>insertQueue = new LinkedBlockingQueue<CASIDDataDescriptor>(128);
		test1000Inserter inserter = new test1000Inserter(insertQueue, testStore);
		Thread insertThread = new Thread(inserter);
		insertThread.start();

		for (int blockNum = 0; blockNum < 1000; blockNum++)
		{
			int offset = blockNum * kSmallBlockSize;
			CASIDDataDescriptor addBlock = generateTestPatternDataDescriptor(blockNum, kSmallBlockSize, 0, offset);
			BlockInfo info = new BlockInfo();
			info.blockNum = blockNum;
			info.blockSize = kSmallBlockSize;
			info.offset = blockNum * kSmallBlockSize;
			info.passNum = 0;
			checkInfo.put(addBlock.getCASIdentifier(), info);
			insertQueue.put(addBlock);
		}
		inserter.setFinished();
		insertThread.join();
		
		testStore.shutdown();
		System.out.println("test1000 - insert took "+(System.currentTimeMillis() - insertStartTime));
		File storeRoot = testStore.getStoreRoot();
		MapDBFSCASStore checkStore = new MapDBFSCASStore(storeRoot);
		for (CASIdentifier checkID:checkInfo.keySet())
		{
			BlockInfo verifyInfo = checkInfo.get(checkID);
			DataDescriptor verifyDescriptor = checkStore.retrieveSegment(checkID);
			assertTrue(verifyTestPatternDataDescriptor(verifyDescriptor, verifyInfo.blockNum, verifyInfo.passNum, verifyInfo.offset));
		}
		checkStore.shutdown();
		TestFilesTool.deleteTree(storeRoot);
	}
	public synchronized void decrementRunningThreads()
	{
		runningThreads--;
	}
	
	public static final int kLargeBlockSize = 1024*1024;
	public static final int kReloadNumberOfSegments = 10000;
	public void testRebuild() throws Exception
	{
		CASIDMemoryDataDescriptor.setMaxGeneratorThreads(0);	// We'll do our own threading
		HashMap<CASIdentifier, BlockInfo> checkInfo = new HashMap<CASIdentifier, BlockInfo>();
		MapDBFSCASStore testStore = createTempCASStore();
		long writeStartTime = System.currentTimeMillis();
		long generateTime = 0, casIDTime = 0, writeTime = 0;
		int numGenerateThreads = Runtime.getRuntime().availableProcessors();
		Thread [] generateThreads = new Thread[numGenerateThreads];
		int blocksPerThread = kReloadNumberOfSegments / numGenerateThreads;
		LinkedBlockingQueue<CASIDDataDescriptor>writeQueue = new LinkedBlockingQueue<CASIDDataDescriptor>(64);
		runningThreads = numGenerateThreads;
		for (int curThreadNum = 0; curThreadNum < numGenerateThreads; curThreadNum++)
		{
			int blocksThisThread = blocksPerThread;
			if (curThreadNum == numGenerateThreads - 1)
			{
				// We're the last thread, make up any breakage
				int threadTotal = blocksPerThread * numGenerateThreads;
				int extras = kReloadNumberOfSegments - threadTotal;
				blocksThisThread += extras;
			}
			GenerateDataRunnable curRunnable = new GenerateDataRunnable(writeQueue, checkInfo, 
					curThreadNum * blocksPerThread, blocksThisThread, 0, kLargeBlockSize, this);
			generateThreads[curThreadNum] = new Thread(curRunnable, "Generate thread "+curThreadNum);
			generateThreads[curThreadNum].start();
		}
		int blocksStored = 0;
		while (runningThreads > 0 || writeQueue.size() > 0)
		{
			CASIDDataDescriptor addBlock = writeQueue.poll(10, TimeUnit.MILLISECONDS);
			if (addBlock != null)
			{
				long writeBeginTime = System.currentTimeMillis();
				testStore.storeSegment(addBlock);
				writeTime += (System.currentTimeMillis() - writeBeginTime);
				blocksStored ++;
			}
		}
		assertEquals(kReloadNumberOfSegments, blocksStored);	// Make sure that we wrote what we were supposed to write
		testStore.shutdown();
		long writeEndTime = System.currentTimeMillis();
		long writeElapsedTime = writeEndTime - writeStartTime;
		System.out.println("Wrote " + kReloadNumberOfSegments+ " "+kLargeBlockSize+" segments in "+writeElapsedTime+" generateTime = "+generateTime+" cas ID time = "+casIDTime+" writeTime = "+writeTime);
		File storeRoot = testStore.getStoreRoot();
		for (File deleteFile:storeRoot.listFiles(new FileFilter()
						{
							
							@Override
							public boolean accept(File paramFile)
							{
								if (paramFile.getName().startsWith("Index"))
									return true;
								return false;
							}
						}))
		{
			assertTrue(deleteFile.delete());
		}
		MapDBFSCASStore checkStore = new MapDBFSCASStore(storeRoot);
		assertTrue(checkStore.getStatus() == CASStoreStatus.kNeedsRebuild);
		long startTime = System.currentTimeMillis();
		checkStore.rebuild();
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		System.out.println("Reload of "+kReloadNumberOfSegments+" "+kLargeBlockSize+" objects took "+elapsedTime+" ms");
		long verifyStartTime = System.currentTimeMillis();
		for (CASIdentifier checkID:checkInfo.keySet())
		{
			BlockInfo verifyInfo = checkInfo.get(checkID);
			DataDescriptor verifyDescriptor = checkStore.retrieveSegment(checkID);
			assertTrue(verifyTestPatternDataDescriptor(verifyDescriptor, verifyInfo.blockNum, verifyInfo.passNum, verifyInfo.offset));
		}
		long verifyEndTime = System.currentTimeMillis();
		long verifyElapsedTime = verifyEndTime - verifyStartTime;
		System.out.println("Verify of "+kReloadNumberOfSegments+" "+kLargeBlockSize+" objects took "+verifyElapsedTime+" ms");
		checkStore.shutdown();
		File casFilesDir = new File(storeRoot, "CASFiles");
		File [] casDirs;
		if (kReloadNumberOfSegments > 1000)
		{
			casDirs = casFilesDir.listFiles();
		}
		else
		{
			casDirs = new File[1];
			casDirs[0] = casFilesDir;	// Less than 1000, there are no subdirs
		}
		// This assumes we haven't gotten to a second level hierarchy.  if you put more than 1,000,000 segments in you will have problems here
		long justReadStartTime = System.currentTimeMillis();
		long bytesRead = 0;
		int numFilesRead = 0;
		for (File curCASDir:casDirs)
		{
			for (File curReadFile:curCASDir.listFiles())
			{
				if (curReadFile.getName().endsWith(".casseg"))
				{
					FileInputStream readStream = new FileInputStream(curReadFile);
					FileChannel readChannel = readStream.getChannel();

					ByteBuffer readBuffer = ByteBuffer.allocateDirect((int)curReadFile.length());
					readChannel.read(readBuffer);
					readStream.close();
					bytesRead += readBuffer.limit();
					numFilesRead++;
				}
			}
		}
		long justReadEndTime = System.currentTimeMillis();
		long justReadElapsedTime = justReadEndTime - justReadStartTime;
		System.out.println("Read "+numFilesRead+" total "+bytesRead+" bytes took "+justReadElapsedTime+" ms");
		TestFilesTool.deleteTree(storeRoot);
	}

	public static final int kOneMillionReloadSegments = 1000*1000;

	public void testRebuild1MObjects() throws Exception
	{
		CASIDMemoryDataDescriptor.setMaxGeneratorThreads(0);	// We'll do our own threading
		HashMap<CASIdentifier, BlockInfo> checkInfo = new HashMap<CASIdentifier, BlockInfo>();
		MapDBFSCASStore testStore = createTempCASStore();
		long writeStartTime = System.currentTimeMillis();
		long generateTime = 0, casIDTime = 0, writeTime = 0;
		int numGenerateThreads = Runtime.getRuntime().availableProcessors();
		Thread [] generateThreads = new Thread[numGenerateThreads];
		int blocksPerThread = kOneMillionReloadSegments / numGenerateThreads;
		LinkedBlockingQueue<CASIDDataDescriptor>writeQueue = new LinkedBlockingQueue<CASIDDataDescriptor>(64);
		runningThreads = numGenerateThreads;
		for (int curThreadNum = 0; curThreadNum < numGenerateThreads; curThreadNum++)
		{
			int blocksThisThread = blocksPerThread;
			if (curThreadNum == numGenerateThreads - 1)
			{
				// We're the last thread, make up any breakage
				int threadTotal = blocksPerThread * numGenerateThreads;
				int extras = kOneMillionReloadSegments - threadTotal;
				blocksThisThread += extras;
			}
			GenerateDataRunnable curRunnable = new GenerateDataRunnable(writeQueue, checkInfo, 
					curThreadNum * blocksPerThread, blocksThisThread, 0, kSmallBlockSize, this);
			generateThreads[curThreadNum] = new Thread(curRunnable, "Generate thread "+curThreadNum);
			generateThreads[curThreadNum].start();
		}
		int blocksStored = 0;
		while (runningThreads > 0 || writeQueue.size() > 0)
		{
			CASIDDataDescriptor addBlock = writeQueue.poll(10, TimeUnit.MILLISECONDS);
			if (addBlock != null)
			{
				long writeBeginTime = System.currentTimeMillis();
				testStore.storeSegment(addBlock);
				writeTime += (System.currentTimeMillis() - writeBeginTime);
				blocksStored ++;
			}
		}
		assertEquals(kOneMillionReloadSegments, blocksStored);	// Make sure that we wrote what we were supposed to write
		testStore.shutdown();
		long writeEndTime = System.currentTimeMillis();
		long writeElapsedTime = writeEndTime - writeStartTime;
		System.out.println("Wrote " + kOneMillionReloadSegments+ " "+kSmallBlockSize+" segments in "+writeElapsedTime+" generateTime = "+generateTime+" cas ID time = "+casIDTime+" writeTime = "+writeTime);
		File storeRoot = testStore.getStoreRoot();
		for (File deleteFile:storeRoot.listFiles(new FileFilter()
						{
							
							@Override
							public boolean accept(File paramFile)
							{
								if (paramFile.getName().startsWith("Index"))
									return true;
								return false;
							}
						}))
		{
			assertTrue(deleteFile.delete());
		}
		MapDBFSCASStore checkStore = new MapDBFSCASStore(storeRoot);
		assertTrue(checkStore.getStatus() == CASStoreStatus.kNeedsRebuild);
		long startTime = System.currentTimeMillis();
		checkStore.rebuild();
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		System.out.println("Reload of "+kOneMillionReloadSegments+" "+kSmallBlockSize+" objects took "+elapsedTime+" ms");
		long verifyStartTime = System.currentTimeMillis();
		for (CASIdentifier checkID:checkInfo.keySet())
		{
			BlockInfo verifyInfo = checkInfo.get(checkID);
			DataDescriptor verifyDescriptor = checkStore.retrieveSegment(checkID);
			assertTrue(verifyTestPatternDataDescriptor(verifyDescriptor, verifyInfo.blockNum, verifyInfo.passNum, verifyInfo.offset));
		}
		long verifyEndTime = System.currentTimeMillis();
		long verifyElapsedTime = verifyEndTime - verifyStartTime;
		System.out.println("Verify of "+kOneMillionReloadSegments+" "+kSmallBlockSize+" objects took "+verifyElapsedTime+" ms");
		checkStore.shutdown();
		File casFilesDir = new File(storeRoot, "CASFiles");
		File [] casDirs;
		if (kOneMillionReloadSegments > 1000)
		{
			casDirs = casFilesDir.listFiles();
		}
		else
		{
			casDirs = new File[1];
			casDirs[0] = casFilesDir;	// Less than 1000, there are no subdirs
		}
		// This assumes we haven't gotten to a second level hierarchy.  if you put more than 1,000,000 segments in you will have problems here
		long justReadStartTime = System.currentTimeMillis();
		long bytesRead = 0;
		int numFilesRead = 0;
		for (File curCASDir:casDirs)
		{
			for (File curReadFile:curCASDir.listFiles())
			{
				if (curReadFile.getName().endsWith(".casseg"))
				{
					FileInputStream readStream = new FileInputStream(curReadFile);
					FileChannel readChannel = readStream.getChannel();

					ByteBuffer readBuffer = ByteBuffer.allocateDirect((int)curReadFile.length());
					readChannel.read(readBuffer);
					readStream.close();
					bytesRead += readBuffer.limit();
					numFilesRead++;
				}
			}
		}
		long justReadEndTime = System.currentTimeMillis();
		long justReadElapsedTime = justReadEndTime - justReadStartTime;
		System.out.println("Read "+numFilesRead+" total "+bytesRead+" bytes took "+justReadElapsedTime+" ms");
		TestFilesTool.deleteTree(storeRoot);
	}
	protected CASIDDataDescriptor generateTestPatternDataDescriptor(long blockNum, int blockSize,
			int passNum, long offset)
	{
		ByteBuffer testBytes = TestFilesTool.generateTestPatternByteBuffer(blockNum, blockSize, passNum, offset);
		testBytes.position(0);
		return new CASIDMemoryDataDescriptor(testBytes);
	}

	protected boolean verifyTestPatternDataDescriptor(DataDescriptor verifyDescriptor, long blockNum,
			int passNum, long offset) throws IOException
	{
		return TestFilesTool.verifyTestPatternBlock(blockNum, passNum, offset, verifyDescriptor.getData());
	}
	
	protected MapDBFSCASStore createTempCASStore() throws IOException
	{
		File testStoreDir = File.createTempFile("MapDBFSCASStoreTest", ".casstore");
		//File testStoreDir = new File("/data/dave/teststore.casstore");
		if (testStoreDir.exists())
			assertTrue(testStoreDir.delete());
		assertTrue(testStoreDir.mkdir());
		MapDBFSCASStore testStore = new MapDBFSCASStore((CASStoreID) oidFactory.getNewOID(CASStore.class), testStoreDir);
		return testStore;
	}
}
