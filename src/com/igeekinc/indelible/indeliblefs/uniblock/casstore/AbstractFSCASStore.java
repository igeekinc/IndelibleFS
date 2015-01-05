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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.FSCASSerialStorage;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.util.CheckCorrectDispatchThread;
import com.igeekinc.util.DirectMemoryUtils;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.logging.ErrorLogMessage;

class RebuildInfo
{
	private long dataID;
	private ByteBuffer buffer;
	private FileChannel channel;

	public RebuildInfo(long dataID, ByteBuffer buffer, FileChannel channel)
	{
		this.dataID = dataID;
		this.buffer = buffer;
		this.channel = channel;
		
	}
	public long getDataID()
	{
		return dataID;
	}

	public ByteBuffer getData()
	{
		return buffer;
	}
	
	public void close() throws IOException
	{
		if (buffer != null)
		{
			if (buffer.isDirect())
			{
				DirectMemoryUtils.clean(buffer);
			}
			channel.close();
			buffer = null;
			channel = null;
		}
	}
}

class InsertInfo
{
	private long dataID;
	private CASIdentifier casID;
	
	public InsertInfo(long dataID, CASIdentifier casID)
	{
		this.dataID = dataID;
		this.casID = casID;
	}

	public long getDataID()
	{
		return dataID;
	}

	public CASIdentifier getCasID()
	{
		return casID;
	}
}

class RebuildCalculatorRunnable implements Runnable
{
	LinkedBlockingQueue<RebuildInfo> rebuildQueue;
	LinkedBlockingQueue<InsertInfo> insertQueue;
	AbstractFSCASStore casStore;
	boolean keepRunning = true;
	Logger logger = Logger.getLogger(getClass());
	public RebuildCalculatorRunnable(LinkedBlockingQueue<RebuildInfo> rebuildQueue, LinkedBlockingQueue<InsertInfo>insertQueue, AbstractFSCASStore casStore)
	{
		this.rebuildQueue = rebuildQueue;
		this.insertQueue = insertQueue;
		this.casStore = casStore;
	}
	
	public void finished()
	{
		keepRunning = false;
	}
	
	@Override
	public void run()
	{
		long startTime = System.currentTimeMillis();
		long bytesCalculated = 0;
		long getDataTime = 0;
		long calculateTime = 0;
		long pollTime = 0;
		long queueTime = 0;
		while(keepRunning)
		{
			RebuildInfo curInfo = null;
			try
			{
				long startPollTime = System.currentTimeMillis();
				curInfo = rebuildQueue.poll(10, TimeUnit.MILLISECONDS);
				pollTime += (System.currentTimeMillis() - startPollTime);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
			if (curInfo != null)
			{
				long startGetDataTime = System.currentTimeMillis();
				getDataTime += (System.currentTimeMillis() - startGetDataTime);
				long calculateStartTime = System.currentTimeMillis();
				CASIdentifier casID = casStore.calculateHash(curInfo.getData());
				long calculeElapsedTime = System.currentTimeMillis() - calculateStartTime;
				calculateTime += calculeElapsedTime;
				bytesCalculated += casID.getSegmentLength();
				
				long insertStartTime = System.currentTimeMillis();
				InsertInfo insertInfo = new InsertInfo(curInfo.getDataID(), casID);
				try
				{
					insertQueue.put(insertInfo);
				} catch (InterruptedException e1)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
				}
				queueTime += (System.currentTimeMillis() - insertStartTime);
				if (insertQueue.size() > 1 && logger.isDebugEnabled())
					logger.debug("insertQueue.size() = "+insertQueue.size());
				try
				{
					curInfo.close();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
		}
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Processed "+bytesCalculated+" totalTime = "+totalTime+" getDataTime = "+getDataTime+" calculateTime = "+calculateTime+" pollTime = "+pollTime+" queue time = "+queueTime);
	}
}


class InsertRunnable implements Runnable
{
	public static final int kCommentInterval = 1000;
	LinkedBlockingQueue<InsertInfo> insertQueue;
	CommitRunnable commitRunnable;
	AbstractFSCASStore casStore;
	boolean keepRunning = true;
	Logger logger = Logger.getLogger(getClass());
	public InsertRunnable(LinkedBlockingQueue<InsertInfo>insertQueue, AbstractFSCASStore casStore, CommitRunnable commitRunnable)
	{
		this.insertQueue = insertQueue;
		this.casStore = casStore;
		this.commitRunnable = commitRunnable;
	}
	
	public void finished()
	{
		keepRunning = false;
	}
	
	@Override
	public void run()
	{
		long startTime = System.currentTimeMillis();
		long pollTime = 0;
		long insertTime = 0;
		long numInserted = 0;
		long commitTime = 0;
		while(keepRunning || insertQueue.size() > 0)
		{
			InsertInfo curInsertInfo = null;
			try
			{
				long pollStartTime = System.currentTimeMillis();
				curInsertInfo = insertQueue.poll(10, TimeUnit.MILLISECONDS);
				pollTime += (System.currentTimeMillis() - pollStartTime);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
			if (curInsertInfo != null)
			{
				long insertStartTime = System.currentTimeMillis();
				casStore.addToIndex(curInsertInfo.getCasID(), curInsertInfo.getDataID());
				insertTime += (System.currentTimeMillis() - insertStartTime);
				numInserted++;
				if (numInserted % kCommentInterval == 0)
				{
					logger.error(new ErrorLogMessage("Rebuilding ID = {0}, endingID = {1}", new Serializable[]{numInserted, curInsertInfo.getDataID()}));
				}
				if (numInserted % AbstractFSCASStore.kCommitInterval == 0)
				{
					commitRunnable.addCommit();
					// Don't let the transaction log get too big
				}
			}
		}
		System.out.println("Insert thread total time = "+(System.currentTimeMillis() - startTime) + " poll time ="+pollTime+" insert time = "+insertTime+" commitTime = "+commitTime);
	}
	
}

class CommitRunnable implements Runnable
{
	AbstractFSCASStore casStore;
	boolean keepRunning = true;
	Logger logger = Logger.getLogger(getClass());
	private int commitsToDo;
	
	public CommitRunnable(AbstractFSCASStore casStore)
	{
		this.casStore = casStore;
	}
	
	public synchronized void addCommit()
	{
		commitsToDo++;
		this.notifyAll();
	}
	
	public synchronized void finished()
	{
		keepRunning = false;
		this.notifyAll();
	}
	
	@Override
	public void run()
	{
		long startTime = System.currentTimeMillis();
		long pollTime = 0;
		long commitTime = 0;
		while(keepRunning || commitsToDo > 0)
		{
			if (commitsToDo > 0)
			{
				try
				{
					long commitStartTime = System.currentTimeMillis();
					casStore.rebuildCommit();
					long elapsedCommitTime = System.currentTimeMillis() - commitStartTime;
					commitTime += elapsedCommitTime;
					logger.error(new ErrorLogMessage("Committed {0} records in {1} ms", new Serializable[]{AbstractFSCASStore.kCommitInterval, elapsedCommitTime}));
					commitsToDo--;
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
				if (commitsToDo > 0)
					logger.error("Falling behind on commits - completed with "+commitsToDo+" commits pending");
			}
			else
			{
				synchronized(this)
				{
					long startPollTime = System.currentTimeMillis();
					try
					{
						wait(10000L);
					} catch (InterruptedException e)
					{
						// TODO Auto-generated catch block
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}
					long elapsedPollTime = System.currentTimeMillis() - startPollTime;
					pollTime += elapsedPollTime;
				}
			}
		}
		System.out.println("Commit thread total time = "+(System.currentTimeMillis() - startTime) + " poll time ="+pollTime+" commitTime = "+commitTime);

	}
}
public abstract class AbstractFSCASStore extends AbstractCASStore
{


	protected CASStoreID	storeID;
	protected File	storeRoot, dbDir;
	protected File	propertiesFile;
	protected File	casStorageRoot;
	protected long	nextDataID;
	protected FSCASSerialStorage	casStorage;
	protected Properties	storeProperties;
	public static final int kCommitInterval = 1000000;
	private static final int kRebuildQueueSize = 1024;
	private static final int kInsertQueueSize = 1024;	// The insert thread should be running ahead of the rebuild threads.  When it starts to lag, it is probably
														// I/O bound, so better to stall the rebuild and read queue and free up I/O for the insert thread
	
	public AbstractFSCASStore(CheckCorrectDispatchThread checker)
	{
		super(checker);
		// TODO Auto-generated constructor stub
	}
	
	public void rebuild() throws IOException
	{
		setStatus(CASStoreStatus.kRebuilding);
		int maxThreads = Runtime.getRuntime().availableProcessors()/2;	// Hyperthreading is lying to us!
		System.out.println("maxThreads = "+maxThreads);
		boolean upgraded = upgradeIfNecessary();
		if (upgraded)
			return;
		
		Object rebuildInfo = prepareForRebuild();
		long startingID = getStartingID();
		long endingID = getLastID();
		
		logger.error(new ErrorLogMessage("First found id = {0}, last found ID = {1}, beginning processing...", startingID, endingID));
		
		LinkedBlockingQueue<RebuildInfo>rebuildQueue = new LinkedBlockingQueue<RebuildInfo>(kRebuildQueueSize);
		LinkedBlockingQueue<InsertInfo>insertQueue = new LinkedBlockingQueue<InsertInfo>(kInsertQueueSize);
		ArrayList<RebuildCalculatorRunnable> calculators = new ArrayList<RebuildCalculatorRunnable>();
		ArrayList<Thread>calculatorThreads = new ArrayList<Thread>();
		RebuildCalculatorRunnable calculator = new RebuildCalculatorRunnable(rebuildQueue, insertQueue, this);
		Thread calculatorThread = new Thread(calculator, "Rebuild calculator "+calculators.size());
		calculatorThread.start();
		calculators.add(calculator);
		calculatorThreads.add(calculatorThread);
		CommitRunnable committer = new CommitRunnable(this);
		Thread commitThread = new Thread(committer, "CAS Commit");
		commitThread.start();
		InsertRunnable inserter = new InsertRunnable(insertQueue, this, committer);
		Thread insertThread = new Thread(inserter, "Rebuild insert thread");
		insertThread.start();
		long readStartTime = System.currentTimeMillis();
		long readTime = 0, queueTime = 0, drainTime = 0;
		for (long checkID = startingID; checkID < endingID + 1; checkID++)
		{
			File checkFile;
			synchronized(this)
			{
				checkFile = casStorage.getSegmentFileForDataID(checkID);
			}
			if (checkFile != null && checkFile.exists())
			{
				long curReadStartTime = System.currentTimeMillis();
				long segmentLength = checkFile.length();
				FileInputStream checkStream = new FileInputStream(checkFile);
				RebuildInfo curInfo;
				FileChannel channel = checkStream.getChannel();
				if (segmentLength < 64*1024)
				{
					ByteBuffer checkDataBuffer = DirectMemoryUtils.allocateDirect((int)checkFile.length());
					ByteBuffer.allocateDirect((int)checkFile.length());
					channel.read(checkDataBuffer);
					checkDataBuffer.position(0);
					curInfo = new RebuildInfo(checkID, checkDataBuffer, channel);
				}
				else
				{

					MappedByteBuffer checkDataBuffer = DirectMemoryUtils.mapReadOnlyBuffer(channel, 0, checkFile.length());
							//channel.map(FileChannel.MapMode.READ_ONLY, 0, checkFile.length());
					checkDataBuffer.load();
					curInfo = new RebuildInfo(checkID, checkDataBuffer, channel);
				}
				readTime += (System.currentTimeMillis() - curReadStartTime);
				long startQueueTime = System.currentTimeMillis();
				try
				{
					rebuildQueue.put(curInfo);
				} catch (InterruptedException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
				queueTime += (System.currentTimeMillis() - startQueueTime);
				int rebuildQueueSize = rebuildQueue.size();
				if (rebuildQueueSize > kRebuildQueueSize/4)
				{
					if (calculators.size() < maxThreads)

					{
						RebuildCalculatorRunnable addCalculator = new RebuildCalculatorRunnable(rebuildQueue, insertQueue, this);
						Thread addCalculatorThread = new Thread(calculator, "Rebuild calculator "+calculators.size());
						addCalculatorThread.start();
						calculators.add(addCalculator);
						calculatorThreads.add(addCalculatorThread);
						if (logger.isDebugEnabled())
							logger.debug("Rebuild queue size = "+rebuildQueueSize+" adding thread "+calculators.size());
					}
					else
					{
						if (logger.isDebugEnabled())
							logger.debug("Rebuild queue size = "+rebuildQueueSize+" but already hit "+maxThreads+" threads");
					}
				}
				// Let the queue drain
				long drainStartTime = System.currentTimeMillis();
				while (rebuildQueue.remainingCapacity() < kRebuildQueueSize/4)
				{
					try
					{
						Thread.sleep(1000);	//	Let go of our locks
					} catch (Exception e)
					{
						// TODO Auto-generated catch block
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}
					if (logger.isDebugEnabled())
						logger.debug("After waking from sleep, queue size = "+rebuildQueue.size());
				}
				drainTime += (System.currentTimeMillis() - drainStartTime);
			}
		}
		long now = System.currentTimeMillis();
		System.out.println("Finished reading at "+now +" elapsed = "+(now-readStartTime) + " read time = "+readTime+" queue time = "+queueTime+" drainTime = "+drainTime);
		while(rebuildQueue.size() > 0)
		{
			try
			{
				Thread.sleep(10L);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		
		// Rebuild threads are done, tell them to go away
		for (RebuildCalculatorRunnable curCalculator:calculators)
		{
			curCalculator.finished();
		}
		// Now wait for them to all finish processing
		for (Thread waitCalculatorThread:calculatorThreads)
		{
			try
			{
				waitCalculatorThread.join();
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		long insertStartTime = System.currentTimeMillis();
		// Now wait for the insert queue to finish
		while(insertQueue.size() > 0)
		{
			try
			{
				Thread.sleep(10L);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		// And now the inserter can exit
		inserter.finished();
		try
		{
			insertThread.join();
		} catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		committer.addCommit();	// Make sure we commit at least once!
		committer.finished();
		try
		{
			commitThread.join();
		} catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		long insertFinishTime = System.currentTimeMillis() - insertStartTime;
		System.out.println("Insert finish time = "+insertFinishTime);
		rebuildPostProcess(rebuildInfo);
		setStatus(CASStoreStatus.kReady);
	}

	protected CASIdentifier calculateHash(ByteBuffer checkData)
	{
		checkData.position(0);
		SHA1HashID checkHash = new SHA1HashID(checkData);
		CASIdentifier checkSegmentID = new CASIdentifier(checkHash, checkData.limit());
		return checkSegmentID;
	}

	protected abstract void addToIndex(CASIdentifier checkSegmentID, long checkID);

	protected abstract long getLastID() throws IOException;

	protected abstract long getStartingID() throws IOException;

	protected abstract Object prepareForRebuild() throws IOException;

	protected abstract void rebuildPostProcess(Object rebuildInfo) throws FileNotFoundException, IOException;
	
	protected abstract boolean upgradeIfNecessary() throws FileNotFoundException, IOException;
	
	protected abstract void rebuildCommit() throws IOException;

}
