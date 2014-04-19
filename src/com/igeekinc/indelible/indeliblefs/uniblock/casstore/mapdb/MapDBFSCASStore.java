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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.AbstractFSCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreStatus;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.FSCASSerialStorage;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.ClientFile;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.async.ComboFutureBase;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.WarnLogMessage;
import com.igeekinc.util.objectcache.LRUQueue;
import com.igeekinc.util.pauseabort.AbortedException;
import com.igeekinc.util.pauseabort.PauseAbort;

class MapDBFSStoreFuture extends ComboFutureBase<Void>
{
	private MapDBFSCASStore parent;
	private CASIDDataDescriptor segmentDescriptor;
	private long dataID;
	
	public MapDBFSStoreFuture(MapDBFSCASStore parent, CASIDDataDescriptor segmentDescriptor)
	{
		this.parent = parent;
		this.segmentDescriptor = segmentDescriptor;
	}
	
	public <V, A>MapDBFSStoreFuture(MapDBFSCASStore parent, CASIDDataDescriptor segmentDescriptor,
			AsyncCompletion<Void, ? super A>completionHandler, A attachment)
	{
		super(completionHandler, attachment);
		this.parent = parent;
		this.segmentDescriptor = segmentDescriptor;
	}
	
	public void setDataID(long dataID)
	{
		this.dataID = dataID;
	}
	
	@Override
	public void completed(Void result, Object attachment)
	{
		try
		{
			parent.addToIndex(segmentDescriptor.getCASIdentifier(), dataID);
			super.completed(result, attachment);
		}
		catch (Throwable t)
		{
			super.failed(t, attachment);
		}
	}
}

class StoreDataAsyncCompletionHandler implements AsyncCompletion<Integer, Void>
{
	private MapDBFSStoreFuture mapDBFuture;
	
	public StoreDataAsyncCompletionHandler(MapDBFSStoreFuture mapDBFuture)
	{
		this.mapDBFuture = mapDBFuture;
	}
	
	@Override
	public void completed(Integer result, Void attachment)
	{
		mapDBFuture.completed(null, null);
	}

	@Override
	public void failed(Throwable exc, Void attachment)
	{
		mapDBFuture.failed(exc, null);
	}
}

class CASIdentifierComparator implements Comparator<CASIdentifier>, Serializable
{
	private static final long	serialVersionUID	= 1L;
	public CASIdentifierComparator()
	{
		
	}
	@Override
	public int compare(CASIdentifier casID0, CASIdentifier casID1)
	{
		byte [] casID0Bytes = casID0.getBytes();
		byte [] casID1Bytes = casID1.getBytes();
		for (int checkByte = 0; checkByte < casID0Bytes.length; checkByte++)
		{
			int casID0Byte = (casID0Bytes[checkByte] & 0xff);
            int casID1Byte = (casID1Bytes[checkByte] & 0xff);
            if (casID0Byte != casID1Byte) 
            {
                return casID0Byte - casID1Byte;
            }
		}
		return 0;	// Equal!
	}
	
}

class LongSerializer implements Serializer<Long>, Serializable 
{
	private static final long	serialVersionUID	= 4211814797074995401L;

	@Override
    public void serialize(DataOutput out, Long value) throws IOException {
        if(value != null)
            out.writeLong(value);
    }

    @Override
    public Long deserialize(DataInput in, int available) throws IOException {
        if(available==0) return null;
        return in.readLong();
    }
}

/**
 * The DBFSCASStore implements a segment store within a single directory with a single database connection.
 * In the storeRoot, the StoreConfiguration.properties is store along with the root of the CAS storage tree.
 *
 */
public class MapDBFSCASStore extends AbstractFSCASStore implements CASStore
{
	Thread preenThread;
    PauseAbort preenPauser;
    
    private File mapDBIndexFile;
    DB casDB;
    HTreeMap<CASIdentifier, Long> casIDToDataIDMapper;
    static Timer commitTimer;	// Commit timer is shared by all JDBMFSCASStores
    
    public static final String kStorePropertiesFileName = "StoreConfiguration.properties";
    public static final String kCASStoreDirectoryName = "CASFiles";
    public static final String kMapDBIndelibleFileName = "Index.mapdb";
    public static final String kJDBMIndelibleFileName = "Index.jdbm";	// For upgrade detection
    public static final String kStoreIDPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.storeid";
    public static final String kCASAIDToDataIDMapperBTreeRecName = "casIDToDataIDMapper";
    
    private LRUQueue<CASIdentifier, CASIDDataDescriptor>recentCache = new LRUQueue<CASIdentifier, CASIDDataDescriptor>(256);
	private TimerTask	commitTimerTask;
	
	private long lastTimeSpaceChecked, writeStoreSpaceAvailable;
	
	public MapDBFSCASStore(File storeRoot)
	throws IOException
	{
		super(null);
		this.storeRoot = storeRoot;
		casStorageRoot = new File(storeRoot, kCASStoreDirectoryName);
		mapDBIndexFile = new File(storeRoot, kMapDBIndelibleFileName);
		initInternal(storeRoot);
		/*
		preenPauser = new PauseAbort(logger);
		preenThread = new Thread(new Runnable(){

			@Override
			public void run()
			{
				preenLoop();
			}
			
		}, "JDBMFSCASStore preener");
		preenThread.setDaemon(true);
		preenThread.start();
		*/
		if (getStatus() == null)
			setStatus(CASStoreStatus.kReady);
	}

	/**
	 * This constructor will initialize the database and the directory and properties file
	 * @param storeRoot
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @throws IOException 
	 */
	public MapDBFSCASStore(CASStoreID storeID, File storeRoot) throws IOException
	{
		super(null);
		if (storeID == null)
			throw new IllegalArgumentException("storeID cannot be null");
		if (storeRoot == null)
			throw new IllegalArgumentException("storeRoot cannot be null");
		this.storeID = storeID;
		this.storeRoot = storeRoot;
		propertiesFile = new File(storeRoot, kStorePropertiesFileName);
		casStorageRoot = new File(storeRoot, kCASStoreDirectoryName);
		mapDBIndexFile = new File(storeRoot, kMapDBIndelibleFileName);
		if (storeRoot.exists())
		{
			if (!storeRoot.isDirectory())
				throw new IllegalArgumentException(storeRoot.getAbsolutePath()+" is not a directory");
			if (propertiesFile.exists())
				throw new IllegalArgumentException("Properties file already exists in "+storeRoot.getAbsolutePath()+", refusing to overwrite");
			if (casStorageRoot.exists())
				throw new IllegalArgumentException("CASStorage already exists in "+storeRoot.getAbsolutePath()+", refusing to overwrite");
		}
		else
		{
			storeRoot.mkdir();
		}
		
		if (!casStorageRoot.mkdir())
			throw new IOException("Could not create casStorageRoot "+casStorageRoot.getAbsolutePath());
		
		Properties writeProperties = new Properties();
		writeProperties.setProperty(kStoreIDPropertyName, storeID.toString());
		FileOutputStream propertiesOutStream = new FileOutputStream(propertiesFile);
		writeProperties.store(propertiesOutStream, storeRoot.getAbsolutePath()+" initialized at "+(new Date()).toString());
		propertiesOutStream.close();
		
        createMapDBIndex(mapDBIndexFile);
		initInternal(storeRoot);
		if (getStatus() == null)
			setStatus(CASStoreStatus.kReady);
	}

	private void createMapDBIndex(File createIndexFile) throws IOException
	{
		// Create with caching, etc. disable to force write of object serialization data
        casDB = DBMaker.newFileDB(createIndexFile).cacheDisable().asyncWriteDisable().writeAheadLogDisable().make();
        HTreeMapMaker mapMaker = casDB.createHashMap(kCASAIDToDataIDMapperBTreeRecName);
        mapMaker.keySerializer(new CASIdentifierSerializer());
        mapMaker.valueSerializer(new LongSerializer());
        casIDToDataIDMapper = mapMaker.make();
        casDB.commit();
        casDB.close();
	}
	
	private void initInternal(File storeRoot) throws FileNotFoundException,
			IOException
	{

		if (storeRoot == null)
			throw new IllegalArgumentException("storeRoot cannot be null");
		if (storeRoot.exists())
		{
			if (!storeRoot.isDirectory())
				throw new IllegalArgumentException(storeRoot.getAbsolutePath()+" is not a directory");
		}
		else
		{
			throw new IllegalArgumentException(storeRoot.getAbsolutePath()+" does not exist - initialize the directory before using");
		}
		propertiesFile = new File(storeRoot, kStorePropertiesFileName);
		if (!propertiesFile.exists())
			throw new IllegalArgumentException(propertiesFile.getAbsolutePath()+" does not exist - initialize the directory before using");
		storeProperties = new Properties();
		FileInputStream propertiesInputStream = new FileInputStream(propertiesFile);
		storeProperties.load(propertiesInputStream);
		propertiesInputStream.close();
		String storeIDStr = storeProperties.getProperty(kStoreIDPropertyName);
		if (storeIDStr == null)
			throw new IllegalArgumentException(kStoreIDPropertyName+" not found in properties file "+propertiesFile.getAbsolutePath()+" - initialize the directory before using");
		storeID = (CASStoreID)ObjectIDFactory.reconstituteFromString(storeIDStr);
		
		casStorage = new FSCASSerialStorage(casStorageRoot);
		
		casDB = null;
        try
		{
        	// Need to disable all caches, otherwise insists on transaction log file being present
			casDB = DBMaker.newFileDB(mapDBIndexFile).cacheDisable().asyncWriteDisable().writeAheadLogDisable().readOnly().make();
			
		} catch (Throwable t)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Could not open "+mapDBIndexFile+" for read only"), t);
		}

        if (casDB == null || casDB.getHashMap(kCASAIDToDataIDMapperBTreeRecName) == null)
        {
			logger.error("Couldn't load BTree - store needs rebuilding");
			setStatus(CASStoreStatus.kNeedsRebuild);
			if (casDB != null)
				casDB.close();
			return;
        }
        
        if (casDB != null)
        	casDB.close();	// Re-open for read/write
        DBMaker newFileDB = DBMaker.newFileDB(mapDBIndexFile);
        newFileDB.closeOnJvmShutdown();
        newFileDB.asyncWriteDisable();
        //newFileDB.transactionDisable();
        newFileDB.cacheDisable();
		casDB = newFileDB.make();
        //casDB = DBMaker.newFileDB(mapDBIndexFile).closeOnJvmShutdown().cacheSoftRefEnable().make();
        
        casIDToDataIDMapper = casDB.getHashMap(kCASAIDToDataIDMapperBTreeRecName);
		nextDataID = -1L;
		long lastFSID = -1L;
		try
		{
			lastFSID = casStorage.getLastDataID();
		}
		catch (IOException e)
		{
			logger.warn(new WarnLogMessage("CASStorage is empty"));
		}
		
		if (lastFSID > 0)
		{
			File lastFile = casStorage.getSegmentFileForDataID(lastFSID);
			byte [] checkData = new byte[(int)lastFile.length()];
			FileInputStream checkStream = new FileInputStream(lastFile);
			checkStream.read(checkData);
			checkStream.close();
			SHA1HashID checkHash = new SHA1HashID(checkData);
			CASIdentifier checkSegmentID = new CASIdentifier(checkHash, checkData.length);

			Long checkDataID = casIDToDataIDMapper.get(checkSegmentID);
			if (checkDataID == null || checkDataID != lastFSID)
			{
				logger.error("Can't find Last ID ("+lastFSID+") from filesystem in the database - store needs reloading");
				setStatus(CASStoreStatus.kNeedsReloadOfNewObjects);
				return;
			}
		}
		nextDataID = lastFSID;
		//speedTest();
		if (commitTimer == null)
		{
			commitTimer = new Timer(true);
		}
		commitTimerTask = new TimerTask()
		{

			@Override
			public void run()
			{
				try
				{
					casDB.commit();
				}
				catch (Throwable t)
				{
					Logger.getLogger(getClass()).error("Caught exception syncing MapDBFSCASStore "+MapDBFSCASStore.this.getStoreRoot());
				}
			}
		};
		commitTimer.schedule(commitTimerTask, 30000, 30000);	// Commit everything every 
	}

	
	@Override
	public void shutdown() throws IOException
	{
		commitTimerTask.cancel();
		casDB.commit();
		casDB.close();
		
	}

	protected void speedTest() throws IOException
	{

		byte [] data = new byte[16*1024];
		for (int curByteNum = 0; curByteNum < data.length; curByteNum++)
		{
			data[curByteNum] = (byte) (Math.random() * 256);
		}
		
		CASIdentifier [] testIDs = new CASIdentifier[10000];
		Long [] dataIDs = new Long[10000];
		for (int testNum = 0; testNum < 10000; testNum++)
		{
			BitTwiddle.intToJavaByteArray(testNum, data, 0);
			testIDs[testNum] = new CASIdentifier(new SHA1HashID(data), data.length);
		}
		long startTime = System.currentTimeMillis();
		for (int testNum = 0; testNum < 10000; testNum++)
		{
			long dataID = ++nextDataID;
			CASIdentifier curKey = testIDs[testNum];
			casIDToDataIDMapper.put(curKey, dataID);
			dataIDs[testNum] = dataID;
		}
		casDB.commit();
		long endTime = System.currentTimeMillis();
		long elapsed = endTime - startTime;
		logger.error("DBFSCASStore start up insert test - 10000 database records inserted in "+elapsed+" "+(10000.0/((double)elapsed/1000.0))+" records/sec");
		
		for (int testNum = 0; testNum < 10000; testNum++)
		{
			CASIdentifier curKey = testIDs[testNum];
			Long foundDataID = casIDToDataIDMapper.get(curKey);
			if (!foundDataID.equals(dataIDs[testNum]))
				throw new InternalError("Fetch from JDBM returned bad value (expected="+dataIDs[testNum]+", got="+foundDataID+")- aborting");
			casIDToDataIDMapper.remove(curKey);
		}
		casDB.commit();
		nextDataID = -1L;
		Iterator<Entry<CASIdentifier, Long>>dataIDIterator = casIDToDataIDMapper.entrySet().iterator();
		while(dataIDIterator.hasNext())
		{
			Entry<CASIdentifier, Long>curEntry = dataIDIterator.next();
			if (curEntry.getValue() > nextDataID)
				nextDataID = curEntry.getValue();
		}
		
		for (int curByteNum = 0; curByteNum < data.length; curByteNum++)
		{
			data[curByteNum] = (byte) (Math.random() * 256);
		}
		startTime = System.currentTimeMillis();
		for (int curInsert = 0; curInsert < 10000; curInsert++)
		{
			BitTwiddle.intToJavaByteArray(curInsert, data, 0);
			CASIDDataDescriptor storeDescriptor = new CASIDMemoryDataDescriptor(data);
			storeSegment(storeDescriptor);
		}
		casDB.commit();
		endTime = System.currentTimeMillis();
		elapsed = endTime - startTime;
		logger.error("DBFSCASStore start up insert test - 10000 16K segments inserted in "+elapsed+" "+(10000.0/((double)elapsed/1000.0))+" records/sec");
	}
	

	@Override
	public CASStoreID getCASStoreID()
	{
		return storeID;
	}

	@Override
	public CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID)
			throws IOException, SegmentNotFound
	{
		CASIDDataDescriptor returnDescriptor = recentCache.get(segmentID);
		if (returnDescriptor == null)
		{
			Long dataID = casIDToDataIDMapper.get(segmentID);
			if (dataID == null)
				throw new SegmentNotFound(segmentID);
			try
			{
				returnDescriptor = casStorage.retrieveSegment(segmentID, dataID);
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				throw new SegmentNotFound(segmentID);
			}
		}
		
		if (returnDescriptor != null)
			recentCache.put(segmentID, returnDescriptor);

		return returnDescriptor;
	}

	@Override
	public void storeSegment(CASIDDataDescriptor segmentDescriptor)
	throws IOException
	{
		long dataID = ++nextDataID;

		addToIndex(segmentDescriptor.getCASIdentifier(), dataID);
		//recentCache.put(segmentDescriptor.getCASIdentifier(), segmentDescriptor);	// We don't put it into the recentCache because this is often a
																					// network data descriptor
		casStorage.storeDataSegment(segmentDescriptor, dataID);
		writeStoreSpaceAvailable -= ((long)(segmentDescriptor.getLength() / (64*1024)) + 1) * (64*1024);
		return;

	}
	
	@Override
	public synchronized void repairSegment(CASIdentifier segmentID, CASIDDataDescriptor dataDescriptor) throws IOException
	{
		Long dataID = casIDToDataIDMapper.get(segmentID);
		if (dataID == null)	// Hmmm...we didn't have it before
		{
			dataID = ++nextDataID;
			writeStoreSpaceAvailable -= ((long)(dataDescriptor.getLength() / (64*1024)) + 1) * (64*1024);
		}
		casStorage.replaceDataSegment(dataDescriptor, dataID);
	}

	@Override
	public Future<Void> storeSegmentAsync(CASIDDataDescriptor segmentDescriptor)
			throws IOException
	{
		MapDBFSStoreFuture returnFuture = new MapDBFSStoreFuture(this, segmentDescriptor);
		storeSegmentAsyncCommon(segmentDescriptor, returnFuture);
		return returnFuture;
	}

	@Override
	public <A> void storeSegmentAsync(CASIDDataDescriptor segmentDescriptor,
			AsyncCompletion<Void, A> completionHandler, A attachment)
			throws IOException
	{
		MapDBFSStoreFuture future = new MapDBFSStoreFuture(this, segmentDescriptor, completionHandler, attachment);
		storeSegmentAsyncCommon(segmentDescriptor, future);
	}

	private synchronized void storeSegmentAsyncCommon(CASIDDataDescriptor segmentDescriptor, MapDBFSStoreFuture future) throws IOException
	{
		long dataID = ++nextDataID;
		future.setDataID(dataID);
		casStorage.storeDataSegmentAsync(segmentDescriptor, dataID, new StoreDataAsyncCompletionHandler(future), null);
		writeStoreSpaceAvailable -= ((long)(segmentDescriptor.getLength() / (64*1024)) + 1) * (64*1024);
		// The dataID will be inserted into the table when the future's completion routine is run when storeDataSegmentAsync finishes
		return;
	}
	@Override
	public synchronized boolean deleteSegment(CASIdentifier releaseID) throws IOException
	{
		try
		{
			Long removeDataID = casIDToDataIDMapper.remove(releaseID);
			if (removeDataID != null)
			{
				casStorage.removeSegment(removeDataID);
				recentCache.remove(releaseID);
				return true;
			}
		} catch (IllegalArgumentException e)
		{
			// Key not found
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		return false;
	}

	@Override
	public long getNumSegments()
	{
		return casIDToDataIDMapper.size();
	}

	@Override
	public long getTotalSpace()
	{
		try
		{
			ClientFile rootDir = SystemInfo.getSystemInfo().getClientFileForFile(storeRoot);
			return rootDir.getVolume().totalSpace();
		} catch (IOException e)
		{
			logger.error(new ErrorLogMessage("Caught IOException while executing getTotalSpace"), e);
		}
		
		return 0;
	}

	@Override
	public synchronized long getUsedSpace()
	{
		try
		{
			ClientFile rootDir = SystemInfo.getSystemInfo().getClientFileForFile(storeRoot);
			return rootDir.getVolume().totalSpace() - rootDir.getVolume().freeSpace();
		} catch (IOException e)
		{
			logger.error(new ErrorLogMessage("Caught IOException while executing getTotalSpace"), e);
		}
		/*
		try
		{
			ResultSet getUsedSpaceSet = getSpaceUsedStmt.executeQuery();
			if (getUsedSpaceSet.next())
			{
				long bytesInUse = getUsedSpaceSet.getLong(1);
				return bytesInUse;
			}
		} catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		return 0;
	}

	@Override
	public synchronized long getFreeSpace()
	{
		if (System.currentTimeMillis() - lastTimeSpaceChecked  > CASStoreManager.kTimeToWaitBetweenSpaceChecked)
		{
			try
			{
				ClientFile rootDir = SystemInfo.getSystemInfo().getClientFileForFile(storeRoot);
				// Do a course correction periodically
				writeStoreSpaceAvailable =  rootDir.getVolume().freeSpace();
				lastTimeSpaceChecked = System.currentTimeMillis();
			} catch (IOException e)
			{
				logger.error(new ErrorLogMessage("Caught IOException while executing getTotalSpace"), e);
			}
		}
		return writeStoreSpaceAvailable;
	}

	public File getStoreRoot()
	{
		return storeRoot;
	}
	
	/**
	 * The preen loop checks CASSegments for consistency and removes unreferenced segments.  It also
	 * looks for references segments that do not exist
	 */
	public void preenLoop()
	{
		while(true)
		{
			if (getStatus() == CASStoreStatus.kReady)
			{
				try
				{
					long startingID = casStorage.getFirstDataID();
					long endingID = casStorage.getLastDataID();
					for (long checkID = startingID; checkID < endingID + 1; checkID++)
					{
						preenPauser.checkPauseAndAbort();
						File checkFile;
						synchronized(this)
						{
							checkFile = casStorage.getSegmentFileForDataID(checkID);
						}
						if (checkFile != null && checkFile.exists())
						{
							byte [] checkData = new byte[(int)checkFile.length()];
							FileInputStream checkStream = new FileInputStream(checkFile);
							checkStream.read(checkData);
							checkStream.close();
							SHA1HashID checkHash = new SHA1HashID(checkData);
							CASIdentifier checkSegmentID = new CASIdentifier(checkHash, checkData.length);
							synchronized(this)
							{
								Long dbDataID = casIDToDataIDMapper.get(checkSegmentID);
								if (dbDataID == null)
								{
									// Figure out if it's actually bad here
									logger.warn(checkID+" was orphaned, reinserting to DB");
									casIDToDataIDMapper.put(checkSegmentID, checkID);
								}
								else
								{
									if (checkID != dbDataID)
									{
										logger.warn(checkID+" was orphaned, removing");
										casStorage.removeSegment(checkID);
									}
								}
							}
						}
						else
						{
							synchronized(this)
							{
								Iterator<Entry<CASIdentifier, Long>>dataIDIterator = casIDToDataIDMapper.entrySet().iterator();
								while(dataIDIterator.hasNext())
								{
									Entry<CASIdentifier, Long>curEntry = dataIDIterator.next();
									if (curEntry.getValue() == checkID)
									{
										logger.warn(checkID+" was in the index but missing from disk - removing from DB");
										casIDToDataIDMapper.remove(curEntry.getKey());
										break;
									}
								}
							}
						}
						if (checkID % 1000 == 0)
						{
							logger.warn("Preen at id "+checkID+" out of "+endingID);
						}
						Thread.sleep(100L);
					}

				} catch (IOException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}  catch (InterruptedException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				} catch (AbortedException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
				try
				{
					Thread.sleep(7*24*60*60*1000);
				} catch (InterruptedException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
			else
			{
				// Sleep for a while waiting for the store to become available
				try
				{
					Thread.sleep(60*1000);
				} catch (InterruptedException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
		}
	}

	@Override
	public void sync() throws IOException
	{
		casDB.commit();
	}

	

	protected boolean upgradeIfNecessary() throws FileNotFoundException, IOException
	{
		boolean upgraded = false;
		
		File jdbmIndexFileDB = new File(storeRoot, kJDBMIndelibleFileName+".db");
		if (jdbmIndexFileDB.exists())
		{
			// upgradeJDBMtoMapDB will set the status to kReady if appropriate
			if (upgradeJDBMtoMapDB())
				upgraded = true;
		}
		return upgraded;
	}

	protected void rebuildPostProcess(Object rebuildInfo)
			throws FileNotFoundException, IOException
	{
		long startRebuildPostProcessTime = System.currentTimeMillis();
		File rebuildIndex = (File)rebuildInfo;
		casDB.commit();
		casDB.close();
		System.out.println("Commit took "+ (System.currentTimeMillis() - startRebuildPostProcessTime));
		File rebuildIndexFileDB = new File(rebuildIndex.getAbsolutePath());
		File newJdbmIndexFileDB = new File(mapDBIndexFile.getAbsolutePath());
		rebuildIndexFileDB.renameTo(newJdbmIndexFileDB);
		
		File rebuildIndexFileLog = new File(rebuildIndex.getAbsolutePath()+".t");
		File newJdbmIndexFileLog = new File(mapDBIndexFile.getAbsolutePath()+".t");
		rebuildIndexFileLog.renameTo(newJdbmIndexFileLog);
		
		File rebuildIndexPhysFile = new File(rebuildIndex.getAbsolutePath()+".p");
		File newJdbmIndexPhysFile= new File(mapDBIndexFile.getAbsolutePath()+".p");
		rebuildIndexPhysFile.renameTo(newJdbmIndexPhysFile);
		
		initInternal(storeRoot);
		logger.error(new ErrorLogMessage("Finished rebuild on store {0}, path={1}", getCASStoreID(), casStorageRoot));
	}

	protected File prepareForRebuild() throws IOException
	{
		int saveIndexNum = 0;
		File mapDBIndexFileDB = new File(mapDBIndexFile.getAbsolutePath());
		if (mapDBIndexFileDB.exists())
		{
			File saveMapDBIndexFile = new File(storeRoot, kMapDBIndelibleFileName+"-save-"+saveIndexNum);
			while (saveMapDBIndexFile.exists())
			{
				saveIndexNum++;
				saveMapDBIndexFile = new File(storeRoot, kMapDBIndelibleFileName+"-save-"+saveIndexNum);
			}
			logger.error(new ErrorLogMessage("Renaming corrupted index file {0} to {1}", mapDBIndexFile, saveMapDBIndexFile));
			mapDBIndexFileDB.renameTo(saveMapDBIndexFile);
		}
		File mapDBLogFile = new File(mapDBIndexFile.getAbsolutePath()+".t");
		if (mapDBLogFile.exists())
		{
			File saveMapDBLogFile = new File(storeRoot, kMapDBIndelibleFileName+".t-save-"+saveIndexNum);
			while (saveMapDBLogFile.exists())
			{
				saveIndexNum++;
				saveMapDBLogFile = new File(storeRoot, kMapDBIndelibleFileName+".t-save-"+saveIndexNum);
			}
			logger.error(new ErrorLogMessage("Renaming corrupted index file {0} to {1}", mapDBIndexFile, saveMapDBLogFile));
			mapDBLogFile.renameTo(saveMapDBLogFile);
		}
		File mapDBPhysFile = new File(mapDBIndexFile.getAbsolutePath()+".p");
		if (mapDBPhysFile.exists())
		{
			File saveMapDBPhysFile = new File(storeRoot, kMapDBIndelibleFileName+".p-save-"+saveIndexNum);
			while (saveMapDBPhysFile.exists())
			{
				saveIndexNum++;
				saveMapDBPhysFile = new File(storeRoot, kMapDBIndelibleFileName+".p-save-"+saveIndexNum);
			}
			logger.error(new ErrorLogMessage("Renaming corrupted index file {0} to {1}", mapDBIndexFile, saveMapDBPhysFile));
			mapDBPhysFile.renameTo(saveMapDBPhysFile);
		}
		File rebuildIndex = new File(storeRoot, kMapDBIndelibleFileName+"-rebuild");
		createMapDBIndex(rebuildIndex);
		
        DBMaker newFileDB = DBMaker.newFileDB(rebuildIndex);
        newFileDB.closeOnJvmShutdown();
        newFileDB.asyncWriteDisable();
        newFileDB.transactionDisable();
		casDB = newFileDB.make();
        casIDToDataIDMapper = casDB.getHashMap(kCASAIDToDataIDMapperBTreeRecName);
        
        logger.error(new ErrorLogMessage("Starting rebuild on store {0}, path={1}", getCASStoreID(), casStorageRoot));
        
		return rebuildIndex;
	}
	
	@Override
	public void reloadNewObjects() throws IOException
	{
		logger.error(new ErrorLogMessage("Starting reload on store {0}, path={1}", getCASStoreID(), casStorageRoot));
		long lastFSID = -1L;
		try
		{
			lastFSID = casStorage.getLastDataID();
		}
		catch (IOException e)
		{
			logger.warn(new WarnLogMessage("CASStorage is empty"));
			throw new InternalError("Asked to reload object store but no storage");
		}
		
		if (lastFSID > 0)
		{
			long lastIndexFSID = -1L;
			Collection<Long> fsIDvalues = casIDToDataIDMapper.values();
			Iterator<Long>fsIDIterator = fsIDvalues.iterator();
			long valuesScanned = 0;
			long numValues = fsIDvalues.size();
			while (fsIDIterator.hasNext())
			{
				long curFSID = fsIDIterator.next();
				if (curFSID > lastIndexFSID)
					lastIndexFSID = curFSID;
				valuesScanned ++;
				if (valuesScanned % 1000 == 0)
					logger.error(new ErrorLogMessage("Reloading scanned value {0} of {1}", new Serializable[]{valuesScanned, numValues}));
			}
			long numObjectsToReload = lastFSID - lastIndexFSID;
			logger.error(new ErrorLogMessage("Last indexed FSID = {0}, last FS ID in physical storage is {1}, {2} objects to reload",
					new Serializable[]{lastIndexFSID, lastFSID, numObjectsToReload}));
			for (long reloadFSID = lastIndexFSID + 1; reloadFSID < lastFSID + 1; reloadFSID++)
			{
				File reloadFile = casStorage.getSegmentFileForDataID(reloadFSID);
				if (reloadFile != null)
				{
					byte [] checkData = new byte[(int)reloadFile.length()];
					FileInputStream checkStream = new FileInputStream(reloadFile);
					checkStream.read(checkData);
					checkStream.close();
					SHA1HashID checkHash = new SHA1HashID(checkData);
					CASIdentifier checkSegmentID = new CASIdentifier(checkHash, checkData.length);
					
					Long reloadDataID = casIDToDataIDMapper.get(checkSegmentID);
					if (reloadDataID != null && reloadDataID != reloadFSID)
					{
						logger.error(new ErrorLogMessage("FSID {0} with CASID {1} is already in index with FSID {2}, skipping insert",
								new Serializable[]{reloadFSID, checkSegmentID, reloadDataID}));
					}
					else
					{
						casIDToDataIDMapper.put(checkSegmentID, reloadFSID);
						logger.error(new ErrorLogMessage("Reloaded FSID {0} ({1} of {2}) with CASID {3}", 
								new Serializable[]{reloadFSID, (reloadFSID - lastIndexFSID), numObjectsToReload, checkSegmentID}));
					}
				}
				if (reloadFSID % 1000 == 0)
					casDB.commit();
			}
		}
		casDB.commit();
		casDB.close();
		initInternal(storeRoot);
		logger.error(new ErrorLogMessage("Finished reload on store {0}, path={1}", getCASStoreID(), casStorageRoot));
		setStatus(CASStoreStatus.kReady);
	}

	public synchronized boolean upgradeJDBMtoMapDB() throws FileNotFoundException, IOException
	{
		setStatus(CASStoreStatus.kRebuilding);
		logger.error(new ErrorLogMessage("Starting upgrade on store {0}, path={1}", getCASStoreID(), casStorageRoot));
		File jdbmIndexFile = new File(storeRoot, kJDBMIndelibleFileName);;
		

        Properties jdbmOptions = new Properties();
        jdbmOptions.put(RecordManagerOptions.DISABLE_TRANSACTIONS, "FALSE");
        jdbmOptions.put(RecordManagerOptions.CACHE_SIZE, "100000");
        RecordManager recordManager = RecordManagerFactory.createRecordManager(jdbmIndexFile.getAbsolutePath(), jdbmOptions);
        long recid = recordManager.getNamedObject( kCASAIDToDataIDMapperBTreeRecName );
        if ( recid == 0 )
        {
        	logger.error("Could not load JDBM index "+jdbmIndexFile);
        	setStatus(CASStoreStatus.kNeedsRebuild);
        	return false;
        }
        
        BTree<CASIdentifier, Long>jdbmDataMapper = BTree.load( recordManager, recid );
		
        File rebuildIndex = new File(storeRoot, kMapDBIndelibleFileName+"-rebuild");
		createMapDBIndex(rebuildIndex);
		
        casDB = DBMaker.newFileDB(rebuildIndex).closeOnJvmShutdown().cacheSoftRefEnable().make();
        casIDToDataIDMapper = casDB.getHashMap(kCASAIDToDataIDMapperBTreeRecName);
        
        logger.error("Upgrading "+jdbmDataMapper.entryCount()+" entries from JDBM to MapDB");
        TupleBrowser<CASIdentifier, Long>browser = jdbmDataMapper.browse();
        Tuple<CASIdentifier, Long>curTuple = new Tuple<CASIdentifier, Long>();
        long curRecordNum = 0;
        while (browser.getNext(curTuple))
        {
        	casIDToDataIDMapper.put(curTuple.getKey(), curTuple.getValue());
        	curRecordNum++;
        	if (curRecordNum % 1000 == 0)
        		logger.error("Upgrading record "+curRecordNum);
        }
        casDB.commit();
		casDB.close();
		
		casDB.close();
		
		int saveIndexNum = 0;
		File jdbmIndexFileDB = new File(jdbmIndexFile.getAbsolutePath()+".db");
		if (jdbmIndexFileDB.exists())
		{
			File saveJDBMIndexFile = new File(storeRoot, kJDBMIndelibleFileName+".db-save-"+saveIndexNum);
			while (saveJDBMIndexFile.exists())
			{
				saveIndexNum++;
				saveJDBMIndexFile = new File(storeRoot, kJDBMIndelibleFileName+".db-save-"+saveIndexNum);
			}
			logger.error(new ErrorLogMessage("Renaming old JDBM index file {0} to {1}", jdbmIndexFile, saveJDBMIndexFile));
			jdbmIndexFileDB.renameTo(saveJDBMIndexFile);
		}
		File jdbmIndexFileLog = new File(jdbmIndexFile.getAbsolutePath()+".lg");
		if (jdbmIndexFileLog.exists())
		{
			File saveJDBMIndexFile = new File(storeRoot, kJDBMIndelibleFileName+".lg-save-"+saveIndexNum);
			while (saveJDBMIndexFile.exists())
			{
				saveIndexNum++;
				saveJDBMIndexFile = new File(storeRoot, kJDBMIndelibleFileName+".lg-save-"+saveIndexNum);
			}
			logger.error(new ErrorLogMessage("Renaming old JDBM index file {0} to {1}", jdbmIndexFile, saveJDBMIndexFile));
			jdbmIndexFileLog.renameTo(saveJDBMIndexFile);
		}
		File rebuildIndexFileDB = new File(rebuildIndex.getAbsolutePath());
		File newJdbmIndexFileDB = new File(mapDBIndexFile.getAbsolutePath());
		rebuildIndexFileDB.renameTo(newJdbmIndexFileDB);
		
		File rebuildIndexFileLog = new File(rebuildIndex.getAbsolutePath()+".t");
		File newJdbmIndexFileLog = new File(mapDBIndexFile.getAbsolutePath()+".t");
		rebuildIndexFileLog.renameTo(newJdbmIndexFileLog);
		
		File rebuildIndexPhysFile = new File(rebuildIndex.getAbsolutePath()+".p");
		File newJdbmIndexPhysFile= new File(mapDBIndexFile.getAbsolutePath()+".p");
		rebuildIndexPhysFile.renameTo(newJdbmIndexPhysFile);
        
		initInternal(storeRoot);
		logger.error(new ErrorLogMessage("Finished upgrade on store {0}, path={1}", getCASStoreID(), casStorageRoot));
		setStatus(CASStoreStatus.kReady);
		return true;
	}
	
	@Override
	protected long getStartingID() throws IOException
	{
		return casStorage.getFirstDataID();
	}
	
	@Override
	protected long getLastID() throws IOException
	{
		return casStorage.getLastDataID();
	}
	@Override
	protected void rebuildCommit() throws IOException
	{
		casDB.commit();	
	}

	@Override
	protected synchronized void addToIndex(CASIdentifier checkSegmentID, long checkID)
	{
		casIDToDataIDMapper.put(checkSegmentID, checkID);
	}
	
	@Override
	public boolean verifySegment(CASIdentifier identifierToVerify)
	{
		try
		{
			CASIDDataDescriptor checkDescriptor = retrieveSegment(identifierToVerify);
			if (checkDescriptor != null)
			{
				if (checkDescriptor.getLength() == identifierToVerify.getSegmentLength())
				{
					SHA1HashID checkID = new SHA1HashID(checkDescriptor.getByteBuffer());
					if (checkID.equals(identifierToVerify.getHashID()))
					{
						return true;
					}
					else
					{
						Long dataID = casIDToDataIDMapper.get(identifierToVerify);
						ArrayList<String>storePathList = casStorage.getStorePathForDataID( dataID);
						StringBuilder storePathBuilder = new StringBuilder();
						for (String curPathElement:storePathList)
						{
							storePathBuilder.append('/');
							storePathBuilder.append(curPathElement);
						}
						logger.error(new ErrorLogMessage("Hash ID does not verify for existing segment {0}, got hash = {1} (data ID = {2}, file = {3})", new Serializable []{
							identifierToVerify, checkID, dataID, storePathBuilder.toString()
						}));
					}
				}
			}
		}
		catch (Throwable t)
		{
			logger.error(new ErrorLogMessage("Caught exception while verifying segment {0}", new Serializable[]{identifierToVerify}));
		}
		return false;
	}
}
