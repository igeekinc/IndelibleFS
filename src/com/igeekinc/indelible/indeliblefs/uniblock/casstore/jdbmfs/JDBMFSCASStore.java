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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.jdbmfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.AbstractCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreStatus;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.FSCASSerialStorage;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.ClientFile;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.WarnLogMessage;
import com.igeekinc.util.objectcache.LRUQueue;
import com.igeekinc.util.pauseabort.AbortedException;
import com.igeekinc.util.pauseabort.PauseAbort;

class CASIdentifierComparator implements Comparator<CASIdentifier>, Serializable
{
	/**
	 * 
	 */
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
/**
 * The DBFSCASStore implements a segment store within a single directory with a single database connection.
 * In the storeRoot, the StoreConfiguration.properties is store along with the root of the CAS storage tree.
 *
 */
public class JDBMFSCASStore extends AbstractCASStore implements CASStore
{
	private CASStoreID storeID;
	private File storeRoot;
	private File propertiesFile;
    private File casStorageRoot;
    private File jdbmIndexFile;
    private RecordManager recordManager;
    private BTree<CASIdentifier, Long>casIDToDataIDMapper;
    private long nextDataID;
    FSCASSerialStorage casStorage;
    
    Properties storeProperties;

    Thread preenThread;
    PauseAbort preenPauser;
    
    static Timer commitTimer;	// Commit timer is shared by all JDBMFSCASStores
    
    public static final String kStorePropertiesFileName = "StoreConfiguration.properties";
    public static final String kCASStoreDirectoryName = "CASFiles";
    public static final String kJDBMIndelibleFileName = "Index.jdbm";
    public static final String kStoreIDPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.storeid";
    public static final String kCASAIDToDataIDMapperBTreeRecName = "casIDToDataIDMapper";
    
    private LRUQueue<CASIdentifier, CASIDDataDescriptor>recentCache = new LRUQueue<CASIdentifier, CASIDDataDescriptor>(256);
	private TimerTask	commitTimerTask;
	
	private long lastTimeSpaceChecked, writeStoreSpaceAvailable;
	
	public JDBMFSCASStore(File storeRoot)
	throws IOException
	{
		super(null);
		this.storeRoot = storeRoot;
		casStorageRoot = new File(storeRoot, kCASStoreDirectoryName);
		jdbmIndexFile = new File(storeRoot, kJDBMIndelibleFileName);
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
	public JDBMFSCASStore(CASStoreID storeID, File storeRoot) throws IOException
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
		jdbmIndexFile = new File(storeRoot, kJDBMIndelibleFileName);
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
		
        createJDBMIndex(jdbmIndexFile);
		initInternal(storeRoot);
		if (getStatus() == null)
			setStatus(CASStoreStatus.kReady);
	}

	private void createJDBMIndex(File createIndexFile) throws IOException
	{
		Properties jdbmOptions = new Properties();
        jdbmOptions.put(RecordManagerOptions.DISABLE_TRANSACTIONS, "TRUE");
        recordManager = RecordManagerFactory.createRecordManager(createIndexFile.getAbsolutePath(), jdbmOptions);
        casIDToDataIDMapper = BTree.createInstance( recordManager, new CASIdentifierComparator() );
        recordManager.setNamedObject( kCASAIDToDataIDMapperBTreeRecName, casIDToDataIDMapper.getRecid() );
        recordManager.commit();
        recordManager.close();
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
		
        Properties jdbmOptions = new Properties();
        jdbmOptions.put(RecordManagerOptions.DISABLE_TRANSACTIONS, "FALSE");
        jdbmOptions.put(RecordManagerOptions.CACHE_SIZE, "100000");
        recordManager = RecordManagerFactory.createRecordManager(jdbmIndexFile.getAbsolutePath(), jdbmOptions);
        long recid = recordManager.getNamedObject( kCASAIDToDataIDMapperBTreeRecName );
        if ( recid == 0 )
        {
        	logger.error("Could not load JDBM index "+jdbmIndexFile);
        	setStatus(CASStoreStatus.kNeedsRebuild);
        	return;
        }
        
        casIDToDataIDMapper = BTree.load( recordManager, recid );

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

			Long checkDataID = casIDToDataIDMapper.find(checkSegmentID);
			if (checkDataID == null || checkDataID != lastFSID)
			{
				logger.error("Can't find Last ID ("+lastFSID+") from filesystem in the database - store needs rebuilding");
				setStatus(CASStoreStatus.kNeedsRebuild);
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
					recordManager.commit();
				} catch (IOException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
		};
		commitTimer.schedule(commitTimerTask, 30000, 30000);	// Commit everything every 
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
			casIDToDataIDMapper.insert(curKey, dataID, false);
			dataIDs[testNum] = dataID;
		}
		recordManager.commit();
		long endTime = System.currentTimeMillis();
		long elapsed = endTime - startTime;
		logger.error("DBFSCASStore start up insert test - 10000 database records inserted in "+elapsed+" "+(10000.0/((double)elapsed/1000.0))+" records/sec");
		
		for (int testNum = 0; testNum < 10000; testNum++)
		{
			CASIdentifier curKey = testIDs[testNum];
			Long foundDataID = casIDToDataIDMapper.find(curKey);
			if (!foundDataID.equals(dataIDs[testNum]))
				throw new InternalError("Fetch from JDBM returned bad value (expected="+dataIDs[testNum]+", got="+foundDataID+")- aborting");
			casIDToDataIDMapper.remove(curKey);
		}
		recordManager.commit();
		nextDataID = -1L;
		TupleBrowser<CASIdentifier, Long>dataIDBrowser = casIDToDataIDMapper.browse();
		Tuple<CASIdentifier, Long>curTuple = new Tuple<CASIdentifier, Long>();
		while(dataIDBrowser.getNext(curTuple))
		{
			if (curTuple.getValue() > nextDataID)
				nextDataID = curTuple.getValue();
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
		recordManager.commit();
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
			throws IOException
	{
		CASIDDataDescriptor returnDescriptor = recentCache.get(segmentID);
		if (returnDescriptor == null)
		{
			Long dataID = casIDToDataIDMapper.find(segmentID);
			if (dataID == null)
				throw new IOException("Could not find dataID for segmentID "+segmentID);
			returnDescriptor = casStorage.retrieveSegment(segmentID, dataID);
		}
		
		if (returnDescriptor != null)
			recentCache.put(segmentID, returnDescriptor);

		return returnDescriptor;
	}

	@Override
	public synchronized void storeSegment(CASIDDataDescriptor segmentDescriptor)
	throws IOException
	{
		long dataID = ++nextDataID;

		casIDToDataIDMapper.insert(segmentDescriptor.getCASIdentifier(), dataID, true);
		//recentCache.put(segmentDescriptor.getCASIdentifier(), segmentDescriptor);	// We don't put it into the recentCache because this is often a
																					// network data descriptor
		casStorage.storeDataSegment(segmentDescriptor, dataID);
		writeStoreSpaceAvailable -= ((long)(segmentDescriptor.getLength() / (64*1024)) + 1) * (64*1024);
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
		return casIDToDataIDMapper.entryCount();
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
								Long dbDataID = casIDToDataIDMapper.find(checkSegmentID);
								if (dbDataID == null)
								{
									// Figure out if it's actually bad here
									logger.warn(checkID+" was orphaned, reinserting to DB");
									casIDToDataIDMapper.insert(checkSegmentID, checkID, false);
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
								TupleBrowser<CASIdentifier, Long>dataIDBrowser = casIDToDataIDMapper.browse();
								Tuple<CASIdentifier, Long>curTuple = new Tuple<CASIdentifier, Long>();
								while(dataIDBrowser.getNext(curTuple))
								{
									if (curTuple.getValue() == checkID)
									{
										logger.warn(checkID+" was in the index but missing from disk - removing from DB");
										casIDToDataIDMapper.remove(curTuple.getKey());
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
		recordManager.commit();
	}

	@Override
	public void shutdown() throws IOException
	{
		commitTimerTask.cancel();
		recordManager.commit();
		recordManager.close();
	}

	@Override
	public synchronized void rebuild() throws IOException
	{
		setStatus(CASStoreStatus.kRebuilding);
		logger.error(new ErrorLogMessage("Starting rebuild on store {0}, path={1}", getCASStoreID(), casStorageRoot));
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
			logger.error(new ErrorLogMessage("Renaming corrupted index file {0} to {1}", jdbmIndexFile, saveJDBMIndexFile));
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
			logger.error(new ErrorLogMessage("Renaming corrupted index file {0} to {1}", jdbmIndexFile, saveJDBMIndexFile));
			jdbmIndexFileLog.renameTo(saveJDBMIndexFile);
		}
		File rebuildIndex = new File(storeRoot, kJDBMIndelibleFileName+"-rebuild");
		createJDBMIndex(rebuildIndex);
		
        Properties jdbmOptions = new Properties();
        jdbmOptions.put(RecordManagerOptions.DISABLE_TRANSACTIONS, "FALSE");
        recordManager = RecordManagerFactory.createRecordManager(rebuildIndex.getAbsolutePath(), jdbmOptions);
        long recid = recordManager.getNamedObject( kCASAIDToDataIDMapperBTreeRecName );
        if ( recid == 0 )
        {
        	throw new IOException("Rebuild thread could not load JDBM index "+jdbmIndexFile);
        }
        
        casIDToDataIDMapper = BTree.load( recordManager, recid );
		long startingID = casStorage.getFirstDataID();
		long endingID = casStorage.getLastDataID();
		
		logger.error(new ErrorLogMessage("First found id = {0}, last found ID = {1}, beginning processing...", startingID, endingID));
		for (long checkID = startingID; checkID < endingID + 1; checkID++)
		{
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
				casIDToDataIDMapper.insert(checkSegmentID, checkID, false);
			}
		}
		recordManager.commit();
		recordManager.close();
		
		File rebuildIndexFileDB = new File(rebuildIndex.getAbsolutePath()+".db");
		File newJdbmIndexFileDB = new File(jdbmIndexFile.getAbsolutePath()+".db");
		rebuildIndexFileDB.renameTo(newJdbmIndexFileDB);
		
		File rebuildIndexFileLog = new File(rebuildIndex.getAbsolutePath()+".lg");
		File newJdbmIndexFileLog = new File(jdbmIndexFile.getAbsolutePath()+".lg");
		rebuildIndexFileLog.renameTo(newJdbmIndexFileLog);
		initInternal(storeRoot);
		logger.error(new ErrorLogMessage("Finished rebuild on store {0}, path={1}", getCASStoreID(), casStorageRoot));
		setStatus(CASStoreStatus.kReady);
	}

	@Override
	public void reloadNewObjects() throws IOException
	{
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean verifySegment(CASIdentifier identifierToVerify)
	{
		// TODO - make this work
		return true;
	}	
}
