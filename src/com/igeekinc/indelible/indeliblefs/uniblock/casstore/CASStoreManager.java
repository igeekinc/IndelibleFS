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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.DBFSCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.jdbmfs.JDBMFSCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.mapdb.MapDBFSCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3.S3CASStore;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.exceptions.DestinationFullException;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.WarnLogMessage;

/**
 * The CASStoreManager is responsible for initializing, managing and disposing of CASStores.
 * The CASStoreManager keeps track of all stores in a persistent properties file.  The CASStoreManager
 * manages local and remote stores.
 * 
 * The properties file contains entries in this style:
 * com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager.storeInfo.0=<store ID>:<store type>:<store type specific information>
 * 
 * The following store types are defined:
 * 
 * DBFSCASStore - com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.DBFSCASStore
 * RemoteCASStore - a remotely accessible store
 *
 */

class CASStoreRebuildRunnable implements Runnable
{
	CASStore storeToRebuild;
	
	public CASStoreRebuildRunnable(CASStore storeToRebuild)
	{
		this.storeToRebuild = storeToRebuild;
	}
	
	public void run()
	{
		try
		{
			storeToRebuild.rebuild();
		}
		catch (Throwable t)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Rebuild of store "+storeToRebuild.getCASStoreID()+" failed with exception"), t);
		}
	}
}

class CASStoreReloadNewObjectsRunnable implements Runnable
{
	CASStore storeToReload;
	
	public CASStoreReloadNewObjectsRunnable(CASStore storeToReload)
	{
		this.storeToReload = storeToReload;
	}
	
	public void run()
	{
		try
		{
			storeToReload.reloadNewObjects();
		}
		catch (Throwable t)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Reload new objects on store "+storeToReload.getCASStoreID()+" failed with exception"), t);
		}
		if (storeToReload.getStatus().equals(CASStoreStatus.kNeedsRebuild))
		{
			
			try
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Reload new objects failed, rebuilding store"));
				storeToReload.rebuild();
			} catch (Throwable t)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Rebuild of store "+storeToReload.getCASStoreID()+" failed with exception"), t);

			}
		}
	}
}
public class CASStoreManager
{
	private static final int kFreeSpaceMargin = 500 * 1024 * 1024;
	// CASStore is an interface and cannot have an initializer so we put it here
	static public void initMappings()
	{
		ObjectIDFactory.addMapping(CASStore.class, CASStoreID.class);
	}
	private HashMap<CASStoreID, CASStore> stores = new HashMap<CASStoreID, CASStore>();
	private ObjectIDFactory oidFactory;
	private Logger logger;
	private Properties storeManagerProperties;
	private File storeManagerPropertiesDirectory, storeManagerPropertiesFile;
	private CASStore writeStore;
	
	public static long kTimeToWaitBetweenSpaceChecked = 30000L;
	
	public static final String kStoreManagerPropertiesFileName = "com.igeekinc.casstoremanager.properties";
	public static final String kInitDBFSStorePropertiesFileName = "com.igeekinc.dbfscasstore.init.properties";
	public static final String kStoreInfoPrefix = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager.storeInfo";
	public static final String kDBFSCASStoreType = "DBFSCASStore";
	public static final String kJDBMFSCASStoreType = "JDBMFSCASStore";
	public static final String kMapDBFSCASStoreType = "MapDBFSCASStore";
	public static final String kS3CASStoreType = "S3CASStore";
	
	public CASStoreManager(ObjectIDFactory oidFactory, File storeManagerPropertiesDirectory) throws IOException
	{
		logger = Logger.getLogger(getClass());
		this.oidFactory = oidFactory;
		this.storeManagerPropertiesDirectory = storeManagerPropertiesDirectory;
		this.storeManagerPropertiesFile = new File(storeManagerPropertiesDirectory, kStoreManagerPropertiesFileName);
		storeManagerProperties = new Properties();
		if (storeManagerPropertiesFile.exists())
		{
			FileInputStream propertiesInputStream = new FileInputStream(storeManagerPropertiesFile);
			storeManagerProperties.load(propertiesInputStream);
			propertiesInputStream.close();
		}
		for (Object curKeyObj:storeManagerProperties.keySet())
		{
			if (curKeyObj instanceof String)
			{
				String curKey = (String)curKeyObj;

				if (curKey.startsWith(kStoreInfoPrefix))
				{
					boolean parsed = false;
					String storeInfo = storeManagerProperties.getProperty(curKey);
					int firstColonPos =  storeInfo.indexOf(':');
					if (firstColonPos >= 0)
					{
						int secondColonPos = storeInfo.indexOf(':', firstColonPos + 1);
						if (secondColonPos > 0)
						{
							try
							{
								String storeIDStr = storeInfo.substring(0, firstColonPos);
								CASStoreID curStoreID = (CASStoreID) ObjectIDFactory.reconstituteFromString(storeIDStr);
								String storeType = storeInfo.substring(firstColonPos + 1, secondColonPos);
								CASStore newStore = null;
								String initString = storeInfo.substring(secondColonPos + 1);
								if (storeType.equals(kDBFSCASStoreType))
								{
									File storeDirectory = new File(initString);
									newStore = new DBFSCASStore(storeDirectory);
								}
								if (storeType.equals(kJDBMFSCASStoreType))
								{
									File storeDirectory = new File(initString);
									newStore = new JDBMFSCASStore(storeDirectory);
								}
								if (storeType.equals(kMapDBFSCASStoreType))
								{
									File storeDirectory = new File(initString);
									newStore = new MapDBFSCASStore(storeDirectory);
								}
								if (storeType.equals(kS3CASStoreType))
								{
									File initPropertiesFile = new File(initString);
									newStore = new S3CASStore(initPropertiesFile);
								}
								if (newStore.getCASStoreID().equals(curStoreID))
								{
									stores.put(curStoreID, newStore);
									parsed = true;
								}
								if (newStore.getStatus() == CASStoreStatus.kNeedsRebuild)
								{
									rebuild(newStore);
								}
								if (newStore.getStatus() == CASStoreStatus.kNeedsReloadOfNewObjects)
								{
									reloadNewObjects(newStore);
								}
								newStore.addStatusChangeListener(new CASStoreStatusChangedListener()
								{
									
									@Override
									public void casStoreStatusChangedEvent(CASStoreStatusChangedEvent event)
									{
										// TODO Auto-generated method stub
										
									}
								});
							}
							catch (Throwable t)
							{
								logger.error(new ErrorLogMessage("Got unexpected error parsing "+storeManagerPropertiesFile+", continuing..."), t);
							}
						}
					}
					if (!parsed)
					{
						logger.error("Ignoring malformed server info from "+storeManagerPropertiesFile+" "+curKey+" = "+storeInfo);
					}
				}
			}
		}
		initStores();
		boolean storesNotAvailable = false;
		do
		{
			storesNotAvailable = false;
			for (CASStore checkStore:stores.values())
			{
				if (checkStore.getStatus() != CASStoreStatus.kReady && checkStore.getStatus() != CASStoreStatus.kReadOnly)
				{
					logger.warn(new WarnLogMessage("CASStore {0} is not available...", checkStore.getCASStoreID()));
					storesNotAvailable = true;
				}
			}
			if (storesNotAvailable)
			{
				try
				{
					logger.warn(new WarnLogMessage("CASStoreManager waiting for stores to become available.  Sleeping for 20s"));
					Thread.sleep(20000);
				} catch (InterruptedException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
		} while (storesNotAvailable);
	}
	
	public void rebuild(CASStore storeToRebuild)
	{
		CASStoreRebuildRunnable runnable = new CASStoreRebuildRunnable(storeToRebuild);
		Thread rebuildThread = new Thread(runnable, "CASStore rebuild thread");
		rebuildThread.start();
	}
	
	public void reloadNewObjects(CASStore storeToRebuild)
	{
		CASStoreReloadNewObjectsRunnable runnable = new CASStoreReloadNewObjectsRunnable(storeToRebuild);
		Thread reloadThread = new Thread(runnable, "CASStore reload thread");
		reloadThread.start();
	}
	public synchronized CASStoreID initDBFSCASStore(File storeRoot, String dbURL, String dbUser, String dbPassword)
	throws IOException
	{
		CASStoreID storeID = (CASStoreID) oidFactory.getNewOID(CASStore.class);
		CASStore newStore = new DBFSCASStore(storeID, storeRoot, dbURL, dbUser, dbPassword);
		addCASStore(newStore);
		return storeID;
	}
	
	public synchronized CASStoreID initJDBMFSCASStore(File storeRoot)
	throws IOException
	{
		CASStoreID storeID = (CASStoreID) oidFactory.getNewOID(CASStore.class);
		CASStore newStore = new JDBMFSCASStore(storeID, storeRoot);
		addCASStore(newStore);
		return storeID;
	}
	
	public synchronized CASStoreID initMapDBFSCASStore(File dbDirPath, File storeRoot)
	throws IOException
	{
		CASStoreID storeID = (CASStoreID) oidFactory.getNewOID(CASStore.class);
		CASStore newStore = new MapDBFSCASStore(storeID, dbDirPath, storeRoot);
		addCASStore(newStore);
		return storeID;
	}
	
	public synchronized CASStoreID initS3CASStore(String awsLogin, String awsPassword, String region) throws IOException
	{
		CASStoreID storeID = (CASStoreID) oidFactory.getNewOID(CASStore.class);
		File s3PropertyStorage = new File(storeManagerPropertiesDirectory, "S3InitProperties");
		if (!s3PropertyStorage.exists())
			s3PropertyStorage.mkdir();
		File initPropertiesFile = new File(s3PropertyStorage, storeID.toString()+".properties");
		CASStore newStore = new S3CASStore(awsLogin, awsPassword, storeID, region, initPropertiesFile);
		addCASStore(newStore);
		return storeID;
	}
	
	public synchronized void addCASStore(CASStore addStore) throws IOException
	{
		CASStoreID storeID = addStore.getCASStoreID();
		stores.put(storeID, addStore);
		int lastStoreNumber = -1;
		for (Object curKeyObj:storeManagerProperties.keySet())
		{
			if (curKeyObj instanceof String)
			{
				String curKey = (String)curKeyObj;
				if (curKey.startsWith(kStoreInfoPrefix))
				{
					String storeNumStr = curKey.substring(kStoreInfoPrefix.length() + 1);
					int storeNum = Integer.parseInt(storeNumStr);
					if (storeNum > lastStoreNumber)
						lastStoreNumber = storeNum;
				}
			}
		}
		lastStoreNumber++;
		String curStoreKey = kStoreInfoPrefix + "." + Integer.toString(lastStoreNumber);
		String storeInfo = null;
		if (addStore instanceof DBFSCASStore)
		{
			storeInfo = addStore.getCASStoreID().toString()+":"+kDBFSCASStoreType+":"+((DBFSCASStore)addStore).getStoreRoot().getAbsolutePath();
		}
		if (addStore instanceof JDBMFSCASStore)
		{
			storeInfo = addStore.getCASStoreID().toString()+":"+kJDBMFSCASStoreType+":"+((JDBMFSCASStore)addStore).getStoreRoot().getAbsolutePath();
			
		}
		if (addStore instanceof MapDBFSCASStore)
		{
			storeInfo = addStore.getCASStoreID().toString()+":"+kMapDBFSCASStoreType+":"+((MapDBFSCASStore)addStore).getStoreRoot().getAbsolutePath();
		}
		if (addStore instanceof S3CASStore)
		{
			storeInfo = addStore.getCASStoreID().toString()+":"+kS3CASStoreType+":"+((S3CASStore)addStore).getInitPropertiesFile().getAbsolutePath();
		}
		storeManagerProperties.put(curStoreKey, storeInfo);
		rewriteStoreManagerProperties();
	}

	protected void rewriteStoreManagerProperties() throws IOException,
			FileNotFoundException
	{
		String prevFileName = storeManagerPropertiesFile.getName()+".prev";
		File prevFile = new File(storeManagerPropertiesFile.getParentFile(), prevFileName);
		if (prevFile.exists())
		{
			if (!prevFile.delete())
				throw new IOException("Could not delete previous storage manager info file "+prevFile.getAbsolutePath());
		}
		storeManagerPropertiesFile.renameTo(prevFile);
		FileOutputStream storeManagerOutStream = new FileOutputStream(storeManagerPropertiesFile);
		storeManagerProperties.store(storeManagerOutStream, "Store Manager Properties saved at"+(new Date()).toString());
		storeManagerOutStream.close();
	}
	
	public synchronized CASStore getCASStore(CASStoreID storeID)
	{
		return stores.get(storeID);
	}
	
	public synchronized CASStore getCASStoreForWriting(long bytesToWrite) throws IOException
	{
		long spaceNeeded = bytesToWrite + kFreeSpaceMargin;
		if (writeStore != null)
		{
			if (writeStore.getStatus() == CASStoreStatus.kReady &&  writeStore.getFreeSpace() > spaceNeeded)
				return writeStore;
		}
		for (CASStoreID curID:stores.keySet())
		{
			CASStore checkStore = stores.get(curID);
			long freeSpace = checkStore.getFreeSpace();
			if (checkStore.getStatus() == CASStoreStatus.kReady && freeSpace > spaceNeeded)
			{
				writeStore = checkStore;
				return writeStore;
			}
		}
		throw new DestinationFullException();
	}
	public static final String kInitDBFSStorePrefix = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.init";
	public static final String kInitJDBMFSStorePrefix = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.jdbmfs.init";
	public static final String kInitMapDBFSStorePrefix = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.mapdbfs.init";
	public static final String kInitS3StorePrefix = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3.init";
	public static final String kDBURLName = "dbURL";
	public static final String kDBUserName = "dbUser";
	public static final String kDBPasswordName = "dbPassword";
	public static final String kStorePathName = "storePath";
	public static final String kDBPathName = "dbPath";
	public static final String kAWSLoginName = "awsLogin";
	public static final String kAWSPasswordName = "awsPassword";
	public static final String kAWSRegionName = "awsRegion";
	// com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.init.0.dbURL=<dbURL>
	// com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.init.0.dbUser=<dbUser>
	// com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.init.0.dbPassword=<dbPassword>
	// com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.init.0.storePath=<storePath>
	public void initStores() throws IOException
	{
		File initDBFSStoresPropertiesFile = new File(storeManagerPropertiesDirectory, kInitDBFSStorePropertiesFileName);
		if (initDBFSStoresPropertiesFile.exists())
		{
			Properties initDBFSStoresProperties = new Properties();
			FileInputStream initDBFSStoresIS = new FileInputStream(initDBFSStoresPropertiesFile);
			initDBFSStoresProperties.load(initDBFSStoresIS);
			initDBFSStoresIS.close();
			
			createStoresForProperties(initDBFSStoresProperties);
			initDBFSStoresPropertiesFile.delete();
		}
	}

	public synchronized void createStoresForProperties(Properties initDBFSStoresProperties)
			throws IOException
	{
		HashSet<String> processedNums = new HashSet<String>();
		for (Object curKeyObj:initDBFSStoresProperties.keySet())
		{
			if (curKeyObj instanceof String)
			{
				String curKey = (String)curKeyObj;
				if (curKey.startsWith(kInitDBFSStorePrefix))
				{
					int lastDotPos = curKey.indexOf('.', kInitDBFSStorePrefix.length() + 1);
					if (lastDotPos > 0)
					{
						String storeNumStr = curKey.substring(kInitDBFSStorePrefix.length() + 1, lastDotPos);
						if (!processedNums.contains(storeNumStr))
						{
							processedNums.add(storeNumStr);
							String dbURL = initDBFSStoresProperties.getProperty(kInitDBFSStorePrefix+"."+storeNumStr+"."+kDBURLName);
							String dbUser = initDBFSStoresProperties.getProperty(kInitDBFSStorePrefix+"."+storeNumStr+"."+kDBUserName);
							String dbPassword = initDBFSStoresProperties.getProperty(kInitDBFSStorePrefix+"."+storeNumStr+"."+kDBPasswordName);
							String dbStorePath = initDBFSStoresProperties.getProperty(kInitDBFSStorePrefix+"."+storeNumStr+"."+kStorePathName);
							
							File storeRoot = new File(dbStorePath);
							initDBFSCASStore(storeRoot, dbURL, dbUser, dbPassword);
						}
					}
				}
				if (curKey.startsWith(kInitJDBMFSStorePrefix))
				{
					int lastDotPos = curKey.indexOf('.', kInitJDBMFSStorePrefix.length() + 1);
					if (lastDotPos > 0)
					{
						String storeNumStr = curKey.substring(kInitJDBMFSStorePrefix.length() + 1, lastDotPos);
						if (!processedNums.contains(storeNumStr))
						{
							processedNums.add(storeNumStr);
							String dbStorePath = initDBFSStoresProperties.getProperty(kInitJDBMFSStorePrefix+"."+storeNumStr+"."+kStorePathName);
							File storeRoot = new File(dbStorePath);
							initJDBMFSCASStore(storeRoot);
						}
					}
				}
				if (curKey.startsWith(kInitMapDBFSStorePrefix))
				{
					int lastDotPos = curKey.indexOf('.', kInitMapDBFSStorePrefix.length() + 1);
					if (lastDotPos > 0)
					{
						String storeNumStr = curKey.substring(kInitMapDBFSStorePrefix.length() + 1, lastDotPos);
						if (!processedNums.contains(storeNumStr))
						{
							processedNums.add(storeNumStr);
							String dbStorePath = initDBFSStoresProperties.getProperty(kInitMapDBFSStorePrefix+"."+storeNumStr+"."+kStorePathName);
							String dbDirPath = initDBFSStoresProperties.getProperty(kInitMapDBFSStorePrefix+"."+storeNumStr+"."+kDBPathName, dbStorePath);

							File storeRoot = new File(dbStorePath);
							File dbDir = new File(dbDirPath);
							initMapDBFSCASStore(dbDir, storeRoot);
						}
					}
				}
				
				if (curKey.startsWith(kInitS3StorePrefix))
				{
					int lastDotPos = curKey.indexOf('.', kInitS3StorePrefix.length() + 1);
					if (lastDotPos > 0)
					{
						String storeNumStr = curKey.substring(kInitS3StorePrefix.length() + 1, lastDotPos);
						if (!processedNums.contains(storeNumStr))
						{
							processedNums.add(storeNumStr);
							String awsLogin = initDBFSStoresProperties.getProperty(kInitS3StorePrefix+"."+storeNumStr+"."+kAWSLoginName);
							String awsPassword = initDBFSStoresProperties.getProperty(kInitS3StorePrefix+"."+storeNumStr+"."+kAWSPasswordName);
							String awsRegion =  initDBFSStoresProperties.getProperty(kInitS3StorePrefix+"."+storeNumStr+"."+kAWSRegionName);
							initS3CASStore(awsLogin, awsPassword, awsRegion);
						}
					}
				}
			}
		}
	}

	public synchronized CASStoreID[] listStoreIDs()
	{
		Set<CASStoreID>storeIDs = stores.keySet();
		CASStoreID [] returnIDs = new CASStoreID[storeIDs.size()];
		returnIDs = storeIDs.toArray(returnIDs);
		return returnIDs;
	}
}
