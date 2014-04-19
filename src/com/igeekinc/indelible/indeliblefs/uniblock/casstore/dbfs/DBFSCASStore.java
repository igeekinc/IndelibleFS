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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.AbstractCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreStatus;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.ClientFile;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.objectcache.LRUQueue;
import com.igeekinc.util.pauseabort.AbortedException;
import com.igeekinc.util.pauseabort.PauseAbort;

/**
 * The DBFSCASStore implements a segment store within a single directory with a single database connection.
 * In the storeRoot, the StoreConfiguration.properties is store along with the root of the CAS storage tree.
 *
 */
public class DBFSCASStore extends AbstractCASStore implements CASStore
{
	private CASStoreID storeID;
	private File storeRoot;
	private File propertiesFile;
    private File casStorageRoot;
    private long nextDataID;
    FSCASSerialStorage casStorage;
    
    Properties storeProperties;
    
    Connection dbConnection;
    Thread preenThread;
    PauseAbort preenPauser;
    
    public static final int kDigitsPerDirLevel = 3;    // Just fix it at this for ease of use
    public static final int kFileNameRadix = 10;
    public static final int kFilesPerDirLevel = (int)Math.pow(kFileNameRadix, kDigitsPerDirLevel);
    public static final String kStorePropertiesFileName = "StoreConfiguration.properties";
    public static final String kCASStoreDirectoryName = "CASFiles";
    public static final String kStoreIDPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.storeid";
    public static final String kStoreDBURLPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.dburl";
    public static final String kStoreDBUserPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.dbuser";
    public static final String kStoreDBPasswordPropertyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs.dbpassword";
    
    private PreparedStatement retrieveSegmentStmt, insertSegmentStmt, deleteSegmentStmt, retrieveByIDStmt;
    
    private LRUQueue<CASIdentifier, CASIDDataDescriptor>recentCache = new LRUQueue<CASIdentifier, CASIDDataDescriptor>(256);
	public DBFSCASStore(File storeRoot)
	throws IOException
	{
		super(null);
		this.storeRoot = storeRoot;
		casStorageRoot = new File(storeRoot, kCASStoreDirectoryName);
		initInternal(storeRoot);
		preenPauser = new PauseAbort(logger);
		preenThread = new Thread(new Runnable(){

			@Override
			public void run()
			{
				preenLoop();
			}
			
		}, "DBFSCASStore preener");
		preenThread.setDaemon(true);
		preenThread.start();
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
	public DBFSCASStore(CASStoreID storeID, File storeRoot, String dbURL, String dbUser, String dbPassword) throws IOException
	{
		super(null);
		if (storeID == null)
			throw new IllegalArgumentException("storeID cannot be null");
		if (storeRoot == null)
			throw new IllegalArgumentException("storeRoot cannot be null");
		if (dbURL == null || dbUser == null || dbPassword == null)
			throw new IllegalArgumentException("dbURL, dbUser and dbPassword cannot be null");	
		this.storeID = storeID;
		this.storeRoot = storeRoot;
		propertiesFile = new File(storeRoot, kStorePropertiesFileName);
		casStorageRoot = new File(storeRoot, kCASStoreDirectoryName);
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
		try
		{
			dbConnection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
			initDB();
		} catch (SQLException e)
		{
			logger.error(new ErrorLogMessage("Caught SQL exception initializing db with {0}, {1}, {2}", dbURL, dbUser, dbPassword), e);
			throw new IOException("Caught SQL Exception initializing database connection");
		}
		finally
		{
			try
			{
				if (dbConnection != null)
					dbConnection.close();  // We'll let initInternal reopen this
			} catch (SQLException e)
			{
				logger.error(new ErrorLogMessage("Caught SQL exception initializing db with {0}, {1}, {2}", dbURL, dbUser, dbPassword), e);
				throw new IOException("Caught SQL Exception initializing database connection");
			}
		}
		if (!casStorageRoot.mkdir())
			throw new IOException("Could not create casStorageRoot "+casStorageRoot.getAbsolutePath());
		
		Properties writeProperties = new Properties();
		writeProperties.setProperty(kStoreIDPropertyName, storeID.toString());
		writeProperties.setProperty(kStoreDBURLPropertyName, dbURL);
		writeProperties.setProperty(kStoreDBUserPropertyName, dbUser);
		writeProperties.setProperty(kStoreDBPasswordPropertyName, dbPassword);
		FileOutputStream propertiesOutStream = new FileOutputStream(propertiesFile);
		writeProperties.store(propertiesOutStream, storeRoot.getAbsolutePath()+" initialized at "+(new Date()).toString());
		propertiesOutStream.close();
		initInternal(storeRoot);
		setStatus(CASStoreStatus.kReady);
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
		String dbURL = storeProperties.getProperty(kStoreDBURLPropertyName);
		String dbUser = storeProperties.getProperty(kStoreDBUserPropertyName);
		String dbPassword = storeProperties.getProperty(kStoreDBPasswordPropertyName);
		
		if (dbURL == null || dbUser == null || dbPassword == null)
			throw new IllegalArgumentException("database not configured in properties file "+propertiesFile.getAbsolutePath()+" - initialize the directory before using");
		try
		{
			dbConnection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
			dbConnection.setAutoCommit(false);
		} catch (SQLException e)
		{
			logger.error(new ErrorLogMessage("Caught SQL exception initializing db with {0}, {1}, {2}", dbURL, dbUser, dbPassword), e);
			throw new IOException("Caught SQL Exception initializing database connection");
		}
		
		casStorage = new FSCASSerialStorage(casStorageRoot);
		
		try
		{
			Statement maxIDStmt = dbConnection.createStatement();
			ResultSet maxIDRS = maxIDStmt.executeQuery("select max(id), count(id) from data");
			if (maxIDRS.next() && maxIDRS.getInt(2) > 0)
			{
				nextDataID = maxIDRS.getLong(1) + 1;
			}
			else
			{
				nextDataID = -1;
			}
			maxIDRS.close();
			retrieveSegmentStmt = dbConnection.prepareStatement("select id from data where data.casid=?");
			insertSegmentStmt = dbConnection.prepareStatement("insert into data (id, casid, length) values(?, ?, ?)");
            String driverName = dbConnection.getMetaData().getDriverName();
            deleteSegmentStmt = dbConnection.prepareStatement("delete from data where casid=?");
            retrieveByIDStmt = dbConnection.prepareStatement("select * from data where id=?");
            /*getNumSegmentsStmt = dbConnection.prepareStatement("select count(*) from data");
            getSpaceUsedStmt = dbConnection.prepareStatement("select sum(length) from data");*/
            
            //speedTest();
		} catch (SQLException e)
		{
			logger.error(new ErrorLogMessage("Caught SQL exception initializing db prepared statements"), e);
			throw new IOException("Caught SQL Exception initializing database connection");
		}
	}

	protected void speedTest() throws SQLException, IOException
	{
		Statement maxIDStmt = dbConnection.createStatement();
		ResultSet maxIDRS;
		long startTime = System.currentTimeMillis();
		for (int testNum = 0; testNum < 10000; testNum++)
		{
			long dataID = ++nextDataID;
			insertSegmentStmt.setLong(1, dataID);
			insertSegmentStmt.setString(2, "SPEEDTEST"+dataID);
			insertSegmentStmt.setLong(3, -1);
			//Insert it into the DB and get the dataID that was assigned
			insertSegmentStmt.execute();
		}
		long endTime = System.currentTimeMillis();
		long elapsed = endTime - startTime;
		logger.error("DBFSCASStore start up insert test - 10000 database records inserted in "+elapsed+" "+(10000.0/((double)elapsed/1000.0))+" records/sec");
		
		Statement cleanStatement = dbConnection.createStatement();
		cleanStatement.execute("delete from data where casid like 'SPEEDTEST%'");
		cleanStatement.close();
		maxIDRS = maxIDStmt.executeQuery("select max(id), count(id) from data");
		if (maxIDRS.next() && maxIDRS.getInt(2) > 0)
		{
			nextDataID = maxIDRS.getLong(1) + 1;
		}
		else
		{
			nextDataID = -1;
		}
		maxIDStmt.close();
		
		byte [] data = new byte[16*1024];
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
		endTime = System.currentTimeMillis();
		elapsed = endTime - startTime;
		logger.error("DBFSCASStore start up insert test - 10000 16K segments inserted in "+elapsed+" "+(10000.0/((double)elapsed/1000.0))+" records/sec");
	}
	
	private void initDB() throws SQLException
	{
		String driverName = dbConnection.getMetaData().getDriverName();
        Statement initStmt = dbConnection.createStatement();
        if (driverName.startsWith("PostgreSQL"))
        {
            initStmt.execute("create table data (id bigint, casid char(136) primary key, length bigint)");
        }
        if (driverName.equals("SQLiteJDBC"))
        {
            initStmt.execute("create table data (id bigint, casid char(136) primary key, length bigint)");
        }
        if (driverName.startsWith("H2"))
        {
            initStmt.execute("create table data (id bigint, casid char(136) primary key, length bigint)");
        }
        initStmt.execute("create index casidindex on data(casid)");
	}
	@Override
	public CASStoreID getCASStoreID()
	{
		return storeID;
	}

	@Override
	public synchronized CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID)
			throws IOException
	{
        CASIDDataDescriptor returnDescriptor = recentCache.get(segmentID);
        if (returnDescriptor == null)
        {
        	String casIDStr = segmentID.toString();

        	try
        	{
        		retrieveSegmentStmt.setString(1, casIDStr);
        		ResultSet retrieveResults = retrieveSegmentStmt.executeQuery();
        		if (retrieveResults.next())
        		{
        			int dataID = retrieveResults.getInt("id");
        			returnDescriptor = casStorage.retrieveSegment(segmentID, dataID);
        			//returnDescriptor = connection.getDataMoverSession().registerDataDescriptor(returnDescriptor);
        		}
        		retrieveResults.close();
        		recentCache.put(segmentID, returnDescriptor);
        	}
        	catch (SQLException e)
        	{
        		logger.error(new ErrorLogMessage("Caught SQL exception while retrieving"), e);
        		throw new IOException("SQL Exception while retrieving");
        	}
        }
        return returnDescriptor;
	}

	@Override
	public synchronized void storeSegment(CASIDDataDescriptor segmentDescriptor)
	throws IOException
	{
		String casIDStr = segmentDescriptor.getCASIdentifier().toString();
		try
		{
			long dataID = ++nextDataID;
			try
			{
				insertSegmentStmt.setLong(1, dataID);
				insertSegmentStmt.setString(2, casIDStr);
				insertSegmentStmt.setLong(3, segmentDescriptor.getLength());
				insertSegmentStmt.execute();
				casStorage.storeDataSegment(segmentDescriptor, dataID);
				dbConnection.commit();
				recentCache.put(segmentDescriptor.getCASIdentifier(), segmentDescriptor);
			} catch (SQLException e)
			{
				dbConnection.rollback();
				// Could be that it's already in the table
				Statement retrieveStmt = dbConnection.createStatement();
				ResultSet retrieveResults = retrieveStmt.executeQuery("select * from data where casid='"+casIDStr+"'");
				if (retrieveResults.next())
				{
					dataID = retrieveResults.getLong("id");
				}
				else
					throw e;	// Must not have just been an already present error
			}
			return;
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Got SQL Exception inserting segment "+casIDStr), e);
			throw new IOException("Could not insert segment "+casIDStr, e);
		}
	}
    
	@Override
	public synchronized boolean deleteSegment(CASIdentifier releaseID) throws IOException
	{
    	String releaseIDStr = releaseID.toString();
        try
        {
            retrieveSegmentStmt.setString(1, releaseIDStr);
            ResultSet retrieveResults = retrieveSegmentStmt.executeQuery();
            if (retrieveResults.next())
            {
            	int dataID = retrieveResults.getInt("id");
            	deleteSegmentStmt.setString(1, releaseIDStr);
            	deleteSegmentStmt.execute();

            	casStorage.removeSegment(dataID);
            	dbConnection.commit();
            	recentCache.remove(releaseID);
            	return true;
            }
        }
        catch (SQLException e)
        {
        	try
			{
				dbConnection.rollback();
			} catch (SQLException e1)
			{
				logger.error(new ErrorLogMessage("SQLException during rollback"), e);
			}
            logger.error(new ErrorLogMessage("SQLException while retrieving"), e);
            throw new IOException("SQL Exception while retrieving");
        }
		return false;
	}

	@Override
	public long getNumSegments()
	{
		// TODO Auto-generated method stub
		return 0;
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
		try
		{
			ClientFile rootDir = SystemInfo.getSystemInfo().getClientFileForFile(storeRoot);
			return rootDir.getVolume().freeSpace();
		} catch (IOException e)
		{
			logger.error(new ErrorLogMessage("Caught IOException while executing getTotalSpace"), e);
		}
		return 0;
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
					if (checkFile.exists())
					{
						byte [] checkData = new byte[(int)checkFile.length()];
						FileInputStream checkStream = new FileInputStream(checkFile);
						checkStream.read(checkData);
						checkStream.close();
						SHA1HashID checkHash = new SHA1HashID(checkData);
						CASIdentifier checkSegmentID = new CASIdentifier(checkHash, checkData.length);
						synchronized(this)
						{
							String casIDStr = checkSegmentID.toString();
							retrieveSegmentStmt.setString(1, casIDStr);
							ResultSet retrieveResults = retrieveSegmentStmt.executeQuery();
							if (!retrieveResults.next())
							{
								// Figure out if it's actually bad here
								logger.warn(checkID+" was orphaned or bad, removing");
								casStorage.removeSegment(checkID);
							}
							else
							{
								long dbDataID = retrieveResults.getLong("id");
								if (checkID != dbDataID)
								{
									logger.warn(checkID+" was orphaned, removing");
									casStorage.removeSegment(checkID);
								}
							}
							retrieveResults.close();
						}
					}
					else
					{
						synchronized(this)
						{
							retrieveByIDStmt.setLong(1, checkID);
							ResultSet retrieveResults = retrieveByIDStmt.executeQuery();
							if (retrieveResults.next())
							{
								logger.warn(checkID+" was in the index but missing from disk - removing from DB");
								retrieveResults.close();
								Statement removeStmt = dbConnection.createStatement();
								removeStmt.execute("delete from data where id ="+checkID);
								dbConnection.commit();
								removeStmt.close();
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
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (SQLException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (AbortedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
			try
			{
				Thread.sleep(7*24*60*60*1000);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
	}

	@Override
	public void sync() throws IOException
	{
		
	}

	@Override
	public void rebuild() throws IOException
	{
		setStatus(CASStoreStatus.kRebuilding);
		logger.error(new ErrorLogMessage("Starting rebuild on store {0}, path={1}", getCASStoreID(), casStorageRoot));
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
				try
				{
					insertSegmentStmt.setLong(1, checkID);
					insertSegmentStmt.setString(2, checkSegmentID.toString());
					insertSegmentStmt.setLong(3, checkData.length);
					insertSegmentStmt.execute();
				} catch (SQLException e)
				{
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					setStatus(CASStoreStatus.kNeedsRebuild);	// Still broken
					throw new IOException("Could not rebuild, got SQL exception");
				}
			}
		}
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
	public void shutdown() throws IOException
	{
		try
		{
			dbConnection.close();
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IOException("Got SQLExcaption on close");
		}
	}

	@Override
	public boolean verifySegment(CASIdentifier identifierToVerify)
	{
		// TODO - make this work
		return true;
	}	
}
