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
 
package com.igeekinc.indelible.indeliblefs.firehose;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.indelible.indeliblefs.CreateDirectoryInfo;
import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.CreateSymlinkInfo;
import com.igeekinc.indelible.indeliblefs.DeleteFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSClientPreferences;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSServer;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileLike;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleNodeInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.MoveObjectInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSession;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.ListVersionsReply;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteInputStream;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteOutputStream;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.util.IndelibleFSDataPipeline;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.junitext.iGeekTestCase;
import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.ClientFile;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.datadescriptor.FileDataDescriptor;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.perf.MBPerSecondLog4jStopWatch;

class FileAppenderRunnable implements Runnable
{
	IndelibleFSForkIF appendFork;
	LinkedBlockingQueue<CASIDMemoryDataDescriptor>appendQueue;
	boolean finished;
	
	public FileAppenderRunnable(IndelibleFSForkIF appendFork, LinkedBlockingQueue<CASIDMemoryDataDescriptor>appendQueue)
	{
		this.appendFork = appendFork;
		this.appendQueue = appendQueue;
	}
	
	public void setFinished()
	{
		finished = true;
	}
	@Override
	public void run()
	{
		while (!finished || appendQueue.size() > 0)
		{
			try
			{
		        StopWatch writeTotalWatch = new Log4JStopWatch("appendQueuePoll");
				CASIDMemoryDataDescriptor appendDescriptor = appendQueue.poll(10, TimeUnit.MILLISECONDS);
				writeTotalWatch.stop();
				if (appendDescriptor != null)
				{
					StopWatch appendWatch = new Log4JStopWatch("append");
					appendFork.appendDataDescriptor(appendDescriptor);
					appendWatch.stop();
				}
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (RemoteException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
	}
	
}

class AppendFileAsyncCompletion implements AsyncCompletion<Void, Void>
{
	int numCompleted = 0;
	int numFailed = 0;
	@Override
	public void completed(Void result, Void attachment)
	{
		numCompleted++;
	}

	@Override
	public void failed(Throwable exc, Void attachment)
	{
		numFailed++;
		Logger.getLogger(getClass()).error("Append failed ", exc);
	}

	public int getNumCompleted()
	{
		return numCompleted;
	}

	public int getNumFailed()
	{
		return numFailed;
	}
	
}

class FileAppenderRunnableAsync implements Runnable
{
	IndelibleFSForkIF appendFork;
	LinkedBlockingQueue<CASIDMemoryDataDescriptor>appendQueue;
	boolean finished;
	
	public FileAppenderRunnableAsync(IndelibleFSForkIF appendFork, LinkedBlockingQueue<CASIDMemoryDataDescriptor>appendQueue)
	{
		this.appendFork = appendFork;
		this.appendQueue = appendQueue;
	}
	
	public void setFinished()
	{
		finished = true;
	}
	@Override
	public void run()
	{
		AppendFileAsyncCompletion completion = new AppendFileAsyncCompletion();
		int numToComplete = 0;
		long offset = 0;
		while (!finished || appendQueue.size() > 0)
		{
			try
			{
		        StopWatch writeTotalWatch = new Log4JStopWatch("appendQueuePoll");
				CASIDMemoryDataDescriptor appendDescriptor = appendQueue.poll(10, TimeUnit.MILLISECONDS);
				writeTotalWatch.stop();
				if (appendDescriptor != null)
				{
					StopWatch appendWatch = new Log4JStopWatch("append");
					//appendFork.appendDataDescriptorAsync(appendDescriptor, completion, null);
					// Async append is not reliable currently.  We'll just manage our own offsets
					appendFork.writeDataDescriptorAsync(offset, appendDescriptor, completion, null);
					offset += appendDescriptor.getLength();
					appendWatch.stop();
					numToComplete++;
				}
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (RemoteException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		long startWaitTime = System.currentTimeMillis();
		while((completion.getNumCompleted() + completion.getNumFailed() < numToComplete) &&
				(System.currentTimeMillis() - startWaitTime < 60*1000))
		{
			try
			{
				Thread.sleep(1000);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
	}
	
}

class ReadDescriptorInfo
{
	CASIDDataDescriptor descriptor;
	long offset;
	
	public ReadDescriptorInfo(CASIDDataDescriptor descriptor, long offset)
	{
		this.descriptor = descriptor;
		this.offset = offset;
	}
}

class DescriptorReaderRunnable implements Runnable
{
	LinkedBlockingQueue<ReadDescriptorInfo>readQueue;
	private byte [] destinationBuffer;
	private byte [] checkBuffer;
	boolean finished;
	
	public DescriptorReaderRunnable(LinkedBlockingQueue<ReadDescriptorInfo>readQueue, byte [] destinationBuffer, byte [] checkBuffer)
	{
		this.readQueue = readQueue;
		this.destinationBuffer = destinationBuffer;
		this.checkBuffer = checkBuffer;
	}
	
	public void setFinished()
	{
		finished = true;
	}
	@Override
	public void run()
	{
		int offset = 0;
		MBPerSecondLog4jStopWatch readTotalWatch = new MBPerSecondLog4jStopWatch("backgroundReadTotal");
		while (!finished || readQueue.size() > 0)
		{
			try
			{
		        StopWatch readQueuePollWatch = new Log4JStopWatch("readQueuePoll");
		        ReadDescriptorInfo readDescriptorInfo = readQueue.poll(10, TimeUnit.MILLISECONDS);
				readQueuePollWatch.stop();
				if (readDescriptorInfo != null)
				{
			        CASIDDataDescriptor readDescriptor = readDescriptorInfo.descriptor;

					StopWatch appendWatch = new Log4JStopWatch("getData");
					junit.framework.TestCase.assertEquals((long)offset, readDescriptorInfo.offset);
					long bytesRead = readDescriptor.getData(destinationBuffer, offset, 0, (int)readDescriptor.getLength(), true);
					/*ByteBuffer verifyBuffer = ByteBuffer.wrap(destinationBuffer, offset, (int)readDescriptor.getLength()).slice();
					SHA1HashID checkID = new SHA1HashID(verifyBuffer);
					if (!readDescriptor.getCASIdentifier().getHashID().equals(checkID))
					{
						System.err.println("Bad data at offset "+offset);
					}*/
					if (checkBuffer != null && destinationBuffer[offset] != checkBuffer[offset])
					{
						System.out.println("destinationBuffer @ "+offset+" != checkBuffer @ "+offset);
						for (int checkOffset = 0; checkOffset < checkBuffer.length; checkOffset += readDescriptor.getLength())
						{
							if (destinationBuffer[offset] == checkBuffer[checkOffset])
							{
								System.out.println("Possible match at "+checkOffset);
								boolean strongMatch = true;
								for (int strongCheckOffset = 0; strongCheckOffset < readDescriptor.getLength(); strongCheckOffset ++)
								{
									if (destinationBuffer[offset + strongCheckOffset] != checkBuffer[checkOffset + strongCheckOffset])
									{
										strongMatch = false;
										break;
									}
								}
								if (strongMatch)
								{
									System.out.println("Strong match at offset "+offset+", checkOffset "+checkOffset);
								}
							}
						}
					}
					junit.framework.TestCase.assertEquals(bytesRead, readDescriptor.getLength());
					offset += bytesRead;
					readTotalWatch.bytesProcessed(readDescriptor.getLength());
					appendWatch.stop();
				}
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (RemoteException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		readTotalWatch.stop();
	}
}
public class IndelibleFSServerTest extends iGeekTestCase
{
	private static final int	kSegmentIDTestFileLength	= 1024*1024*16;
	private static final String	kSnapTestDirName	= "snaptest";
	private static final String	kSingleTestFileName	= "singleTestFile";
	private static final String	kDirOneName	= "dir one";
	private static final String	kSnapshotTestProperty	= "testInfo";
	private static final int kSmallDirTreeFileSize = 64*1024;
	private static IndelibleFSServer fsServer;
    private static IndelibleFSVolumeIF testVolume;
	private static String	testVolumeName;
    private static IndelibleDirectoryNodeIF root;
    private static IndelibleServerConnectionIF connection;
    private static DataMoverSession moverSession;
    private static boolean dataMoverInitialized = false;
    private Logger logger;
    
	public Level getLoggingLevel()
	{
		return Level.WARN;
	}
    
    public void setUp()
    throws Exception
    {
    	if (root == null)
    	{
    		IndelibleFSClientPreferences.initPreferences(null);

    		//DataMoverSource.noServer = true;
    		MonitoredProperties serverProperties = IndelibleFSClientPreferences.getProperties();
    		PropertyConfigurator.configure(serverProperties);
    		logger = Logger.getLogger(getClass());
    		File preferencesDir = new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName));
    		File securityClientKeystoreFile = new File(preferencesDir, IndelibleFSClient.kIndelibleEntityAuthenticationClientConfigFileName);
    		EntityAuthenticationClient.initializeEntityAuthenticationClient(securityClientKeystoreFile, null, serverProperties);
    		EntityAuthenticationClient.startSearchForServers();
    		EntityAuthenticationServer [] securityServers = new EntityAuthenticationServer[0];
    		while(securityServers.length == 0)
    		{
    			securityServers = EntityAuthenticationClient.listEntityAuthenticationServers();
    			if (securityServers.length == 0)
    				Thread.sleep(1000);
    		}

    		EntityAuthenticationServer securityServer = securityServers[0];

    		EntityAuthenticationClient.getEntityAuthenticationClient().trustServer(securityServer);
    		IndelibleFSClient.start(null, serverProperties);
    		IndelibleFSServer[] servers = new IndelibleFSServer[0];

    		while(servers.length == 0)
    		{
    			servers = IndelibleFSClient.listServers();
    			if (servers.length == 0)
    				Thread.sleep(1000);
    		}
    		fsServer = servers[0];

    		if (!dataMoverInitialized)
    		{
    			GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
    			GeneratorID testBaseID = genIDFactory.createGeneratorID();
    			ObjectIDFactory oidFactory = new ObjectIDFactory(testBaseID);
    			File localSocketFile = new File("/tmp/indelible-fs-test-dm");
    			AFUNIXSocketAddress localSocketAddress = new AFUNIXSocketAddress(localSocketFile);
    			if (localSocketFile.exists())
    			{
    				try
    				{
    					AFUNIXSocket checkSocket = AFUNIXSocket.connectTo(localSocketAddress);
    					logger.fatal("Data mover socket "+localSocketFile+" active, another Executor must be running - exiting");
    					System.exit(0);
    				}
    				catch (IOException e)
    				{
    					// Closed, let's nuke it
    					localSocketFile.delete();
    				}
    			}
    			DataMoverReceiver.init(oidFactory);
    			DataMoverSource.init(oidFactory, new InetSocketAddress(0), /*localSocketAddress*/null);
    			dataMoverInitialized = true;
    		}
    		EntityID serverID = fsServer.getServerID();
    		logger.warn("Connected to server "+serverID);
    		connection = fsServer.open();
    		connection.startTransaction();
    		Properties volumeProperties = new Properties();
    		Date now = new Date();
            testVolumeName = "Test Volume "+now;
			volumeProperties.put(IndelibleFSVolumeIF.kVolumeNamePropertyName, testVolumeName);
    		testVolume = connection.createVolume(volumeProperties);
    		connection.commit();
    		EntityAuthentication serverAuthentication = connection.getServerEntityAuthentication();
    		moverSession = DataMoverSource.getDataMoverSource().createDataMoverSession(securityServer.getEntityID());

    		/*
    		 * Authorize the server to read data from us
    		 */
    		 SessionAuthentication sessionAuthentication = moverSession.addAuthorizedClient(serverAuthentication);
    		 connection.addClientSessionAuthentication(sessionAuthentication);

    		 /*
    		  * Now, get the authentication that allows us to read data from the server
    		  */
    		 SessionAuthentication remoteAuthentication = connection.getSessionAuthentication();
    		 DataMoverReceiver.getDataMoverReceiver().addSessionAuthentication(remoteAuthentication);

    		 root = testVolume.getRoot();
    	}
    }
    
    @Override
	protected void tearDown() throws Exception
	{
    	try
    	{
    		if (testVolume != null)
    		{
    			connection.deleteVolume(testVolume.getVolumeID());
    		}
    	}
    	finally
    	{
    		testVolume = null;
    		root = null;
    	}
    	super.tearDown();
	}

    public void testCreateSingleFile() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = kSingleTestFileName;
    		CreateFileInfo testInfo = root.createChildFile(name, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    		SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024);
    		forkOutputStream.close();
    		connection.commit();
    		String forkName = testDataFork.getName();
    		assertEquals("data", forkName);
    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024, hashID));
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    
    public void testCreateSingleFileReadOneByteAtATime() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = "singleTestFileReadOneByte";
    		CreateFileInfo testInfo = root.createChildFile(name, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    		SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024);
    		forkOutputStream.close();
    		connection.commit();
    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    		int curByte;
    		while ((curByte = forkInputStream.read()) >= 0)
    		{
    			baos.write(curByte);
    		}
    		baos.close();
    		forkInputStream.close();
    		byte [] readBytes = baos.toByteArray();
    		ByteArrayInputStream bais = new ByteArrayInputStream(readBytes);
    		assertTrue(TestFilesTool.verifyInputStream(bais, 1024, hashID));
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    
    private static final int kIFSDTestFileSize = (16 * 1024 * 1024) + 786;
    private  static int kSegmentChunk = 1024*1024;
    public void testCreateSingleFileWithIndelibleFSDataPipeline() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = kSingleTestFileName+"-withifsdp";
    		ClientFile localFile = SystemInfo.getSystemInfo().getClientFileForFile(File.createTempFile("ifsdp", "dat"));
    		SHA1HashID hashID = TestFilesTool.createTestFile(localFile, kIFSDTestFileSize);

    		
			DataDescriptor sourceDescriptor = new FileDataDescriptor(localFile);
			CASIDDataDescriptor initDescriptor = new CASIDMemoryDataDescriptor(sourceDescriptor, 0, kSegmentChunk);
			sourceDescriptor.close();

			HashMap<String, CASIDDataDescriptor>initialForkData = new HashMap<String, CASIDDataDescriptor>();
			initialForkData.put("data", initDescriptor);
			
    		CreateFileInfo testInfo = root.createChildFile(name, initialForkData, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		long curForkLength = localFile.length();
			sourceDescriptor = new FileDataDescriptor(localFile);

			IndelibleFSForkIF dataFork = curTestFile.getFork("data", false);
			IndelibleFSDataPipeline writePipeline = new IndelibleFSDataPipeline(dataFork);
			for (long offset = kSegmentChunk; offset < curForkLength; offset+= kSegmentChunk)
			{
				int bytesToSend = kSegmentChunk;
				if (offset + kSegmentChunk > curForkLength)
					bytesToSend = (int)(curForkLength - offset);
				CASIDDataDescriptor curChunkDescriptor = new CASIDMemoryDataDescriptor(sourceDescriptor, offset, bytesToSend);
				//dataFork.appendDataDescriptor(curChunkDescriptor);
				writePipeline.appendData(sourceDescriptor, offset, bytesToSend);
			}
			try
			{
				writePipeline.waitForPipelineCompletion(1, TimeUnit.MINUTES);
			} catch (InterruptedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e); //$NON-NLS-1$
			}
			
    		connection.commit();

    		CASIdentifier [] segmentIDs = testDataFork.getSegmentIDs();
    		long segmentIDTotalLength = 0;
    		FileInputStream localInputStream = new FileInputStream(localFile);
    		CASIdentifier [] localSegmentIDs = new CASIdentifier[segmentIDs.length];
    		SHA1HashID verifyHashID = new SHA1HashID();
    		
    		System.out.println("Stored segments:");
    		for (int curSegmentIDNum = 0; curSegmentIDNum < segmentIDs.length; curSegmentIDNum++)
    		{
    			System.out.println(curSegmentIDNum+" : "+segmentIDs[curSegmentIDNum]);

    		}
    		System.out.println("Local segments");
    		for (int curSegmentIDNum = 0; curSegmentIDNum < segmentIDs.length; curSegmentIDNum++)
    		{
    			CASIdentifier curSegmentID = segmentIDs[curSegmentIDNum];
    			byte [] localBuf = new byte[(int)curSegmentID.getSegmentLength()];
    			int bytesRead = localInputStream.read(localBuf);
    			SHA1HashID localHash = new SHA1HashID(localBuf);
    			CASIdentifier localIdentifier = new CASIdentifier(localHash, bytesRead);
    			localSegmentIDs[curSegmentIDNum] = localIdentifier;
    			System.out.println(curSegmentIDNum+" : "+localIdentifier);
    		}
    		
    		localInputStream.close();
    		
    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);

    		for (int curSegmentIDNum = 0; curSegmentIDNum < segmentIDs.length; curSegmentIDNum++)
    		{
    			CASIdentifier curSegmentID = segmentIDs[curSegmentIDNum];
    			byte [] curBuf = new byte[(int)curSegmentID.getSegmentLength()];
    			int bytesRead = forkInputStream.read(curBuf);
    			assertEquals(bytesRead, curBuf.length);
    			SHA1HashID checkHash = new SHA1HashID(curBuf);
    			CASIdentifier checkIdentifier = new CASIdentifier(checkHash, curBuf.length);
    			assertEquals(curSegmentID, checkIdentifier);

    			segmentIDTotalLength += curSegmentID.getSegmentLength();
    			verifyHashID.update(curBuf);
    			assertEquals(curSegmentID, localSegmentIDs[curSegmentIDNum]);
    		}
    		forkInputStream.close();
    		
    		forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, kIFSDTestFileSize, hashID));
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    
    public void testGetSegmentIDs() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = "segmentIDTestFile";
    		CreateFileInfo testInfo = root.createChildFile(name, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    		SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, kSegmentIDTestFileLength);
    		forkOutputStream.close();
    		connection.commit();
    		CASIdentifier [] segmentIDs = testDataFork.getSegmentIDs();
    		long segmentIDTotalLength = 0;
    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		SHA1HashID verifyHashID = new SHA1HashID();
    		for (int curSegmentIDNum = 0; curSegmentIDNum < segmentIDs.length; curSegmentIDNum++)
    		{
    			CASIdentifier curSegmentID = segmentIDs[curSegmentIDNum];
    			byte [] curBuf = new byte[(int)curSegmentID.getSegmentLength()];
    			int bytesRead = forkInputStream.read(curBuf);
    			assertEquals(bytesRead, curBuf.length);
    			SHA1HashID checkHash = new SHA1HashID(curBuf);
    			CASIdentifier checkIdentifier = new CASIdentifier(checkHash, curBuf.length);
    			assertEquals(curSegmentID, checkIdentifier);
    			segmentIDTotalLength += curSegmentID.getSegmentLength();
    			verifyHashID.update(curBuf);
    		}
    		assertEquals(kSegmentIDTestFileLength, segmentIDTotalLength);
    		verifyHashID.finalizeHash();
    		assertEquals(hashID, verifyHashID);
    		forkInputStream.close();
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    public void testWriteMultipleSingleBlock() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = kSingleTestFileName+"-sb";
    		CreateFileInfo testInfo = root.createChildFile(name, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    		byte [] testData = TestFilesTool.generateTestPatternBlock(0, 1024, 0, 0);
    		forkOutputStream.write(testData, 0, 40);
    		forkOutputStream.write(testData, 40, testData.length - 40);
    		forkOutputStream.close();
    		connection.commit();
    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		byte [] checkData = new byte[1024];
    		assertEquals(1024, forkInputStream.read(checkData));
    		assertTrue(Arrays.equals(checkData, testData));
    		assertTrue(TestFilesTool.verifyTestPatternBlock(0, 0, 0, checkData));
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    
    public void testCreateAndRewriteSingleFile() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = "rewriteTestFile";
    		CreateFileInfo testInfo = root.createChildFile(name, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    		SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024);
    		forkOutputStream.close();
    		connection.commit();

    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024, hashID));
    		forkInputStream.close();
    		connection.startTransaction();
    		curTestFile = root.getChildNode(name);
    		testDataFork = curTestFile.getFork("data", false);
    		forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    		hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024*16);
    		forkOutputStream.close();
    		connection.commit();

    		forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024*16, hashID));
    		forkInputStream.close();
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    
    public void testCreateAndRewriteSingleFileViaCreate() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = "rewriteTestFileViaCreate";
    		CASIDDataDescriptor oneK = writeTestDataToDataDescriptor(1024);
    		HashMap<String, CASIDDataDescriptor>data = new HashMap<String, CASIDDataDescriptor>();
    		data.put("data", oneK);
    		CreateFileInfo testInfo = root.createChildFile(name, data, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", false);
    		connection.commit();

    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024, oneK.getCASIdentifier().getHashID()));
    		forkInputStream.close();
    		connection.startTransaction();
    		CASIDDataDescriptor sixteenK = writeTestDataToDataDescriptor(1024*16);
    		data = new HashMap<String, CASIDDataDescriptor>();
    		data.put("data", sixteenK);
    		testInfo = root.createChildFile(name, data, false);
    		curTestFile = testInfo.getCreatedNode();
    		testDataFork = curTestFile.getFork("data", false);
    		connection.commit();

    		forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024*16, sixteenK.getCASIdentifier().getHashID()));
    		forkInputStream.close();
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    public void testCreateAndWriteToFile() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		String name = "singleTestWrite";
    		CreateFileInfo testInfo = root.createChildFile(name, true);
    		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    		root = testInfo.getDirectoryNode();
    		long lastModified = curTestFile.lastModified();
    		assertTrue(lastModified > 0);
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    		SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, 1024);
    		byte [] data = baos.toByteArray();
    		CASIDMemoryDataDescriptor dataDescriptor = new CASIDMemoryDataDescriptor(data);
    		testDataFork.writeDataDescriptor(0, dataDescriptor);
    		connection.commit();
    		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024, hashID));

    		byte [] zeros = new byte[1024];
    		CASIDMemoryDataDescriptor zeroDescriptor = new CASIDMemoryDataDescriptor(zeros);
    		connection.startTransaction();
    		testDataFork.writeDataDescriptor(0, zeroDescriptor);
    		connection.commit();
    		CASIDDataDescriptor checkZeroDD = testDataFork.getDataDescriptor(0, 1024);
    		byte [] checkZeros = checkZeroDD.getData();
    		assertTrue(Arrays.equals(zeros, checkZeros));
    		long nextModified = curTestFile.lastModified();
    		assertTrue(nextModified > lastModified);
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }
    
    public void testCreateDirTreeSingleTransaction() throws Exception
    {
    	connection.startTransaction();
    	try
    	{
    		IndelibleDirectoryNodeIF dirRoot = createSmallDirTree();
    		connection.commit();
    		checkSmallDirTree(dirRoot);
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }

	private IndelibleDirectoryNodeIF createSmallDirTree() throws IOException,
			PermissionDeniedException, RemoteException, FileExistsException,
			ForkNotFoundException, ObjectNotFoundException {
		Date now = new Date();
    	FilePath testDirPath = FilePath.getFilePath("/testCreateDirTreeSingleTrans "+now);
    	CreateDirectoryInfo testDirInfo = root.createChildDirectory(testDirPath.getName());
    	root = testDirInfo.getDirectoryNode();

        for (int dirNum =0 ;dirNum < 3; dirNum++)
        {
        	IndelibleDirectoryNodeIF testDir = (IndelibleDirectoryNodeIF) testVolume.getObjectByPath(testDirPath);
        	FilePath curDirPath = testDirPath.getChild("test"+dirNum);
        	CreateDirectoryInfo curDirInfo = testDir.createChildDirectory(curDirPath.getName());
        	IndelibleDirectoryNodeIF curDir = (IndelibleDirectoryNodeIF) testVolume.getObjectByPath(curDirPath);
        	FilePath subDirPath = curDirPath.getChild("sub-"+dirNum);
        	CreateDirectoryInfo subDirInfo = curDir.createChildDirectory(subDirPath.getName());
        	IndelibleDirectoryNodeIF subDir = (IndelibleDirectoryNodeIF) testVolume.getObjectByPath(subDirPath);
        	String name = "testfile-"+dirNum;
        	/*
            CreateFileInfoRemote testInfo = subDir.createChildFile(name);
            IndelibleFileNodeRemote curTestFile = testInfo.getCreateNode();
            IndelibleFSForkRemote testDataFork = curTestFile.getFork("data", true);
            IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
            SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, kSmallDirTreeFileSize);
            
            forkOutputStream.close();
            */
        	ByteArrayOutputStream baos = new ByteArrayOutputStream(kSmallDirTreeFileSize);
        	SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, kSmallDirTreeFileSize);
        	baos.close();
        	CASIDMemoryDataDescriptor dataDescriptor = new CASIDMemoryDataDescriptor(baos.toByteArray());
        	HashMap<String, CASIDDataDescriptor>initialForkData = new HashMap<String, CASIDDataDescriptor>();
        	initialForkData.put("data", dataDescriptor);
        	CreateFileInfo testInfo = subDir.createChildFile(name, initialForkData, true);
        }
        return (IndelibleDirectoryNodeIF) testVolume.getObjectByPath(testDirPath);
	}
	
	private void checkSmallDirTree(IndelibleDirectoryNodeIF dirRoot) throws IOException, PermissionDeniedException, ObjectNotFoundException
	{
		for (int dirNum =0 ;dirNum < 3; dirNum++)
        {
			IndelibleDirectoryNodeIF curDirNode = (IndelibleDirectoryNodeIF)dirRoot.getChildNode("test"+dirNum);
			IndelibleDirectoryNodeIF subDir = (IndelibleDirectoryNodeIF) curDirNode.getChildNode("sub-"+dirNum);
			IndelibleFileNodeIF curFile = subDir.getChildNode("testfile-"+dirNum);
			assertEquals(kSmallDirTreeFileSize, curFile.totalLength());
        }
	}
    public void testCreateChildDirectory()
    throws Exception
    {
    	Date now = new Date();
    	CreateDirectoryInfo testDirInfo = root.createChildDirectory("testCreateChildDirectory1 "+now);
    	IndelibleDirectoryNodeIF testDir = testDirInfo.getCreatedNode();
        for (int dirNum =0 ;dirNum < 10; dirNum++)
        {
        	testDir.createChildDirectory("test"+dirNum);
        }
        String [] children = testDir.list();
        assertEquals(children.length, 10);
        for (int curChildNum = 0 ;curChildNum < children.length; curChildNum++)
        {
            System.out.println(children[curChildNum]);
        }
        IndelibleNodeInfo [] childNodeInfo = testDir.getChildNodeInfo(new String []{IndelibleFileLike.kClientFileMetaDataPropertyName});
        assertEquals(childNodeInfo.length, 10);
        for (int curChildNum = 0 ;curChildNum < children.length; curChildNum++)
        {
            System.out.println(childNodeInfo[curChildNum]);
        }
    }
 
    public static final int kListDirectoryRuns = 1000;
    public void testListDirectoryPerf()
    throws Exception
    {
    	Date now = new Date();
    	CreateDirectoryInfo testDirInfo = root.createChildDirectory("testCreateChildDirectory2 "+now);
    	IndelibleDirectoryNodeIF testDir = testDirInfo.getCreatedNode();
        for (int dirNum =0 ;dirNum < 10; dirNum++)
        {
        	testDir.createChildDirectory("test"+dirNum);
        }
        Log4JStopWatch listWatch = new Log4JStopWatch("List total");
        for (int runNum = 0; runNum < kListDirectoryRuns; runNum++)
        {
        	Log4JStopWatch curWatch = new Log4JStopWatch("listDirectory");
        	String [] children = testDir.list();
        	assertEquals(children.length, 10);
        	curWatch.stop();
        }
        listWatch.stop();
        System.out.println("Listed directory "+kListDirectoryRuns+" times in "+listWatch.getElapsedTime()+"ms");
    }
    
    public void testCreateChildFile()
    throws Exception
    {
        for (int fileNum = 0; fileNum < 10; fileNum++)
        {
            String name = "testfile-"+fileNum;
            CreateFileInfo testInfo = root.createChildFile(name, true);
            IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
            root = testInfo.getDirectoryNode();
            IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
            IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
            SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
            forkOutputStream.close();
            IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
            assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        }
    }

    public void testCreateFileSpeed()
    throws Exception
    {
        long writeStartMillis = System.currentTimeMillis();
        connection.startTransaction();
        CreateDirectoryInfo newInfo = root.createChildDirectory("speedTestDir");
        IndelibleDirectoryNodeIF speedTestDir = newInfo.getCreatedNode();
        for (int fileNum = 0; fileNum < 1000; fileNum++)
        {
            String name = "testfile-"+fileNum;
            CreateFileInfo curInfo = speedTestDir.createChildFile(name, true);
            IndelibleFileNodeIF curTestFile = curInfo.getCreatedNode();
            speedTestDir = curInfo.getDirectoryNode();
            IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
            IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
            SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024);
            forkOutputStream.close();
        }
        connection.commit();
        long writeEndMillis = System.currentTimeMillis();
        long elapsedWriteMillis = writeEndMillis - writeStartMillis;
        System.out.println("testCreateFileSpeed - Created 1000 files (1K) in "+elapsedWriteMillis+" ms, speed = "+(1000.0/((double)elapsedWriteMillis/1000.0))+" files/s");
    }

    public void testNewCreateFileSpeed()
    throws Exception
    {
    	long writeStartMillis = System.currentTimeMillis();
    	connection.startTransaction();
    	try
    	{
    		CreateDirectoryInfo dirInfo = root.createChildDirectory("speedTestDir1");
    		IndelibleDirectoryNodeIF speedTestDir = dirInfo.getCreatedNode();
    		//Thread.sleep(1000);
    		for (int fileNum = 0; fileNum < 1000; fileNum++)
    		{
    			String name = "testfile-"+fileNum;
    			//System.out.print(name+"\r"); System.out.flush();
    			ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    			SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, 1024);
    			baos.close();
    			CASIDDataDescriptor childData = new CASIDMemoryDataDescriptor(baos.toByteArray());
    			childData = moverSession.registerDataDescriptor(childData);
    			HashMap<String, CASIDDataDescriptor>forkData = new HashMap<String, CASIDDataDescriptor>();
    			forkData.put("data", childData);
    			CreateFileInfo curTestInfo = speedTestDir.createChildFile(name, forkData, true);
    			moverSession.removeDataDescriptor(childData);
    			IndelibleFileNodeIF curTestfile = curTestInfo.getCreatedNode();
    			speedTestDir = curTestInfo.getDirectoryNode();
    		}
    		connection.commit();
    		long writeEndMillis = System.currentTimeMillis();
    		long elapsedWriteMillis = writeEndMillis - writeStartMillis;
    		System.out.println("testNewCreateFileSpeed - Created 1000 files (1K) in "+elapsedWriteMillis+" ms, speed = "+(1000.0/(((double)elapsedWriteMillis)/1000.0))+" files/s");
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }

    class CreateFileAsyncCompletion implements AsyncCompletion<CreateFileInfo, String>
    {
    	int numCompleted = 0;
    	int numFailed = 0;
		@Override
		public void completed(CreateFileInfo result, String attachment)
		{
			numCompleted++;
		}

		@Override
		public void failed(Throwable exc, String attachment)
		{
			numFailed++;
			Logger.getLogger(getClass()).error("Create of "+attachment+" failed ", exc);
		}

		public int getNumCompleted()
		{
			return numCompleted;
		}

		public int getNumFailed()
		{
			return numFailed;
		}
    	
    }
    public void testNewCreateFileAsyncSpeed() throws Exception
    {
    	long writeStartMillis = System.currentTimeMillis();
    	connection.startTransaction();
    	try
    	{
    		CreateDirectoryInfo dirInfo = root.createChildDirectory("speedTestDir2");
    		IndelibleDirectoryNodeIF speedTestDir = dirInfo.getCreatedNode();
    		CreateFileAsyncCompletion completionHandler = new CreateFileAsyncCompletion();
    		//Thread.sleep(1000);
    		for (int fileNum = 0; fileNum < 1000; fileNum++)
    		{
    			String name = "testfile-"+fileNum;
    			//System.out.print(name+"\r"); System.out.flush();
    			ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    			SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, 1024);
    			baos.close();
    			CASIDDataDescriptor childData = new CASIDMemoryDataDescriptor(baos.toByteArray());
    			childData = moverSession.registerDataDescriptor(childData);
    			HashMap<String, CASIDDataDescriptor>forkData = new HashMap<String, CASIDDataDescriptor>();
    			forkData.put("data", childData);
    			speedTestDir.createChildFileAsync(name, forkData, true, completionHandler, name);
    			moverSession.removeDataDescriptor(childData);
    			/*IndelibleFileNodeIF curTestfile = curTestInfo.getCreatedNode();
    			speedTestDir = curTestInfo.getDirectoryNode();*/
    		}
    		
    		// This should happen AFTER the commit, but we're a little broken at the moment (commit does not wait for everything
    		// ahead of it to finish) so we do it instead.  TODO - fix this!
    		long timeWaited = 0;
    		while (timeWaited < 10000 && (completionHandler.getNumCompleted() + completionHandler.getNumFailed() != 1000))
    		{
    			long timeStarted = System.currentTimeMillis();
    			Thread.sleep(100);	// Give them a chance to finish
    			long timeElapsed = System.currentTimeMillis() - timeStarted;
    			timeWaited += timeElapsed;
    		}
    		
    		connection.commit();

    		
    		assertEquals(1000, completionHandler.getNumCompleted());
    		assertEquals(0, completionHandler.getNumFailed());	// If we get here and numFailed > 0 then the previous test failed somehow
    		long writeEndMillis = System.currentTimeMillis();
    		long elapsedWriteMillis = writeEndMillis - writeStartMillis;
    		System.out.println("testNewCreateFileAsyncSpeed - Created 1000 files (1K) in "+elapsedWriteMillis+" ms, speed = "+(1000.0/(((double)elapsedWriteMillis)/1000.0))+" files/s");
    	}
    	finally
    	{
    		if (connection.inTransaction())
    			connection.rollback();
    	}
    }

    public static final int meg100FileBlocks = 100;
    public static final int gig1FileBlocks = 1000;
    public static final int megBlockSize = 1024*1024;
    public void testReadWriteSpeedViaIS()
    throws Exception
    {
    	connection.startTransaction();
        CreateFileInfo testInfo = root.createChildFile("speedTest", true);
        IndelibleFileNodeIF speedTestFile = testInfo.getCreatedNode();
        
        IndelibleFSObjectID objectID = speedTestFile.getObjectID();
        IndelibleFSForkIF speedTestFork = speedTestFile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(speedTestFork, false, moverSession);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, meg100FileBlocks*megBlockSize);
        byte [] testData = baos.toByteArray();
        long writeStartMillis = System.currentTimeMillis();
        for (int curBlockNum = 0; curBlockNum < meg100FileBlocks; curBlockNum++)
        {
        	forkOutputStream.write(testData, curBlockNum * megBlockSize, megBlockSize);
        }
        forkOutputStream.close();
        connection.commit();
        long writeEndMillis = System.currentTimeMillis();
        long elapsedWriteMillis = writeEndMillis - writeStartMillis;
        System.out.println("Wrote 100MB in "+elapsedWriteMillis+" ms, speed = "+(100.0/((double)elapsedWriteMillis/1000.0))+" MB/s");
        
        IndelibleFileNodeIF readbackFile = root.getChildNode("speedTest");
        assertNotNull(readbackFile);
        assertEquals(objectID, readbackFile.getObjectID());
        
        IndelibleFSForkIF readbackFork = readbackFile.getFork("data", false);
        assertNotNull(readbackFork);
        assertEquals(100*1024*1024, readbackFile.totalLength());
        long readStartMillis = System.currentTimeMillis();
        IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(readbackFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 100*1024*1024, hashID));
        long readEndMillis = System.currentTimeMillis();
        long elapsedReadMillis = readEndMillis - readStartMillis;
        System.out.println("Read 100MB in "+elapsedReadMillis+" ms, speed = "+(100.0/((double)elapsedReadMillis/1000.0))+" MB/s");
        forkInputStream.close();
        forkInputStream = new IndelibleFSForkRemoteInputStream(readbackFork);
        readStartMillis = System.currentTimeMillis();
        forkInputStream.read(new byte[1024*1024]);
        readEndMillis = System.currentTimeMillis();
        elapsedReadMillis = readEndMillis - readStartMillis;
        System.out.println("Read 1 MB in "+elapsedReadMillis+" ms, speed = "+(1.0/((double)elapsedReadMillis/1000.0))+" MB/s");
    }

    public void testReadWriteSpeedViaDescriptors()
    throws Exception
    {
    	connection.startTransaction();
        CreateFileInfo testInfo = root.createChildFile("speedTest-IS", true);
        IndelibleFileNodeIF speedTestFile = testInfo.getCreatedNode();
        
        IndelibleFSObjectID objectID = speedTestFile.getObjectID();
        IndelibleFSForkIF speedTestFork = speedTestFile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(speedTestFork, false, moverSession);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, meg100FileBlocks*megBlockSize);
        byte [] testData = baos.toByteArray();
        LinkedBlockingQueue<CASIDMemoryDataDescriptor>appendQueue = new LinkedBlockingQueue<CASIDMemoryDataDescriptor>();
        CASIDMemoryDataDescriptor.setMaxGeneratorThreads(Runtime.getRuntime().availableProcessors() - 1);
        FileAppenderRunnable runnable = new FileAppenderRunnable(speedTestFork, appendQueue);
        Thread appendThread = new Thread(runnable, "Append thread");
        appendThread.start();
        MBPerSecondLog4jStopWatch writeTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.writeTotal");
        for (int curBlockNum = 0; curBlockNum < meg100FileBlocks; curBlockNum++)
        {
        	StopWatch appendQueueWatch = new Log4JStopWatch("appendQueue");
        	CASIDMemoryDataDescriptor appendDescriptor = new CASIDMemoryDataDescriptor(testData, curBlockNum * megBlockSize, megBlockSize);
        	appendQueue.add(appendDescriptor);
        	appendQueueWatch.stop();
        }
        runnable.setFinished();
        appendThread.join();
        forkOutputStream.close();
        StopWatch commitWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.commit");
        connection.commit();
        commitWatch.stop();
        writeTotalWatch.bytesProcessed(meg100FileBlocks*megBlockSize);
        writeTotalWatch.stop();
        System.out.println(MessageFormat.format("Wrote 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", writeTotalWatch.getElapsedTime(), writeTotalWatch.getMegaBytesPerSecond()));
        

        IndelibleFileNodeIF readbackFile = root.getChildNode("speedTest-IS");
        assertNotNull(readbackFile);
        assertEquals(objectID, readbackFile.getObjectID());
        
        IndelibleFSForkIF readbackFork = readbackFile.getFork("data", false);
        assertNotNull(readbackFork);
        assertEquals(meg100FileBlocks*megBlockSize, readbackFile.totalLength());
        byte [] readBuffer = new byte[meg100FileBlocks*megBlockSize];
        LinkedBlockingQueue<ReadDescriptorInfo>readQueue = new LinkedBlockingQueue<ReadDescriptorInfo>(16);
        DescriptorReaderRunnable readRunnable = new DescriptorReaderRunnable(readQueue, readBuffer, testData);
        Thread readThread = new Thread(readRunnable, "Read thread");
        readThread.start();
        MBPerSecondLog4jStopWatch readTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.readTotal");
        for (int block = 0; block < meg100FileBlocks; block++)
        {
        	Log4JStopWatch getDataDescriptorWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.getDataDescriptor");
        	int offset = block*megBlockSize;
			CASIDDataDescriptor readDescriptor = readbackFork.getDataDescriptor(offset, megBlockSize);
        	getDataDescriptorWatch.stop();
        	Log4JStopWatch putWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.put");
        	ReadDescriptorInfo readDescriptorInfo = new ReadDescriptorInfo(readDescriptor, offset);
        	readQueue.put(readDescriptorInfo);
        	putWatch.stop();
        }
        readRunnable.setFinished();
        readThread.join();
        readTotalWatch.bytesProcessed(readBuffer.length);
        readTotalWatch.stop();
        System.out.println(MessageFormat.format("Read 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", readTotalWatch.getElapsedTime(), readTotalWatch.getMegaBytesPerSecond()));
        MBPerSecondLog4jStopWatch verifyTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.verifyTotal");
        assertTrue(TestFilesTool.verifyInputStream(new ByteArrayInputStream(readBuffer), readBuffer.length, hashID));
        verifyTotalWatch.bytesProcessed(readBuffer.length);
        verifyTotalWatch.stop();
        System.out.println(MessageFormat.format("Verify 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", verifyTotalWatch.getElapsedTime(), verifyTotalWatch.getMegaBytesPerSecond()));
        
    }
    
    public void testReadWriteSpeedViaDescriptorsAsync()
    	    throws Exception
    	    {
    	    	connection.startTransaction();
    	        String testFileName = "speedTest-IS-async";
    	        
				CreateFileInfo testInfo = root.createChildFile(testFileName, true);
    	        IndelibleFileNodeIF speedTestFile = testInfo.getCreatedNode();
    	        
    	        IndelibleFSObjectID objectID = speedTestFile.getObjectID();
    	        IndelibleFSForkIF speedTestFork = speedTestFile.getFork("data", true);
    	        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(speedTestFork, false, moverSession);
    	        ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(baos, meg100FileBlocks*megBlockSize);
    	        byte [] testData = baos.toByteArray();
    	        LinkedBlockingQueue<CASIDMemoryDataDescriptor>appendQueue = new LinkedBlockingQueue<CASIDMemoryDataDescriptor>();
    	        CASIDMemoryDataDescriptor.setMaxGeneratorThreads(Runtime.getRuntime().availableProcessors() - 1);
    	        
    	        FileAppenderRunnableAsync runnable = new FileAppenderRunnableAsync(speedTestFork, appendQueue);
    	        
    	        Thread appendThread = new Thread(runnable, "Append thread");
    	        appendThread.start();
    	        MBPerSecondLog4jStopWatch writeTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.writeTotal");
    	        for (int curBlockNum = 0; curBlockNum < meg100FileBlocks; curBlockNum++)
    	        {
    	        	StopWatch appendQueueWatch = new Log4JStopWatch("appendQueue");
    	        	CASIDMemoryDataDescriptor appendDescriptor = new CASIDMemoryDataDescriptor(testData, curBlockNum * megBlockSize, megBlockSize);
    	        	appendQueue.add(appendDescriptor);
    	        	appendQueueWatch.stop();
    	        }
    	        runnable.setFinished();
    	        appendThread.join();
    	        forkOutputStream.close();
    	        StopWatch commitWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.commit");
    	        connection.commit();
    	        commitWatch.stop();
    	        writeTotalWatch.bytesProcessed(meg100FileBlocks*megBlockSize);
    	        writeTotalWatch.stop();
    	        System.out.println(MessageFormat.format("Wrote 100MB (async) in {0,number,#} ms, speed = {1,number,##} MB/s", writeTotalWatch.getElapsedTime(), writeTotalWatch.getMegaBytesPerSecond()));
    	        

    	        IndelibleFileNodeIF readbackFile = root.getChildNode(testFileName);
    	        assertNotNull(readbackFile);
    	        assertEquals(objectID, readbackFile.getObjectID());
    	        
    	        IndelibleFSForkIF readbackFork = readbackFile.getFork("data", false);
    	        assertNotNull(readbackFork);
    	        assertEquals(meg100FileBlocks*megBlockSize, readbackFile.totalLength());
    	        byte [] readBuffer = new byte[meg100FileBlocks*megBlockSize];
    	        LinkedBlockingQueue<ReadDescriptorInfo>readQueue = new LinkedBlockingQueue<ReadDescriptorInfo>(16);
    	        DescriptorReaderRunnable readRunnable = new DescriptorReaderRunnable(readQueue, readBuffer, testData);
    	        Thread readThread = new Thread(readRunnable, "Read thread");
    	        readThread.start();
    	        MBPerSecondLog4jStopWatch readTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.readTotal");
    	        for (int block = 0; block < meg100FileBlocks; block++)
    	        {
    	        	Log4JStopWatch getDataDescriptorWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.getDataDescriptor");
    	        	int offset = block*megBlockSize;
					CASIDDataDescriptor readDescriptor = readbackFork.getDataDescriptor(offset, megBlockSize);
    	        	getDataDescriptorWatch.stop();
    	        	Log4JStopWatch putWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.put");
    	        	ReadDescriptorInfo readDescriptorInfo = new ReadDescriptorInfo(readDescriptor, offset);
    	        	readQueue.put(readDescriptorInfo);
    	        	putWatch.stop();
    	        }
    	        readRunnable.setFinished();
    	        readThread.join();
    	        readTotalWatch.bytesProcessed(readBuffer.length);
    	        readTotalWatch.stop();
    	        System.out.println(MessageFormat.format("Read 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", readTotalWatch.getElapsedTime(), readTotalWatch.getMegaBytesPerSecond()));
    	        MBPerSecondLog4jStopWatch verifyTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.verifyTotal");
    	        for (int checkByteNum = 0; checkByteNum < readBuffer.length; checkByteNum++)
    	        {
    	        	if (readBuffer[checkByteNum] != testData[checkByteNum])
    	        		System.out.println("data differs at "+checkByteNum);
    	        }
    	        assertTrue(TestFilesTool.verifyInputStream(new ByteArrayInputStream(readBuffer), readBuffer.length, hashID));
    	        verifyTotalWatch.bytesProcessed(readBuffer.length);
    	        verifyTotalWatch.stop();
    	        System.out.println(MessageFormat.format("Verify 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", verifyTotalWatch.getElapsedTime(), verifyTotalWatch.getMegaBytesPerSecond()));
    	        
    	    }
    
    public void testReadWriteSpeedViaPipeline()
    	    throws Exception
    	    {
    	    	connection.startTransaction();
    	        String testFileName = "speedTest-P-async";
    	        
				CreateFileInfo testInfo = root.createChildFile(testFileName, true);
    	        IndelibleFileNodeIF speedTestFile = testInfo.getCreatedNode();
    	        
    	        IndelibleFSObjectID objectID = speedTestFile.getObjectID();
    	        IndelibleFSForkIF speedTestFork = speedTestFile.getFork("data", true);
    	        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(speedTestFork, false, moverSession);

    	        File tempFile = File.createTempFile("ifst-pipeline", "data");
    	        FileOutputStream tfos = new FileOutputStream(tempFile);
    	        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(tfos, meg100FileBlocks*megBlockSize);
    	        tfos.close();
    	        
    	        FileDataDescriptor allOfIt = new FileDataDescriptor(SystemInfo.getSystemInfo().getClientFileForFile(tempFile));
    	        CASIDMemoryDataDescriptor.setMaxGeneratorThreads(Runtime.getRuntime().availableProcessors() - 1);
    	        
    	        IndelibleFSDataPipeline pipeline = new IndelibleFSDataPipeline(speedTestFork);
    	        
    	        MBPerSecondLog4jStopWatch writeTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.writeTotal");
    	        for (int curBlockNum = 0; curBlockNum < meg100FileBlocks; curBlockNum++)
    	        {
    	        	StopWatch appendQueueWatch = new Log4JStopWatch("appendQueue");
    	        	pipeline.appendData(allOfIt, curBlockNum * megBlockSize, megBlockSize);
    	        	appendQueueWatch.stop();
    	        }
    	        pipeline.waitForPipelineCompletion(0, TimeUnit.MILLISECONDS);
    	        forkOutputStream.close();
    	        StopWatch commitWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.commit");
    	        connection.commit();
    	        commitWatch.stop();
    	        writeTotalWatch.bytesProcessed(meg100FileBlocks*megBlockSize);
    	        writeTotalWatch.stop();
    	        System.out.println(MessageFormat.format("Wrote 100MB (pipeline) in {0,number,#} ms, speed = {1,number,##} MB/s", writeTotalWatch.getElapsedTime(), writeTotalWatch.getMegaBytesPerSecond()));
    	        

    	        IndelibleFileNodeIF readbackFile = root.getChildNode(testFileName);
    	        assertNotNull(readbackFile);
    	        assertEquals(objectID, readbackFile.getObjectID());
    	        
    	        IndelibleFSForkIF readbackFork = readbackFile.getFork("data", false);
    	        assertNotNull(readbackFork);
    	        assertEquals(meg100FileBlocks*megBlockSize, readbackFile.totalLength());
    	        byte [] readBuffer = new byte[meg100FileBlocks*megBlockSize];
    	        LinkedBlockingQueue<ReadDescriptorInfo>readQueue = new LinkedBlockingQueue<ReadDescriptorInfo>(16);
    	        DescriptorReaderRunnable readRunnable = new DescriptorReaderRunnable(readQueue, readBuffer, null);
    	        Thread readThread = new Thread(readRunnable, "Read thread");
    	        readThread.start();
    	        MBPerSecondLog4jStopWatch readTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.readTotal");
    	        for (int block = 0; block < meg100FileBlocks; block++)
    	        {
    	        	Log4JStopWatch getDataDescriptorWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.getDataDescriptor");
    	        	int offset = block*megBlockSize;
					CASIDDataDescriptor readDescriptor = readbackFork.getDataDescriptor(offset, megBlockSize);
    	        	getDataDescriptorWatch.stop();
    	        	Log4JStopWatch putWatch = new Log4JStopWatch("testReadWriteSpeedViaDescriptors.put");
    	        	ReadDescriptorInfo readDescriptorInfo = new ReadDescriptorInfo(readDescriptor, offset);
    	        	readQueue.put(readDescriptorInfo);
    	        	putWatch.stop();
    	        }
    	        readRunnable.setFinished();
    	        readThread.join();
    	        readTotalWatch.bytesProcessed(readBuffer.length);
    	        readTotalWatch.stop();
    	        System.out.println(MessageFormat.format("Read 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", readTotalWatch.getElapsedTime(), readTotalWatch.getMegaBytesPerSecond()));
    	        MBPerSecondLog4jStopWatch verifyTotalWatch = new MBPerSecondLog4jStopWatch("testReadWriteSpeedViaDescriptors.verifyTotal");
    	        assertTrue(TestFilesTool.verifyInputStream(new ByteArrayInputStream(readBuffer), readBuffer.length, hashID));
    	        verifyTotalWatch.bytesProcessed(readBuffer.length);
    	        verifyTotalWatch.stop();
    	        System.out.println(MessageFormat.format("Verify 100MB in {0,number,#} ms, speed = {1,number,##} MB/s", verifyTotalWatch.getElapsedTime(), verifyTotalWatch.getMegaBytesPerSecond()));
    	        
    	    }

    public void testDuplicateFile()
    throws Exception
    {
        String sourceName="dupSrcFile";
        CreateFileInfo testInfo = root.createChildFile(sourceName, true);
        IndelibleFileNodeIF curTestfile = testInfo.getCreatedNode();
        root = testInfo.getDirectoryNode();
        IndelibleFSForkIF testDataFork = curTestfile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
        forkOutputStream.close();
        IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
        
        String duplicateName = "dupFile";
        CreateFileInfo dupTestInfo = root.createChildFile(duplicateName, curTestfile, true);
        IndelibleFileNodeIF dupTestfile = dupTestInfo.getCreatedNode();
        root = dupTestInfo.getDirectoryNode();
        IndelibleFSForkIF testDupFork = dupTestfile.getFork("data", false);
        forkInputStream = new IndelibleFSForkRemoteInputStream(testDupFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
    }
    
    public void testDeleteFile()
    throws Exception
    {
    	String sourceName="deleteFile";
    	CreateFileInfo testInfo = root.createChildFile(sourceName, true);
    	IndelibleFileNodeIF curTestfile = testInfo.getCreatedNode();
    	root = testInfo.getDirectoryNode();
    	IndelibleFSForkIF testDataFork = curTestfile.getFork("data", true);
    	IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    	SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
    	forkOutputStream.close();
    	IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    	assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
    	forkInputStream.close();

    	assertTrue(root.deleteChild(sourceName).deleteSucceeded());
    	IndelibleFileNodeIF deletedFile;

    	boolean deleted = false;
    	try
    	{
    		deletedFile = root.getChildNode(sourceName);
    	}
    	catch (ObjectNotFoundException e)
    	{
    		deleted = true;
    	}
    	assertTrue(deleted);

    	String dirName = "deleteDir";
    	CreateDirectoryInfo newDirInfo = root.createChildDirectory(dirName);

    	curTestfile = root.getChildNode(dirName);
    	assertNotNull(curTestfile);

    	assertTrue(root.deleteChildDirectory(dirName).deleteSucceeded());

    	deleted = false;
    	try
    	{
    		curTestfile = root.getChildNode(dirName);
    	}
    	catch (ObjectNotFoundException e)
    	{
    		deleted = true;
    	}
    	assertTrue(deleted);

    }
    
    public void testDeleteFileByPath()
    throws Exception
    {
    	String sourceName="deleteFile1";
    	CreateFileInfo testInfo = root.createChildFile(sourceName, true);
    	IndelibleFileNodeIF curTestfile = testInfo.getCreatedNode();
    	root = testInfo.getDirectoryNode();
    	IndelibleFSForkIF testDataFork = curTestfile.getFork("data", true);
    	IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    	SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
    	forkOutputStream.close();
    	IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    	assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
    	forkInputStream.close();
    	FilePath deletePath = FilePath.getFilePath("/"+sourceName);
    	DeleteFileInfo deleteInfo = testVolume.deleteObjectByPath(deletePath);
    	assertNotNull(deleteInfo);
    	assertTrue(deleteInfo.deleteSucceeded());
    	IndelibleFileNodeIF deletedFile;

    	boolean deleted = false;
    	try
    	{
    		deletedFile = root.getChildNode(sourceName);
    	}
    	catch (ObjectNotFoundException e)
    	{
    		deleted = true;
    	}
    	assertTrue(deleted);

    	String dirName = "deleteDir";
    	CreateDirectoryInfo newDirInfo = root.createChildDirectory(dirName);

    	curTestfile = root.getChildNode(dirName);
    	assertNotNull(curTestfile);

    	CreateFileInfo subDirFileInfo = ((IndelibleDirectoryNodeIF)curTestfile).createChildFile(sourceName, true);
    	assertNotNull(subDirFileInfo);
    	assertNotNull(subDirFileInfo.getCreatedNode());
    	FilePath deleteSubDirPath = FilePath.getFilePath("/"+dirName+"/"+sourceName);
    	
    	DeleteFileInfo deleteSubDirInfo = testVolume.deleteObjectByPath(deleteSubDirPath);
    	assertNotNull(deleteSubDirInfo);
    	assertTrue(deleteSubDirInfo.deleteSucceeded());

    	deleted = false;
    	try
    	{
    		curTestfile = testVolume.getObjectByPath(deleteSubDirPath);
    	}
    	catch (ObjectNotFoundException e)
    	{
    		deleted = true;
    	}
    	assertTrue(deleted);

    }
    private static final long kOneGBLength = 1024L*1024L*1024L;
	private static final long kOneTBLength = kOneGBLength * 1024;
    public static final long kOneHundredGBLength = 100L * kOneGBLength;
    
    public void testAllocateFile()
    throws Exception
    {
    	String oneGBName = "allocateFile1GB";
    	String oneHundredGBName = "allocateFile100GB";
    	String oneHundredGBDupName = "duplicateFile100GB";
    	String oneTBName= "allocateFile1TB";
    	String oneTBDupName = "duplicateFile1TB";
    	
    	CreateFileInfo oneGBInfo = root.createChildFile(oneGBName, true);
        IndelibleFileNodeIF oneGBFile = oneGBInfo.getCreatedNode();
        root = oneGBInfo.getDirectoryNode();
        long startOneGB = System.currentTimeMillis();
        IndelibleFSForkIF oneGBFork = oneGBFile.getFork("data", true);
        oneGBFork.extend(kOneGBLength);
        System.out.println("Created 1GB blank file in "+(System.currentTimeMillis() - startOneGB)+" ms");
        assertEquals(kOneGBLength, oneGBFile.totalLength());
        
        /*
    	CreateFileInfoRemote oneHundredGBInfo = root.createChildFile(oneHundredGBName);
        IndelibleFileNodeRemote oneHundredGBFile = oneHundredGBInfo.getCreateNode();
        root = oneHundredGBInfo.getDirectoryNode();
        long startOneHundredGB = System.currentTimeMillis();
        IndelibleFSForkRemote oneHundredGBFork = oneHundredGBFile.getFork("data", true);

		oneHundredGBFork.extend(kOneHundredGBLength);
        System.out.println("Created 100GB blank file in "+(System.currentTimeMillis() - startOneHundredGB)+" ms");
        assertEquals(kOneHundredGBLength, oneHundredGBFile.totalLength());
        
        long startDuplicate100GB = System.currentTimeMillis();
        CreateFileInfoRemote oneHundredGBDupInfo = root.createChildFile(oneHundredGBDupName, (IndelibleFSObjectID)oneHundredGBFile.getObjectID());
        System.out.println("Duplicated 100GB blank file in "+(System.currentTimeMillis() - startDuplicate100GB));
        
    	CreateFileInfoRemote oneTBInfo = root.createChildFile(oneTBName);
        IndelibleFileNodeRemote oneTBFile = oneTBInfo.getCreateNode();
        root = oneTBInfo.getDirectoryNode();
        long startOneTB = System.currentTimeMillis();
        IndelibleFSForkRemote oneTBFork = oneTBFile.getFork("data", true);
        oneTBFork.extend(kOneTBLength);
        System.out.println("Created 1TB blank file in "+(System.currentTimeMillis() - startOneTB)+" ms");
        assertTrue(oneTBFile.totalLength() == kOneTBLength);
        
        long startDuplicate1TB = System.currentTimeMillis();
        CreateFileInfoRemote oneTBDupInfo = root.createChildFile(oneTBDupName, (IndelibleFSObjectID)oneTBFile.getObjectID());
        System.out.println("Duplicated 1TB blank file in "+(System.currentTimeMillis() - startDuplicate1TB));
        */
    }
    
    public void testBasicSnapshots()
    throws Exception
    {
    	IndelibleVersion startVersion = testVolume.getRoot().getCurrentVersion();
    	HashMap<String, Serializable> metadata = new HashMap<String, Serializable>();
    	metadata.put(kSnapshotTestProperty, "1");
    	IndelibleSnapshotInfo startInfo = new IndelibleSnapshotInfo(startVersion, metadata);
		testVolume.addSnapshot(startInfo);
    	IndelibleSnapshotInfo checkInfo = testVolume.getInfoForSnapshot(startVersion);
    	assertEquals(startInfo.getVersion(), checkInfo.getVersion());
    	assertEquals(startInfo.getMetadataProperty(kSnapshotTestProperty), checkInfo.getMetadataProperty(kSnapshotTestProperty));
    	connection.startTransaction();
    	CreateDirectoryInfo newDir = root.createChildDirectory(kSnapTestDirName);
		root = newDir.getDirectoryNode();
    	IndelibleDirectoryNodeIF snaptestDir = newDir.getCreatedNode();
    	String name = kSingleTestFileName;
		CreateFileInfo testInfo = snaptestDir.createChildFile(name, true);
		IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
		IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
		SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024);
		forkOutputStream.close();
		metadata.put(kSnapshotTestProperty, "2");
		IndelibleVersion singleCreatedVersion = connection.commitAndSnapshot(metadata);
		assertFalse(startVersion.equals(singleCreatedVersion));
		IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
		assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 1024, hashID));
		
		connection.startTransaction();
		curTestFile = snaptestDir.getChildNode(name);
		IndelibleFSForkIF testDataFork1 = curTestFile.getFork("data", true);
		IndelibleFSForkRemoteOutputStream forkOutputStream1 = new IndelibleFSForkRemoteOutputStream(testDataFork1, false, moverSession);
		SHA1HashID hashID1 = TestFilesTool.writeTestDataToOutputStream(forkOutputStream1, 2000);
		forkOutputStream.close();
		metadata.put(kSnapshotTestProperty, "3");
		IndelibleVersion singleCreatedVersion2 = connection.commitAndSnapshot(metadata);
		
		assertFalse(singleCreatedVersion.equals(singleCreatedVersion2));
		IndelibleFSForkRemoteInputStream forkInputStream1 = new IndelibleFSForkRemoteInputStream(testDataFork1);
		assertTrue(TestFilesTool.verifyInputStream(forkInputStream1, 2000, hashID1));
		
		IndelibleFileNodeIF previousVersionTestFile = curTestFile.getVersion(singleCreatedVersion, RetrieveVersionFlags.kExact);
		assertNotNull(previousVersionTestFile);
		assertEquals(singleCreatedVersion, previousVersionTestFile.getCurrentVersion());
		
		IndelibleFSForkIF testDataFork2 = previousVersionTestFile.getFork("data", true);
		assertEquals(1024, testDataFork2.length());
		IndelibleFSForkRemoteInputStream forkInputStream2 = new IndelibleFSForkRemoteInputStream(testDataFork2);
		assertTrue(TestFilesTool.verifyInputStream(forkInputStream2, 1024, hashID));
		
		// We've verified basic data versioning.  Let's try adding and deleting some files now
		
		connection.startTransaction();
		snaptestDir.createChildDirectory(kDirOneName);
		IndelibleVersion dirOneCreated1 = connection.commit();
		
		String [] children1 = snaptestDir.list();
		Arrays.sort(children1);
		assertEquals(2, children1.length);
		assertEquals(kDirOneName, children1[0]);
		assertEquals(kSingleTestFileName, children1[1]);
		
		IndelibleDirectoryNodeIF singleFileSnapTestDir = snaptestDir.getVersion(singleCreatedVersion, RetrieveVersionFlags.kExact);
		assertNotNull(singleFileSnapTestDir);
		String [] singleChildren = singleFileSnapTestDir.list();
		assertEquals(1, singleChildren.length);
		assertEquals(kSingleTestFileName, singleChildren[0]);
		
		snaptestDir.deleteChildDirectory(kDirOneName);
		String [] dirOneDeletedChildren = snaptestDir.list();
		assertEquals(1, dirOneDeletedChildren.length);
		assertEquals(kSingleTestFileName, dirOneDeletedChildren[0]);
		
		IndelibleDirectoryNodeIF dirOneCreatedDir1 = snaptestDir.getVersion(dirOneCreated1, RetrieveVersionFlags.kExact);
		String [] dirOneCreatedChildren1 = dirOneCreatedDir1.list();
		assertEquals(2, dirOneCreatedChildren1.length);
		Arrays.sort(dirOneCreatedChildren1);
		assertEquals(kDirOneName, dirOneCreatedChildren1[0]);
		assertEquals(kSingleTestFileName, dirOneCreatedChildren1[1]);
    }
    
    public static CASIDDataDescriptor writeTestDataToDataDescriptor(int size) 
    {
        SHA1HashID returnID = new SHA1HashID();
        byte [] buffer = new byte[size];
        long bytesRemaining = size;
        Random randomBytes = new Random(size);
        randomBytes.nextBytes(buffer);
        CASIDDataDescriptor returnDescriptor = new CASIDMemoryDataDescriptor(buffer);
        return returnDescriptor;
    }
    
    public void testMoveFile()
    throws Exception
    {
        String sourceName="moveSrcFile";
        CreateFileInfo testInfo = root.createChildFile(sourceName, true);
        IndelibleFileNodeIF curTestfile = testInfo.getCreatedNode();
        root = testInfo.getDirectoryNode();
        IndelibleFSForkIF testDataFork = curTestfile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
        forkOutputStream.close();
        IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
        
        String duplicateName = "moveDestFile";
        FilePath sourcePath = FilePath.getFilePath("/"+sourceName);
        FilePath destinationPath = FilePath.getFilePath("/"+duplicateName);
        MoveObjectInfo moveInfo = testVolume.moveObject(sourcePath, destinationPath);
        

        
        root = moveInfo.getDestInfo().getDirectoryNode();
        boolean sourceExists = true;
        try
        {
        	root.getChildNode(sourceName);
        }
        catch (ObjectNotFoundException e)
        {
        	sourceExists = false;
        }
        assertFalse(sourceExists);
        IndelibleFileNodeIF dupTestfile = root.getChildNode(duplicateName);
        IndelibleFSForkIF testDupFork = dupTestfile.getFork("data", false);
        forkInputStream = new IndelibleFSForkRemoteInputStream(testDupFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
    }
    
    public void testObjectByPathSpeed()
    throws Exception
    {
    	Date now = new Date();
    	FilePath retrievePath = FilePath.getFilePath("/");
    	String testDirName = "testPathDir "+now;
		CreateDirectoryInfo testDirInfo = root.createChildDirectory(testDirName);
		retrievePath = retrievePath.getChild(testDirName);
    	IndelibleDirectoryNodeIF testDir = testDirInfo.getCreatedNode();
    	IndelibleDirectoryNodeIF curDir = testDir;
        for (int dirNum =0 ;dirNum < 10; dirNum++)
        {
        	String curDirName = "test"+dirNum;
        	retrievePath = retrievePath.getChild(curDirName);
			CreateDirectoryInfo curDirInfo = curDir.createChildDirectory(curDirName);
        	curDir = curDirInfo.getCreatedNode();
        }
        System.out.println("Starting retrieve path");
        long retrieveStartTime = System.currentTimeMillis();
        for (int repeat = 0; repeat < 1000; repeat++)
        {
        	testVolume.getObjectByPath(retrievePath);
        }
        long retrieveEndTime = System.currentTimeMillis();
        long elapsedTime = retrieveEndTime - retrieveStartTime;
        System.out.println("Retrieve path 1000 time in "+elapsedTime+" ms");
    }
    
    static final int kNumVersionCycles = (ListVersionsReply.kMaxReturnVersions * 2) + (ListVersionsReply.kMaxReturnVersions / 2);	// Force two full retrieves + a partial
    static final int kVersionBytesToWrite = 1024;
    public void testFileVersions()
    throws Exception
    {
    	String name = "versionsTestFile";
    	CreateFileInfo testInfo = root.createChildFile(name, true);
    	IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    	for (int cycle = 0; cycle < kNumVersionCycles; cycle++)
    	{
    		connection.startTransaction();
    		IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    		byte [] testData = new byte[kVersionBytesToWrite];
    		byte testByte = Integer.toString(cycle).getBytes("UTF-8")[0];
    		for (int curByteNum = 0; curByteNum < testData.length; curByteNum++)
    		{
    			testData[curByteNum] = testByte;
    		}
    		testDataFork.append(testData);
    		connection.commit();
    	}
    	assertEquals(kVersionBytesToWrite * kNumVersionCycles, curTestFile.length());
    	IndelibleVersionIterator iterator = curTestFile.listVersions();
    	int numVersions = 0;
        IndelibleVersion lastVersion = null;
        boolean noForkForVersionZero = false;
        while (iterator.hasNext())
        {
        	IndelibleVersion curVersion = iterator.next();
        	if (lastVersion != null)
        	{
        		if (curVersion.getVersionTime() < lastVersion.getVersionTime())
        			fail ("Versions not in order");
        		if (curVersion.getVersionTime() == lastVersion.getVersionTime())
        		{
        			if (curVersion.getUniquifier() <= lastVersion.getUniquifier())
        				fail ("Uniquifier not incrementing or out of order");
        		}
        	}
        	IndelibleFileNodeIF curVersionNode = curTestFile.getVersion(curVersion, RetrieveVersionFlags.kExact);
        	assertEquals(kVersionBytesToWrite * numVersions, curVersionNode.length());
        	
        	byte [] checkBuffer = new byte[kVersionBytesToWrite];
        	try
        	{
        		assertEquals(kVersionBytesToWrite, curVersionNode.getFork("data", false).read(kVersionBytesToWrite * (numVersions - 1), checkBuffer));
        		byte checkByte = Integer.toString(numVersions - 1).getBytes("UTF-8")[0];
        		assertEquals(checkByte, checkBuffer[0]);
        		assertEquals(checkByte, checkBuffer[kVersionBytesToWrite - 1]);
        	}
        	catch (ForkNotFoundException e)
        	{
        		if (numVersions == 0)
        			noForkForVersionZero = true;
        		else
        			fail("Fork for version "+curVersion+" "+numVersions+" not found");
        	}
        	numVersions++;
        }
        assertTrue(noForkForVersionZero);
    	assertEquals(kNumVersionCycles + 1, numVersions);
    }
    
    public void testCreateChildSymlink()
    throws Exception
    {
            String name = "symlink-target";
            CreateFileInfo testInfo = root.createChildFile(name, true);
            IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
            root = testInfo.getDirectoryNode();
            IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
            IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
            SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
            forkOutputStream.close();
            IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
            assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
            
            String symlinkName = "test-symlink";
            CreateSymlinkInfo symlinkInfo = root.createChildSymlink(symlinkName, name, true);
    }
    
    public void testGetVolumeName() throws PermissionDeniedException, IOException
    {
    	String checkVolumeName = testVolume.getVolumeName();
    	assertEquals(testVolumeName, checkVolumeName);
    }
}
