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
 
package com.igeekinc.indelible.indeliblefs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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
import com.igeekinc.indelible.indeliblefs.proxies.IndelibleFSServerProxy;
import com.igeekinc.indelible.indeliblefs.remote.CreateDirectoryInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.CreateFileInfoRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleDirectoryNodeRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteInputStream;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteOutputStream;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SHA1HashID;

public class IndelibleFSServerVersionsTest extends TestCase
{
	private static final int kSmallDirTreeFileSize = 64*1024;
	private static IndelibleFSServerProxy fsServer;
    private static IndelibleFSVolumeIF testVolume;
    private static IndelibleDirectoryNodeIF root;
    private static IndelibleServerConnectionIF connection;
    private static DataMoverSession moverSession;
    private static boolean dataMoverInitialized = false;
    private Logger logger;
    
    public void setUp()
    throws Exception
    {
    	if (root == null)
    	{
    		IndelibleServerPreferences.initPreferences(null);

    		MonitoredProperties serverProperties = IndelibleServerPreferences.getProperties();
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
    		IndelibleFSServerProxy[] servers = new IndelibleFSServerProxy[0];

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
    			DataMoverSource.init(oidFactory);
    			DataMoverReceiver.init(oidFactory);
    			dataMoverInitialized = true;
    		}
    		connection = fsServer.open();
    		connection.startTransaction();
    		testVolume = connection.createVolume(null);
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

    public void testSingleFileVersion() throws Exception
    {
    	connection.startTransaction();
    	CreateFileInfo fileInfo = root.createChildFile("versionFile0", true);
    	IndelibleFileNodeIF curTestFile = fileInfo.getCreatedNode();
    	IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
        PrintStream ps = new PrintStream(forkOutputStream);
        ps.println("0");
        ps.close();
    	connection.commit();
    	
    	connection.startTransaction();
    	curTestFile = root.getChildNode("versionFile0");
    	testDataFork = curTestFile.getFork("data", false);
        forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
        ps = new PrintStream(forkOutputStream);
        ps.println("1");
        ps.close();
    	connection.commit();
    	
    	IndelibleVersionIterator versions = curTestFile.listVersions();
    	int numVersions = 0;
    	while(versions.hasNext())
    	{
    		IndelibleVersion curVersion = versions.next();
			IndelibleFileNodeIF curVersionFile = curTestFile.getVersion(curVersion, RetrieveVersionFlags.kExact);
    		assertNotNull(curVersionFile);
    		IndelibleVersion checkVersion = curVersionFile.getVersion();
			assertEquals(curVersion, checkVersion);
    		IndelibleFSForkIF testReadFork = curVersionFile.getFork("data", false);
    		BufferedReader br = new BufferedReader(new InputStreamReader(new IndelibleFSForkRemoteInputStream(testReadFork)));
    		String line = br.readLine();
    		line = line.trim();
    		assertEquals(((Integer)numVersions).toString(), line);
    		numVersions++;
    		br.close();
    	}
    	assertEquals(2, numVersions);
    }

    public void testVersionedTree() throws Exception
    {
    	connection.startTransaction();
    	IndelibleDirectoryNodeRemote testRoot = createSmallDirTree();
    	connection.commit();
    	
    	
    }
	private IndelibleDirectoryNodeRemote createSmallDirTree() throws IOException,
			PermissionDeniedException, RemoteException, FileExistsException,
			ForkNotFoundException, ObjectNotFoundException {
		Date now = new Date();
    	FilePath testDirPath = FilePath.getFilePath("/testCreateDirTreeSingleTrans "+now);
    	CreateDirectoryInfo testDirInfo = root.createChildDirectory(testDirPath.getName());
    	root = testDirInfo.getDirectoryNode();

        for (int dirNum =0 ;dirNum < 3; dirNum++)
        {
        	IndelibleDirectoryNodeRemote testDir = (IndelibleDirectoryNodeRemote) testVolume.getObjectByPath(testDirPath);
        	FilePath curDirPath = testDirPath.getChild("test"+dirNum);
        	CreateDirectoryInfoRemote curDirInfo = testDir.createChildDirectory(curDirPath.getName());
        	IndelibleDirectoryNodeRemote curDir = (IndelibleDirectoryNodeRemote) testVolume.getObjectByPath(curDirPath);
        	FilePath subDirPath = curDirPath.getChild("sub-"+dirNum);
        	CreateDirectoryInfoRemote subDirInfo = curDir.createChildDirectory(subDirPath.getName());
        	IndelibleDirectoryNodeRemote subDir = (IndelibleDirectoryNodeRemote) testVolume.getObjectByPath(subDirPath);
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
        	CreateFileInfoRemote testInfo = subDir.createChildFile(name, initialForkData, true);
        }
        return (IndelibleDirectoryNodeRemote) testVolume.getObjectByPath(testDirPath);
	}
	
	/*
    public void testCreateChildDirectory()
    throws Exception
    {
    	Date now = new Date();
    	CreateDirectoryInfoRemote testDirInfo = root.createChildDirectory("testCreateChildDirectory "+now);
    	IndelibleDirectoryNodeRemote testDir = testDirInfo.getCreateNode();
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
    }
 
    public void testCreateChildFile()
    throws Exception
    {
        for (int fileNum = 0; fileNum < 10; fileNum++)
        {
            String name = "testfile-"+fileNum;
            CreateFileInfoRemote testInfo = root.createChildFile(name);
            IndelibleFileNodeRemote curTestFile = testInfo.getCreateNode();
            root = testInfo.getDirectoryNode();
            IndelibleFSForkRemote testDataFork = curTestFile.getFork("data", true);
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
        CreateDirectoryInfoRemote newInfo = root.createChildDirectory("speedTestDir");
        IndelibleDirectoryNodeRemote speedTestDir = (IndelibleDirectoryNodeRemote) newInfo.getCreateNode();
        for (int fileNum = 0; fileNum < 1000; fileNum++)
        {
            String name = "testfile-"+fileNum;
            CreateFileInfoRemote curInfo = speedTestDir.createChildFile(name);
            IndelibleFileNodeRemote curTestFile = curInfo.getCreateNode();
            speedTestDir = curInfo.getDirectoryNode();
            IndelibleFSForkRemote testDataFork = curTestFile.getFork("data", true);
            IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
            SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 1024);
            forkOutputStream.close();
        }
        connection.commit();
        long writeEndMillis = System.currentTimeMillis();
        long elapsedWriteMillis = writeEndMillis - writeStartMillis;
        System.out.println("Created 1000 files (1K) in "+elapsedWriteMillis+" ms, speed = "+(1000.0/((double)elapsedWriteMillis/1000.0))+" files/s");
    }

    public void testNewCreateFileSpeed()
    throws Exception
    {
        long writeStartMillis = System.currentTimeMillis();
        connection.startTransaction();
        CreateDirectoryInfoRemote dirInfo = root.createChildDirectory("speedTestDir1");
        IndelibleDirectoryNodeRemote speedTestDir = dirInfo.getCreateNode();
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
            CreateFileInfoRemote curTestInfo = speedTestDir.createChildFile(name, forkData);
            moverSession.removeDataDescriptor(childData);
            IndelibleFileNodeRemote curTestfile = curTestInfo.getCreateNode();
            speedTestDir = curTestInfo.getDirectoryNode();
        }
        connection.commit();
        long writeEndMillis = System.currentTimeMillis();
        long elapsedWriteMillis = writeEndMillis - writeStartMillis;
        System.out.println("Created 1000 files (1K) in "+elapsedWriteMillis+" ms, speed = "+(1000.0/(((double)elapsedWriteMillis)/1000.0))+" files/s");
    }

    public void testReadWriteSpeed()
    throws Exception
    {
        CreateFileInfoRemote testInfo = root.createChildFile("speedTest");
        IndelibleFileNodeRemote speedTestFile = testInfo.getCreateNode();
        
        IndelibleFSObjectID objectID = (IndelibleFSObjectID) speedTestFile.getObjectID();
        IndelibleFSForkRemote speedTestFork = speedTestFile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(speedTestFork, false, moverSession);
        long writeStartMillis = System.currentTimeMillis();
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 100*1024*1024);
        forkOutputStream.close();
        long writeEndMillis = System.currentTimeMillis();
        long elapsedWriteMillis = writeEndMillis - writeStartMillis;
        System.out.println("Wrote 100MB in "+elapsedWriteMillis+" ms, speed = "+(100.0/(elapsedWriteMillis/1000))+" MB/s");
        
        IndelibleFileNodeRemote readbackFile = root.getChildNode("speedTest");
        assertNotNull(readbackFile);
        assertEquals(objectID, readbackFile.getObjectID());
        
        IndelibleFSForkRemote readbackFork = readbackFile.getFork("data", false);
        assertNotNull(readbackFork);
        assertEquals(100*1024*1024, readbackFile.totalLength());
        long readStartMillis = System.currentTimeMillis();
        IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(readbackFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 100*1024*1024, hashID));
        long readEndMillis = System.currentTimeMillis();
        long elapsedReadMillis = readEndMillis - readStartMillis;
        System.out.println("Read 100MB in "+elapsedReadMillis+" ms, speed = "+(100.0/(elapsedReadMillis/1000))+" MB/s");
    }

    public void testDuplicateFile()
    throws Exception
    {
        String sourceName="dupSrcFile";
        CreateFileInfoRemote testInfo = root.createChildFile(sourceName);
        IndelibleFileNodeRemote curTestfile = testInfo.getCreateNode();
        root = testInfo.getDirectoryNode();
        IndelibleFSForkRemote testDataFork = curTestfile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
        forkOutputStream.close();
        IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
        
        String duplicateName = "dupFile";
        CreateFileInfoRemote dupTestInfo = root.createChildFile(duplicateName, (IndelibleFSObjectID)curTestfile.getObjectID());
        IndelibleFileNodeRemote dupTestfile = dupTestInfo.getCreateNode();
        root = dupTestInfo.getDirectoryNode();
        IndelibleFSForkRemote testDupFork = dupTestfile.getFork("data", false);
        forkInputStream = new IndelibleFSForkRemoteInputStream(testDupFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
        
    }
    
    public void testDeleteFile()
    throws Exception
    {
        String sourceName="deleteFile";
        CreateFileInfoRemote testInfo = root.createChildFile(sourceName);
        IndelibleFileNodeRemote curTestfile = testInfo.getCreateNode();
        root = testInfo.getDirectoryNode();
        IndelibleFSForkRemote testDataFork = curTestfile.getFork("data", true);
        IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
        forkOutputStream.close();
        IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        forkInputStream.close();
        
        assertTrue(root.deleteChild(sourceName));
        IndelibleFileNodeRemote deletedFile;
        
        deletedFile = root.getChildNode(sourceName);
        
        assertNull(deletedFile);
        
        String dirName = "deleteDir";
        CreateDirectoryInfoRemote newDirInfo = root.createChildDirectory(dirName);
        
        curTestfile = root.getChildNode(dirName);
        assertNotNull(curTestfile);
        
        assertTrue(root.deleteChildDirectory(dirName));
        
        curTestfile = root.getChildNode(dirName);
        assertNull(curTestfile);
        
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
    	
    	CreateFileInfoRemote oneGBInfo = root.createChildFile(oneGBName);
        IndelibleFileNodeRemote oneGBFile = oneGBInfo.getCreateNode();
        root = oneGBInfo.getDirectoryNode();
        long startOneGB = System.currentTimeMillis();
        IndelibleFSForkRemote oneGBFork = oneGBFile.getFork("data", true);
        oneGBFork.extend(kOneGBLength);
        System.out.println("Created 1GB blank file in "+(System.currentTimeMillis() - startOneGB)+" ms");
        assertEquals(kOneGBLength, oneGBFile.totalLength());

    }
*/
}
