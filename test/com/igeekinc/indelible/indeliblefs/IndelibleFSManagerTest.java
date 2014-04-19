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

import java.io.File;
import java.io.FilenameFilter;
import java.security.Security;
import java.util.Arrays;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleDirectoryNode;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSFork;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManager;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManagerConnection;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFileNode;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServerCore;
import com.igeekinc.indelible.indeliblefs.server.IndelibleFSMain;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.FatalLogMessage;

public class IndelibleFSManagerTest extends TestCase
{
    private String dbURL;
    private String dbUser;
    private String dbPassword;
    private GeneratorIDFactory myGenIDFactory;
    private GeneratorID myGenID;
    private ObjectIDFactory testFactory;
    private EntityID testServerID;
    private DBCASServer server;
    private IndelibleFSManager fsManager;
    private IndelibleFSVolumeIF testVolume;
    private IndelibleDirectoryNodeIF root;
    private IndelibleFSManagerConnection managerConnection;
    private Logger logger;
    
    public void setUp()
    throws Exception
    {
        logger = Logger.getLogger(getClass());
        Security.insertProviderAt(new org.bouncycastle.jce.provider.BouncyCastleProvider(), 2);
        dbURL = System.getProperty("dbURL");
        dbUser = System.getProperty("dbUser");
        dbPassword = System.getProperty("dbPass");
        myGenIDFactory = new GeneratorIDFactory();
        myGenID = myGenIDFactory.createGeneratorID();
        testFactory = new ObjectIDFactory(myGenID);
        testServerID = (EntityID)testFactory.getNewOID(DBCASServer.class);
        
        File casStorageDir = new File("/tmp/castest");
        casStorageDir.mkdir();
        server = new DBCASServer(dbURL, dbUser, dbPassword, null);
        
        IndelibleServerPreferences.initPreferences(null);
        MonitoredProperties serverProperties = IndelibleServerPreferences.getProperties();

        File preferencesDir = new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName));
        
        File securityRootDir = new File(preferencesDir, IndelibleFSMain.kIndelibleEntityAuthenticationServerRootDir);
        
        if (serverProperties.getProperty(IndelibleServerPreferences.kEntityAuthenticationServerWasConfigured).equals("N"))
        {
            if (serverProperties.getProperty(IndelibleServerPreferences.kAutoInitEntityAuthenticationServerPropertyName).equals("Y"))
            {

                if (securityRootDir.exists())
                {
                    if (securityRootDir.isDirectory())
                    {
                        if (securityRootDir.list(new FilenameFilter() {
                            public boolean accept(File arg0, String arg1)
                            {
                                if (arg1.startsWith("."))
                                    return false;
                                return true;
                            }
                        }).length > 0)
                        {
                            logger.fatal("Security root dir "+securityRootDir.getAbsolutePath()+" exists but we are not configured to use it - exiting");
                            System.exit(-1);
                        }
                        else
                        {
                            securityRootDir.delete();
                        }
                    }
                    else
                    {
                        logger.fatal("Security root dir "+securityRootDir.getAbsolutePath()+" exists but is not a directory - exiting");
                        System.exit(-1);
                    }
                }
                if (securityRootDir.mkdir())
                {
                    EntityAuthenticationServerCore.initRootSecurity(securityRootDir, (EntityID)server.getOIDFactory().getNewOID(EntityAuthenticationServerCore.class));
                }
                else
                {
                    logger.fatal("Could not create security root dir "+securityRootDir.getAbsolutePath());
                    System.exit(-1);
                }
            }
            serverProperties.setProperty(IndelibleServerPreferences.kEntityAuthenticationServerWasConfigured, "Y");
            IndelibleServerPreferences.storeProperties();
        }
        EntityAuthenticationServer securityServer;
        if (securityRootDir.exists())
        {
            securityServer = new EntityAuthenticationServerCore(securityRootDir);
        }
        else
        {
            assertFalse("Could not configure security server", true);
            return;
        }
        
        // Now, check and configure the security client
        
        File securityClientKeystoreFile = new File(preferencesDir, IndelibleFSClient.kIndelibleEntityAuthenticationClientConfigFileName);
        if (securityClientKeystoreFile.exists())
        {
            try
            {
                EntityAuthenticationClient.initializeEntityAuthenticationClient(securityClientKeystoreFile, testFactory, serverProperties);
                EntityAuthenticationClient.getEntityAuthenticationClient().trustServer(securityServer);
            } catch (AuthenticationFailureException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
                System.exit(-1);
            }
        }
        else
        {
            if (serverProperties.getProperty(IndelibleServerPreferences.kAutoInitEntityAuthenticationServerPropertyName).equals("Y") && securityServer != null)
            {
                // We're supposed to auto init...If securityServer is set then we can just use that (it's either our server in this
                // JVM or the only security server found on the net.  If it's not set, then we need to bail
                EntityAuthenticationClient.initIdentity(securityClientKeystoreFile, server.getServerID(), securityServer.getServerCertificate());
            }
            else
            {
                // Not initialized and we're not supposed to do it automatically - just exit
                logger.fatal(new FatalLogMessage("No security server configured and auto configure is disabled, exiting"));
                System.exit(-1);
            }
        }
        
        DataMoverSource.shutdown();
        DataMoverReceiver.shutdown();
        
        // Should have the CAS server and the security server and client configured by this point
        DataMoverSource.init(server.getOIDFactory());   // TODO - move this someplace logical
        DataMoverReceiver.init(server.getOIDFactory());
        fsManager = new IndelibleFSManager(server);
        managerConnection = fsManager.open((EntityID)null);
        testVolume = managerConnection.createVolume(null);
        Date createDate = new Date();
        testVolume.setVolumeName("Test volume "+createDate);
        root = testVolume.getRoot();
    }
    
    public void testCreateChildDirectory()
    throws Exception
    {
        //managerConnection.startTransaction();
        long startTime = System.currentTimeMillis();
        for (int dirNum =0 ;dirNum < 10; dirNum++)
        {
            root.createChildDirectory("test"+dirNum);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Created 10 directories in "+(endTime-startTime)+" ms");
        String [] children = root.list();
        assertEquals(10, children.length);
        Arrays.sort(children);
        for (int curChildNum = 0 ;curChildNum < children.length; curChildNum++)
        {
            System.out.println(children[curChildNum]);
            assertEquals("test"+curChildNum, children[curChildNum]);
        }
        //managerConnection.commit();
        
        IndelibleFSObjectID volumeID = testVolume.getObjectID();
        
        File casStorageDir = new File("/tmp/castest");
        casStorageDir.mkdir();
        server = new DBCASServer(dbURL, dbUser, dbPassword, null);
        fsManager = new IndelibleFSManager(server);
        managerConnection = fsManager.open((EntityID)null);
        testVolume = managerConnection.retrieveVolume(volumeID);
        root = testVolume.getRoot();
        
        String [] checkChildren = root.list();
        assertEquals(10, checkChildren.length);
        Arrays.sort(checkChildren);
        for (int curChildNum = 0 ;curChildNum < children.length; curChildNum++)
        {
            System.out.println(checkChildren[curChildNum]);
            assertEquals("test"+curChildNum, checkChildren[curChildNum]);
        }
    }
    
    public void testCreateChildFile()
    throws Exception
    {
        managerConnection.startTransaction();
        for (int fileNum = 0; fileNum < 10; fileNum++)
        {
            String name = "testfile-"+fileNum;
            CreateFileInfo newInfo = root.createChildFile(name, true);
            IndelibleFileNodeIF curTestfile = newInfo.getCreatedNode();
            IndelibleFSForkIF testDataFork = curTestfile.getFork("data", true);
            long startTime = System.currentTimeMillis();
            IndelibleFSForkOutputStream forkOutputStream = new IndelibleFSForkOutputStream(testDataFork, false);
            SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 10*1024*1024);
            forkOutputStream.close();
            long endTime = System.currentTimeMillis();
            System.out.println("Wrote 10MB in "+(endTime-startTime)+" ms");
            startTime = System.currentTimeMillis();
            IndelibleFSForkInputStream forkInputStream = new IndelibleFSForkInputStream(testDataFork);
            endTime = System.currentTimeMillis();
            System.out.println("Verified 10MB in "+(endTime-startTime)+" ms");
            assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 10*1024*1024, hashID));
        }
        managerConnection.commit();
    }
    
    public void testReadWriteSpeed()
    throws Exception
    {
        managerConnection.startTransaction();
        CreateFileInfo newInfo = root.createChildFile("speedTest", true);
        IndelibleFileNodeIF speedTestFile = newInfo.getCreatedNode();
        IndelibleFSForkIF speedTestFork = speedTestFile.getFork("data", true);
        IndelibleFSForkOutputStream forkOutputStream = new IndelibleFSForkOutputStream(speedTestFork, false);
        long writeStartMillis = System.currentTimeMillis();
        SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, 100*1024*1024);
        forkOutputStream.close();
        long writeEndMillis = System.currentTimeMillis();
        long elapsedWriteMillis = writeEndMillis - writeStartMillis;
        System.out.println("Wrote 100MB in "+elapsedWriteMillis+" ms, speed = "+(100.0/(elapsedWriteMillis/1000))+" MB/s");
        
        long readStartMillis = System.currentTimeMillis();
        IndelibleFSForkInputStream forkInputStream = new IndelibleFSForkInputStream(speedTestFork);
        assertTrue(TestFilesTool.verifyInputStream(forkInputStream, 100*1024*1024, hashID));
        long readEndMillis = System.currentTimeMillis();
        long elapsedReadMillis = readEndMillis - readStartMillis;
        System.out.println("Read 100MB in "+elapsedReadMillis+" ms, speed = "+(100.0/(elapsedReadMillis/1000))+" MB/s");
        managerConnection.commit();
    }
    void fillDir(IndelibleDirectoryNode parent, String name, int numSubDirs, int curLevel, int maxLevels)
    {
        for (int dirNum = 0; dirNum < numSubDirs; dirNum++)
        {
        }
    }
}
