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
 
package com.igeekinc.indelible.indeliblefs.uniblock.dbfs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServerCore;
import com.igeekinc.indelible.indeliblefs.server.IndelibleFSMain;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.FatalLogMessage;

public class DBFSCASServerTest extends TestCase
{

    private String dbURL;
    private String dbUser;
    private String dbPassword;
    private GeneratorIDFactory myGenIDFactory;
    private GeneratorID myGenID;
    private ObjectIDFactory testFactory;
    private DBCASServer server;
    private DBCASServerConnection connection;
    private static boolean dataMoverInitialized = false;
    
    private CASCollectionConnection testCollection;
    private Logger logger;
    
    public void setUp()
    throws Exception
    {
        logger = Logger.getLogger(getClass());
        BasicConfigurator.configure();
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
        
        dbURL = System.getProperty("dbURL");
        dbUser = System.getProperty("dbUser");
        dbPassword = System.getProperty("dbPass");
        myGenIDFactory = new GeneratorIDFactory();
        myGenID = myGenIDFactory.createGeneratorID();
        testFactory = new ObjectIDFactory(myGenID);
        File casStorageDir = new File("/tmp/castest");
        casStorageDir.mkdir();
        server = new DBCASServer(dbURL, dbUser, dbPassword, null);
        server.setSecurityServerID(securityServer.getEntityID());
        
        if (!dataMoverInitialized)
        {
            GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
            GeneratorID testBaseID = genIDFactory.createGeneratorID();
            ObjectIDFactory oidFactory = new ObjectIDFactory(testBaseID);
            DataMoverSource.init(oidFactory);
            DataMoverReceiver.init(oidFactory);
            dataMoverInitialized = true;
        }
        
        connection = server.open(EntityAuthenticationClient.getEntityAuthenticationClient().getEntityID());
        testCollection = connection.createNewCollection();
    }
    
    /*
     * Class under test for void BasicDataDescriptor(byte[])
     */
    public void testBasicDataStorage()
    throws Exception
    {
        long totalStartTime = System.currentTimeMillis();
        byte [] test10 = new byte[10];
        byte [] test100 = new byte[100];
        byte [] test1024 = new byte[1024];
        byte [] test1M = new byte[1024*1024];
        fillInArray(test10);
        fillInArray(test100);
        fillInArray(test1024);
        fillInArray(test1M);
        
        long startTime = System.currentTimeMillis();
        CASIDMemoryDataDescriptor bdd10 = new CASIDMemoryDataDescriptor(test10);
        System.out.println("Created BasicDataDescriptor with 10 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        CASIDMemoryDataDescriptor bdd100 = new CASIDMemoryDataDescriptor(test100);
        System.out.println("Created BasicDataDescriptor with 100 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        CASIDMemoryDataDescriptor bdd1024 = new CASIDMemoryDataDescriptor(test1024);
        System.out.println("Created BasicDataDescriptor with 1024 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        CASIDMemoryDataDescriptor bdd1M = new CASIDMemoryDataDescriptor(test1M);
        System.out.println("Created BasicDataDescriptor with 1M bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        
        CASSegmentID bdd10SegID = testCollection.storeSegment(bdd10).getSegmentID();
        System.out.println("Stored BasicDataDescriptor with 10 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        CASSegmentID bdd100SegID = testCollection.storeSegment(bdd100).getSegmentID();
        System.out.println("Stored BasicDataDescriptor with 100 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        CASSegmentID bdd1024SegID = testCollection.storeSegment(bdd1024).getSegmentID();
        System.out.println("Stored BasicDataDescriptor with 1024 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        CASSegmentID bdd1MSegID = testCollection.storeSegment(bdd1M).getSegmentID();
        System.out.println("Stored BasicDataDescriptor with 1M bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        
        retrieveAndCompare(bdd10);
        System.out.println("Retrieved BasicDataDescriptor with 10 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        retrieveAndCompare(bdd100);
        System.out.println("Retrieved BasicDataDescriptor with 100 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        retrieveAndCompare(bdd1024);
        System.out.println("Retrieved BasicDataDescriptor with 1024 bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        retrieveAndCompare(bdd1M);
        System.out.println("Retrieved BasicDataDescriptor with 1M bytes - elapsed time = "+(System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        
        System.out.println("Total test time = "+(System.currentTimeMillis()-totalStartTime));
        
        testCollection.releaseSegment(bdd10SegID);
        testCollection.releaseSegment(bdd100SegID);
        testCollection.releaseSegment(bdd1024SegID);
        testCollection.releaseSegment(bdd1MSegID);
        
    }
    void fillInArray(byte [] arrayToFill)
    {
        for (int curByteNum = 0; curByteNum < arrayToFill.length; curByteNum++)
            arrayToFill[curByteNum] = (byte)curByteNum;
    }
    
    void retrieveAndCompare(CASIDMemoryDataDescriptor originalDataDescriptor)
    throws IOException, SegmentNotFound
    {
        CASIdentifier segmentID = originalDataDescriptor.getCASIdentifier();
        DataDescriptor retrievedDataDescriptor = testCollection.retrieveSegment(segmentID);
        byte [] retrievedData = retrievedDataDescriptor.getData();
        byte [] originalData = originalDataDescriptor.getData();
        assertTrue(Arrays.equals(retrievedData, originalData));
    }
}
