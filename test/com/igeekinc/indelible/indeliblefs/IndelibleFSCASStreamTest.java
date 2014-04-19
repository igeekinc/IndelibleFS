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

import java.util.Arrays;

import junit.framework.TestCase;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSCASFork;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServerConnection;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;

public class IndelibleFSCASStreamTest extends TestCase
{
    private String dbURL;
    private String dbUser;
    private String dbPassword;
    private GeneratorIDFactory myGenIDFactory;
    private GeneratorID myGenID;
    private ObjectIDFactory testFactory;
    private EntityID testServerID;
    private DBCASServer server;
    private DBCASServerConnection connection;
    private CASCollectionConnection testCollection;
    
    public void setUp()
    throws Exception
    {
        dbURL = System.getProperty("dbURL");
        dbUser = System.getProperty("dbUser");
        dbPassword = System.getProperty("dbPass");
        myGenIDFactory = new GeneratorIDFactory();
        myGenID = myGenIDFactory.createGeneratorID();
        testFactory = new ObjectIDFactory(myGenID);
        testServerID = (EntityID)testFactory.getNewOID(DBCASServer.class);
        server = new DBCASServer(dbURL, dbUser, dbPassword, null);
        connection = server.open((EntityID)null);
        testCollection = connection.createNewCollection();
    }
    
    public void testA()
    throws Exception
    {
        IndelibleFSCASFork testStream = new IndelibleFSCASFork(null, "");
        testStream.setCASCollection(testCollection);
        
        long totalStartTime = System.currentTimeMillis();
        byte [] test10 = new byte[10];
        byte [] test100 = new byte[100];
        byte [] test1024 = new byte[1024];
        byte [] test1M = new byte[1024*1024];
        fillInArray(test10);
        fillInArray(test100);
        fillInArray(test1024);
        fillInArray(test1M);
        
        connection.startTransaction();
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
        
        for (int i = 0; i < 10; i++)
        {
            testStream.appendDataDescriptor(bdd1M);
        }
        
        testStream.flush();
        
        testStream = new IndelibleFSCASFork(null, "");
        testStream.setCASCollection(testCollection);
        
        for (int i=0; i < 10*1024; i++)
        {
            testStream.appendDataDescriptor(bdd1024);
        }
        
        testStream.flush();
        
        byte [] readData = new byte[1024];
        for (int i=0; i < 10*1024; i++)
        {
            testStream.read(i*1024, readData);
            assertTrue(Arrays.equals(test1024, readData));
        }
        connection.commit();
    }
    
    void fillInArray(byte [] arrayToFill)
    {
        for (int curByteNum = 0; curByteNum < arrayToFill.length; curByteNum++)
            arrayToFill[curByteNum] = (byte)curByteNum;
    }
}
