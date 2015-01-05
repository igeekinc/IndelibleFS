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
 
package com.igeekinc.indelible.indeliblefs.datamover;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.text.MessageFormat;

import org.newsclub.net.unix.AFUNIXSocketAddress;

import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.junitext.iGeekTestCase;
import com.igeekinc.util.MonitoredProperties;

public class BasicNetworkDataMoverTest extends iGeekTestCase
{
    private static final double kKilobytes = 1024.0;
    private static final double kMegabytes = kKilobytes * kKilobytes;
    private static final double	kGigabytes	= kKilobytes*kMegabytes;


	public BasicNetworkDataMoverTest()
    {
		
    }
    private static boolean securityAndMoverInitialized = false;
    private static DataMoverSession moverSession;
    private static EntityAuthenticationServer securityServer;
    private static DataMoverTargetRemote remote;
    
    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        if (!securityAndMoverInitialized)
        {
        	IndelibleServerPreferences.initPreferences(null);

            MonitoredProperties serverProperties = IndelibleServerPreferences.getProperties();
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
            
            securityServer = securityServers[0];
            
            EntityAuthenticationClient.getEntityAuthenticationClient().trustServer(securityServer);

            GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
            GeneratorID testBaseID = genIDFactory.createGeneratorID();
            ObjectIDFactory oidFactory = new ObjectIDFactory(testBaseID);
            DataMoverReceiver.init(oidFactory);
            DataMoverSource.init(oidFactory, new InetSocketAddress(0), new AFUNIXSocketAddress(new File("/tmp/bndt-socket")));
            
            moverSession = DataMoverSource.getDataMoverSource().createDataMoverSession(securityServer.getEntityID());
            String remoteHost = System.getProperty("com.igeekinc.indelible.indeliblefs.datamover.BasicNetworkDataMoverTest.remoteHost");
            if (remoteHost == null || remoteHost.trim().length() == 0)
            	remoteHost = "localhost";
            remote = (DataMoverTargetRemote) LocateRegistry.getRegistry(remoteHost, 7989).lookup("DataMoverTarget");
            EntityAuthentication serverAuthentication = remote.getServerEntityAuthentication();
            SessionAuthentication serverSessionAuthentication = moverSession.addAuthorizedClient(serverAuthentication);
            remote.addClientSessionAuthentication(serverSessionAuthentication);
			/*
			 * Now, get the authentication that allows us to read data from the server
			 */
			SessionAuthentication remoteAuthentication = remote.addClient();
			DataMoverReceiver.getDataMoverReceiver().addSessionAuthentication(remoteAuthentication);
			
            securityAndMoverInitialized = true;
            
        }
    }
    public static final int kSingleReadSize = 1024*1024;
    
    public void testSingleRead() throws IOException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException, IllegalStateException, CertificateException, UnrecoverableKeyException, AuthenticationFailureException
    {
		CASIDDataDescriptor single = remote.pullDescriptor(kSingleReadSize, 0);
        byte [] readBuf = single.getData();
        assertTrue(DataMoverTargetMain.verifyBuffer(readBuf, single.getCASIdentifier().getHashID(), 0, kSingleReadSize, 0));
    }
    
    public static final int kSmallBufSize=16;
    public static final int kNumSmallBufs = 1000;
    public void test1000SmallBufs() throws IOException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException, IllegalStateException, CertificateException, UnrecoverableKeyException, AuthenticationFailureException
    {
    	long startTime = System.currentTimeMillis();
    	for (int seq = 0; seq < kNumSmallBufs; seq++)
    	{
    		CASIDDataDescriptor curSmallBuf = remote.pullDescriptor(kSmallBufSize, seq);
            byte [] readBuf = curSmallBuf.getData();
            assertTrue(DataMoverTargetMain.verifyBuffer(readBuf, curSmallBuf.getCASIdentifier().getHashID(), 0, kSmallBufSize, seq));
    	}
    	long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
    	logger.error("test1000SmallBufs - "+kNumSmallBufs+" "+kSmallBufSize+" buffers read in "+elapsedMS+" ms, "+((double)kNumSmallBufs/((double)elapsedMS/1000.0))+" calls/s");
    }

    public static final int kLargeBufSize = 64 * 1024 * 1024;
    public void testSingleLargeBuf() throws IOException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException, IllegalStateException, CertificateException, UnrecoverableKeyException, AuthenticationFailureException
    {
    	long startTime = System.currentTimeMillis();
		CASIDDataDescriptor single = remote.pullDescriptor(kLargeBufSize, 0);
        long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
		logger.error(kLargeBufSize+" bytes generated in "+elapsedMS+" ms, "+speed(kLargeBufSize, elapsedMS));
		startTime = System.currentTimeMillis();
        byte [] readBuf = single.getData();
        endTime = System.currentTimeMillis();
        assertTrue(DataMoverTargetMain.verifyBuffer(readBuf, single.getCASIdentifier().getHashID(), 0, kLargeBufSize, 0));
    	elapsedMS = endTime - startTime;
    	logger.error("testSingleLargeBuf - "+kLargeBufSize+" bytes read in "+elapsedMS+" ms, "+speed(kLargeBufSize, elapsedMS));
    }
    
    public void testWriteLargeBufToFile() throws IOException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException, IllegalStateException, CertificateException, UnrecoverableKeyException, AuthenticationFailureException
    {
    	File outputFile = File.createTempFile("bndmt", ".data");
    	FileOutputStream fos = new FileOutputStream(outputFile);
    	long startTime = System.currentTimeMillis();
		CASIDDataDescriptor single = remote.pullDescriptor(kLargeBufSize, 0);
        long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
		logger.error(kLargeBufSize+" bytes generated in "+elapsedMS+" ms, "+speed(kLargeBufSize, elapsedMS));
		startTime = System.currentTimeMillis();
        single.writeData(fos);
        fos.close();
        endTime = System.currentTimeMillis();
        FileInputStream readStream = new FileInputStream(outputFile);
        byte [] readBuf = new byte[kLargeBufSize];
        assertEquals(kLargeBufSize, readStream.read(readBuf));
        readStream.close();
        assertTrue(DataMoverTargetMain.verifyBuffer(readBuf, single.getCASIdentifier().getHashID(), 0, kLargeBufSize, 0));
    	elapsedMS = endTime - startTime;
    	logger.error("testWriteLargeBufToFile - "+kLargeBufSize+" bytes read in "+elapsedMS+" ms, "+speed(kLargeBufSize, elapsedMS));
    }
    public static final int kMultipleBufSize = 1024*1024;
    public static final int kNumMultipleBufs = 64;
    public void testMultipleBuf() throws IOException 
    {
    	CASIDDataDescriptor [] multiples = new CASIDDataDescriptor[kNumMultipleBufs];

    	long startTime = System.currentTimeMillis();
    	for (int curBufNum = 0; curBufNum < kNumMultipleBufs; curBufNum++)
    		multiples[curBufNum] = remote.pullDescriptor(kMultipleBufSize, curBufNum);
    	long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
    	long bytes = kNumMultipleBufs * kMultipleBufSize;
    	logger.error(bytes+" bytes ("+kNumMultipleBufs+" of "+kMultipleBufSize+") generated in "+elapsedMS+" ms, "+speed(bytes, elapsedMS));
    	startTime = System.currentTimeMillis();
    	byte [][] readBufs = new byte [kNumMultipleBufs][];
    	for (int curBufNum = 0; curBufNum < kNumMultipleBufs; curBufNum++)
    	{
    		readBufs[curBufNum] = multiples[curBufNum].getData();

    	}
    	endTime = System.currentTimeMillis();
    	for (int curBufNum = 0; curBufNum < kNumMultipleBufs; curBufNum++)
    	{
    		assertTrue(DataMoverTargetMain.verifyBuffer(readBufs[curBufNum], multiples[curBufNum].getCASIdentifier().getHashID(), 0, kMultipleBufSize, curBufNum));
    	}
    	elapsedMS = endTime - startTime;
    	logger.error("testMultipleBuf - "+bytes+" bytes read in "+elapsedMS+" ms, "+speed(bytes, elapsedMS));
    }
    public void testPutSingleBuf() throws Exception
    {
    	CASIDDataDescriptor singleBuf = moverSession.registerDataDescriptor(DataMoverTargetMain.generateDescriptor(kSingleReadSize, 0));
    	long startTime = System.currentTimeMillis();
    	remote.pushDescriptor(singleBuf, 0);
    	long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
    	logger.error("testPutSingleBuf - "+kSingleReadSize+" sent in "+elapsedMS+" ms, "+speed(kSingleReadSize, elapsedMS));
    }
    
    public void testPutMultipleBufs() throws Exception
    {
    	CASIDDataDescriptor [] multiples = new CASIDDataDescriptor[kNumMultipleBufs];
    	for (int curBufNum = 0; curBufNum < kNumMultipleBufs; curBufNum ++)
    		multiples[curBufNum] = moverSession.registerDataDescriptor(DataMoverTargetMain.generateDescriptor(kMultipleBufSize, curBufNum));
    	long startTime = System.currentTimeMillis();
    	for (int curBufNum = 0; curBufNum < kNumMultipleBufs; curBufNum ++)
    		remote.pushDescriptor(multiples[curBufNum], curBufNum);
    	long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
    	long bytes = kMultipleBufSize * kNumMultipleBufs;
    	logger.error("testPutMultipleBufs - "+bytes+" sent in "+elapsedMS+" ms, "+speed(bytes, elapsedMS));
    }
    
    public void testPutMultipleBufsGroup() throws Exception
    {
    	CASIDDataDescriptor [] multiples = new CASIDDataDescriptor[kNumMultipleBufs];
    	for (int curBufNum = 0; curBufNum < kNumMultipleBufs; curBufNum ++)
    		multiples[curBufNum] = moverSession.registerDataDescriptor(DataMoverTargetMain.generateDescriptor(kMultipleBufSize, curBufNum));
    	long startTime = System.currentTimeMillis();
    	remote.pushDescriptors(multiples, 0);
    	long endTime = System.currentTimeMillis();
    	long elapsedMS = endTime - startTime;
    	long bytes = kMultipleBufSize * kNumMultipleBufs;
    	logger.error("testPutMultipleBufsGroup - "+bytes+" sent in "+elapsedMS+" ms, "+speed(bytes, elapsedMS));
    }
    
    public String speed(long bytesMoved, long elapsedMilliseconds)
    {
    	double seconds = (double)elapsedMilliseconds/1000.0;
    	double bytesPerSec = (double)bytesMoved/seconds;
    	if (bytesPerSec > kGigabytes)
    	{
    		double gigabytesPerSec = bytesPerSec/kGigabytes;
    		return MessageFormat.format("{0,number, #.##}GB/s", gigabytesPerSec);
    	}
    	if (bytesPerSec > kMegabytes)
    	{
    		double megabytesPerSec = bytesPerSec/kMegabytes;
    		return MessageFormat.format("{0,number, #.##}MB/s", megabytesPerSec);
    	}
    	if (bytesPerSec > kKilobytes)
    	{
    		double kilobytesPerSec = bytesPerSec/kKilobytes;
    		return MessageFormat.format("{0,number, #.##}KB/s", kilobytesPerSec);
    	}
    	return MessageFormat.format("{0,number, #.##}B/s", bytesPerSec);
    }
}
