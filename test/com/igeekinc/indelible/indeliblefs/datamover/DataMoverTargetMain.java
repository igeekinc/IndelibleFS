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

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SHA1HashID;

public class DataMoverTargetMain
{
    private DataMoverSession moverSession;
    private Logger logger;
    private EntityAuthenticationServer securityServer;
    private DataMoverTargetImpl server;
    private Registry registry;
    
	/**
	 * @param args
	 * @throws AuthenticationFailureException 
	 * @throws IOException 
	 * @throws CertificateException 
	 * @throws IllegalStateException 
	 * @throws SignatureException 
	 * @throws NoSuchProviderException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyStoreException 
	 * @throws UnrecoverableKeyException 
	 * @throws InvalidKeyException 
	 * @throws InterruptedException 
	 * @throws AlreadyBoundException 
	 */
	public static void main(String[] args) throws InvalidKeyException, UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException, IllegalStateException, CertificateException, IOException, AuthenticationFailureException, InterruptedException, AlreadyBoundException
	{
        BasicConfigurator.configure();
        LongOpt [] longOptions = {
                new LongOpt("registryPort", LongOpt.REQUIRED_ARGUMENT, null, 'r'),
        };
       // Getopt getOpt = new Getopt("MultiFSTestRunner", args, "p:ns:", longOptions);
        Getopt getOpt = new Getopt("DataMoverTargetMain", args, "r:", longOptions);
        
        int opt;
        String registryPortStr = "7989";
        while ((opt = getOpt.getopt()) != -1)
        {
            switch(opt)
            {
            case 'r': 
            	registryPortStr = getOpt.getOptarg();
                break;
            }
        }
        int registryPort = Integer.parseInt(registryPortStr);
        DataMoverTargetMain target = new DataMoverTargetMain(registryPort);
        while(true)
        {
        	Thread.sleep(100000);
        }
	}

	public DataMoverTargetMain(int registryPort)
			throws IOException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, NoSuchProviderException, 
			SignatureException, IllegalStateException, CertificateException, UnrecoverableKeyException, AuthenticationFailureException, InterruptedException, AlreadyBoundException
	{
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);
        logger = Logger.getLogger(getClass());
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
        DataMoverSource.init(oidFactory);
        DataMoverReceiver.init(oidFactory);
        
        moverSession = DataMoverSource.getDataMoverSource().createDataMoverSession(securityServer.getEntityID());
        registry = LocateRegistry.createRegistry(registryPort);
        server = new DataMoverTargetImpl(this, moverSession, 0, new IndelibleEntityAuthenticationClientRMIClientSocketFactory(securityServer.getEntityID()), new IndelibleEntityAuthenticationClientRMIServerSocketFactory(securityServer.getEntityID()));
        registry.bind("DataMoverTarget", server);
	}
		
	public static CASIDDataDescriptor generateDescriptor(int length, int sequence)
	{
		byte [] buffer = new byte[length];
		writeTestDataToBuffer(buffer, 0, length, sequence);
		CASIDDataDescriptor returnDescriptor = new CASIDMemoryDataDescriptor(buffer);
		return returnDescriptor;
	}
	
    public static void writeTestDataToBuffer(byte [] buffer, int bufferOffset, int size, int sequence)
    {
    	BitTwiddle.intToJavaByteArray(sequence, buffer, bufferOffset);
    	int bytesRemaining = size - 4;
    	int curBufOffset = bufferOffset;
    	curBufOffset += 4;
    	long curRandom = (sequence << 32) | size;	// We need quick pseudo-random numbers (sequence should reproduce).  This
    												// algorithm is quick and, also, supposed to be quite random
    												// http://javamex.com/tutorials/random_numbers/xorshift.shtml

    	// Not aligned so we have to do it the slow way
    	for (int curByteNum = 0; curByteNum < bytesRemaining; curByteNum+=8)
    	{
    		curRandom ^= (curRandom << 21);
    		curRandom ^= (curRandom >>> 35);
    		curRandom ^= (curRandom << 4);
    		bytesRemaining -= 8;
    		if (bytesRemaining > 8)
    		{
    			BitTwiddle.longToJavaByteArray(curRandom, buffer, curBufOffset+curByteNum);
    		}
    		else
    		{
    			// Handle the less than 1 long space at the end
    			byte [] curRandomBytes = new byte[8];
    			BitTwiddle.longToJavaByteArray(curRandom, curRandomBytes, 0);
    			System.arraycopy(curRandomBytes, 0, buffer, curBufOffset + curByteNum, bytesRemaining);
    		}
    	}
    	return;
    }
    
    public static boolean verifyBuffer(byte [] checkBuffer, SHA1HashID originalHash, int bufferOffset, int checkSize, int sequence)
    {
    	if (BitTwiddle.javaByteArrayToInt(checkBuffer,  bufferOffset) != sequence)
    		return false;	// out of sequence
    	SHA1HashID checkHash = new SHA1HashID();
    	checkHash.update(checkBuffer, bufferOffset, checkSize);
    	checkHash.finalizeHash();
    	if (originalHash.equals(checkHash))
    		return true;
    	return false;
    }
    
}
