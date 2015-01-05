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
 
package com.igeekinc.indelible.indeliblefs.server;

import java.beans.PropertyChangeListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.security.Security;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import com.igeekinc.firehose.AddressFilter;
import com.igeekinc.firehose.FirehoseTarget;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManager;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServer;
import com.igeekinc.indelible.indeliblefs.jmx.IndelibleFSMgmt;
import com.igeekinc.indelible.indeliblefs.security.AuthenticatedTargetSSLSetup;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationFirehoseServer;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServerCore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.firehose.CASServerFirehoseServer;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServer;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.FatalLogMessage;
import com.igeekinc.util.logging.WarnLogMessage;

public class IndelibleFSMain extends IndelibleServer
{
	static
	{
		IndelibleFSServerOIDs.initMapping();
	}
    public static final String kIndelibleEntityAuthenticationServerRootDir = "entityAuthenticationServer";
    CASStoreManager casStoreManager;
    DBCASServer casServer;
    IndelibleFSManager fsManager;
    EntityAuthenticationServerCore localEntityAuthenticationServer;
    EntityAuthenticationFirehoseServer entityAuthenticationNetworkServer;
    private ObjectIDFactory oidFactory;
    @SuppressWarnings("unused")
	private FirehoseTarget indelibleFSServerTarget, casServerTarget;	// We may not use it but we don't want to let go of it!
    private IndelibleFSFirehoseServer indelibleFSServer;		// The Firehose server for IndelibleFS
    private CASServerFirehoseServer casServerFirehoseServer;	// The Firehose server for CASServer
    private int casServerPort = 50904;
    public static void main(String [] args) throws IOException, InterruptedException
    {
    	try
    	{
    		BasicConfigurator.configure();
    		Security.insertProviderAt(new org.bouncycastle.jce.provider.BouncyCastleProvider(), 2);
    		IndelibleServer main = new IndelibleFSMain();
    		Logger.getLogger(IndelibleFSMain.class).error("IndelibleFS initialization finished");
    		// Now, just let the FS server run
    		while (true)
    		{
    			synchronized(main)
    			{
    				main.wait();
    			}
    		}
    	} catch (Throwable t)
    	{
    		Logger.getLogger(IndelibleFSMain.class).fatal("Got unexpected exception", t);
    		t.printStackTrace();
    		System.exit(1);
    	}
    }
    
    public static final long kMinimumDirectMemory = 2L * 1024L * 1024L * 1024L;
    public IndelibleFSMain()
    throws IOException
    {
        logger = Logger.getLogger(getClass());
        try
        {
            EntityAuthenticationClient.startSearchForServers(); // Start looking for other servers out there
            IndelibleServerPreferences.initPreferences(null);
            setServerProperties(IndelibleServerPreferences.getProperties());

            configureLogging(serverProperties);
            
            File preferencesDir = new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName));
            
            GeneratorID generatorID;
            File generatorIDFile = new File(preferencesDir, "com.igeekinc.indelible.server.generatorID");
            if (generatorIDFile.exists())
            {
            	BufferedReader gidReader = new BufferedReader(new FileReader(generatorIDFile));
            	String generatorIDStr = gidReader.readLine().trim();
            	generatorID = new GeneratorID(generatorIDStr);
            }
            else
            {
            	GeneratorIDFactory gidFactory = new GeneratorIDFactory();
            	generatorID = gidFactory.createGeneratorID();
            	BufferedWriter gidWriter = new BufferedWriter(new FileWriter(generatorIDFile));
            	gidWriter.write(generatorID.toString()+"\n");
            	gidWriter.close();
            }
            oidFactory = new ObjectIDFactory(generatorID);
            
            casStoreManager = new CASStoreManager(oidFactory, preferencesDir);
            casStoreManager.initStores();	// Pick up an initialize any stores that were spec'd but not created yet
            logger.warn(new WarnLogMessage("Preferences were loaded from {0}", new Serializable[]{IndelibleServerPreferences.getPreferencesFile()}));
            String dbURL = serverProperties.getProperty(IndelibleServerPreferences.kIndelibleFSCASDBURLPropertyName);
            String dbUser = serverProperties.getProperty(IndelibleServerPreferences.kIndelibleFSCASDBUserPropertyName);
            String dbPassword = serverProperties.getProperty(IndelibleServerPreferences.kIndelibleFSCASDBPasswordPropertyName);
            
            logger.warn(new WarnLogMessage("dbURL = {0}, dbUser = {1}, dbPassword = {2}", new Serializable[]{dbURL, dbUser, dbPassword}));

            casServer = new DBCASServer(dbURL, dbUser, dbPassword, casStoreManager);
            
            File entityAuthenticationRootDir = new File(preferencesDir, kIndelibleEntityAuthenticationServerRootDir);
            
            autoConfigureSecurityServer(entityAuthenticationRootDir, casServer.getOIDFactory());

            if (entityAuthenticationRootDir.exists())
            {
                localEntityAuthenticationServer = new EntityAuthenticationServerCore(entityAuthenticationRootDir);
            }
            else
            {
                localEntityAuthenticationServer = null;  // We are not an entity authentication server - null passed to initializeEntityAuthenticationClient causes it to search for a server
            }

            entityAuthenticationServer = localEntityAuthenticationServer;

            File entityAuthenticationClientKeystoreFile = new File(preferencesDir, IndelibleFSClient.kIndelibleEntityAuthenticationClientConfigFileName);
            
            try
            {
            	configureEntityAuthenticationClient(entityAuthenticationClientKeystoreFile, casServer.getServerID(), null, serverProperties);
            }
            catch (Throwable t)
            {
            	logger.fatal(new FatalLogMessage("Caught unexpected error initializing Entity Authentication Client, exiting..."), t);
            	System.exit(1);
            }

            if (localEntityAuthenticationServer != null)
            {                
            	int registryPort = 0;
                if (serverProperties.getProperty(EntityAuthenticationServer.kEntityAuthenticationServerRandomPortPropertyName, "Y").equals("Y"))
                {
                	logger.error(new ErrorLogMessage("EntityAuthenticationServer starting with random port"));
                	registryPort = 0;
                }
                else
                {
                	String portNumberStr = serverProperties.getProperty(EntityAuthenticationServer.kEntityAuthenticationServerPortNumberPropertyName, Integer.toString(EntityAuthenticationServer.kDefaultEntityAuthenticationServerStaticPort));
                	registryPort = Integer.parseInt(portNumberStr);
                	logger.error(new ErrorLogMessage("EntityAuthenticationServer starting with specified port {0}", new Serializable[]{registryPort}));
                }
				try
				{
					InetSocketAddress serverAddress = new InetSocketAddress(registryPort);
					entityAuthenticationNetworkServer = new EntityAuthenticationFirehoseServer(localEntityAuthenticationServer, serverAddress);
	                EntityAuthenticationClient.getEntityAuthenticationClient().trustServer(localEntityAuthenticationServer);
					int registryPortUsed = entityAuthenticationNetworkServer.getListenAddresses(new AddressFilter()
					{
						
						@Override
						public boolean add(InetSocketAddress checkAddress)
						{
							return (!(checkAddress instanceof AFUNIXSocketAddress));
						}
					})[0].getPort();
					if (bonjour != null)
						bonjour.advertiseEntityAuthenticationServer(registryPortUsed);
					logger.error(new ErrorLogMessage("EntityAuthenticationServer registry port = {0}", new Serializable[]{registryPortUsed}));
				} catch (Exception e)
				{
					logger.error(new ErrorLogMessage("Caught exception initializing"), e);
				}
            }
            
            String moverPortStr = serverProperties.getProperty(IndelibleServerPreferences.kMoverPortPropertyName);
            int moverPort = Integer.parseInt(moverPortStr);
            
            String localPortDirStr = serverProperties.getProperty(IndelibleServerPreferences.kLocalPortDirectory);
            File localPortDir = new File(localPortDirStr);
            if (localPortDir.exists() && !localPortDir.isDirectory())
            {
            	localPortDir.delete();
            }
            if (!localPortDir.exists())
            {
            	localPortDir.mkdirs();
            }
            File localPortSocketFile = new File(localPortDir, "dataMover");
            // Should have the CAS server and the entity authentication server and client configured by this point
            AFUNIXSocketAddress localDataMoverSocketAddr = new AFUNIXSocketAddress(localPortSocketFile, moverPort);
            checkAFUnixSocket(localPortSocketFile, localDataMoverSocketAddr);
            
            DataMoverReceiver.init(casServer.getOIDFactory());
            DataMoverSource.init(casServer.getOIDFactory(), new InetSocketAddress(moverPort),
            		localDataMoverSocketAddr);   // TODO - move this someplace logical
            
            String advertiseMoverPortsStr = serverProperties.getProperty(IndelibleServerPreferences.kAdvertiseMoverAddressesPropertyName);
            if (advertiseMoverPortsStr != null)
            	DataMoverSource.getDataMoverSource().setAdvertiseAddresses(advertiseMoverPortsStr);
            fsManager = new IndelibleFSManager(casServer);
            indelibleFSServer = new IndelibleFSFirehoseServer(fsManager);
            
            AuthenticatedTargetSSLSetup sslSetup = new AuthenticatedTargetSSLSetup();

            
            InetSocketAddress casServerAddress = new InetSocketAddress(casServerPort);
            casServerFirehoseServer = new CASServerFirehoseServer(casServer);
            casServerTarget = new FirehoseTarget(casServerAddress, casServerFirehoseServer, null, sslSetup);
            indelibleFSServer.setCASServerPort(casServerPort);
            
            InetSocketAddress serverAddress = new InetSocketAddress(serverPort);
            indelibleFSServerTarget = new FirehoseTarget(serverAddress, indelibleFSServer, null, sslSetup);

            
            logger.warn(new WarnLogMessage("Starting managment interface"));
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
            ObjectName name = new ObjectName("com.igeekinc.indelible:type=IndelibleFSMgmt"); 
            IndelibleFSMgmt mbean = new IndelibleFSMgmt(this); 
            mbs.registerMBean(mbean, name); 
            logger.warn(new WarnLogMessage("IndelibleServer started on port {0}", new Serializable[]{serverPort}));
        } catch (Throwable t)
        {
            Logger.getLogger(getClass()).fatal(new ErrorLogMessage("Caught exception during IndelibleFS initialization, exiting..."), t);
    		t.printStackTrace();
    		System.exit(1);
        }
    }
    
    public boolean shouldAutoInitEntityAuthenticationServer()
    {
        return serverProperties.getProperty(IndelibleServerPreferences.kAutoInitEntityAuthenticationServerPropertyName).equals("Y");
    }

    public boolean entityAuthenticationServerWasConfigured()
    {
        return serverProperties.getProperty(IndelibleServerPreferences.kEntityAuthenticationServerWasConfigured).equals("Y");
    }
    
    public void setEntityAuthenticationServerConfiguredProperty()
    {
        serverProperties.setProperty(IndelibleServerPreferences.kEntityAuthenticationServerWasConfigured, "Y");
    }
    
    public void storeProperties() throws IOException
    {
        IndelibleServerPreferences.storeProperties();
    }
    
    public String getLogFileDir()
    {
        return serverProperties.getProperty(IndelibleServerPreferences.kLogFileDirectoryPropertyName);
    }
    
    public String getServerLogFileName()
    {
        return "indelibleFSServer.log";
    }
    
	@Override
	public void setupPropertiesListener()
	{
		IndelibleServerPreferences.getProperties().addPropertyChangeListener(new PropertyChangeListener()
		{
			
			@Override
            public void propertyChange(java.beans.PropertyChangeEvent evt) {
                setLogFileLevelFromPrefs();
            };
        });
	}
	

	@Override
	public void setLogFileLevelFromPrefs()
    {
		rollingLog.setThreshold(Level.toLevel(IndelibleServerPreferences.getProperties().getProperty(IndelibleServerPreferences.kVerboseLogFileLevelPropertyName, "INFO"), Level.INFO)); //$NON-NLS-1$
    }

	@Override
	protected boolean shouldCreateRegistry()
	{
		return false;
	}

	@Override
	public File getAdditionalLoggingConfigFile(
			MonitoredProperties serverProperties)
	{
		return new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName),
		        "indeliblefsLoggingOptions.properties");
	}

	public FirehoseTarget getIndelibleFSTarget()
	{
		return indelibleFSServerTarget;
	}

	@Override
	public void dumpServer()
	{
		if (indelibleFSServer != null)
		{
			System.out.println(indelibleFSServer.dump());
		}
		try
		{
			System.out.println(DataMoverSource.getDataMoverSource().dump());
		}
		catch (InternalError e)
		{
			System.out.println("DataMoverSource not initialized\n");
		}
		try
		{
			System.out.println(DataMoverReceiver.getDataMoverReceiver().dump());
		}
		catch (InternalError e)
		{
			System.out.println("DataMoverReceiver not initialized\n");
		}
		
		if (entityAuthenticationNetworkServer != null)
		{
			System.out.println(entityAuthenticationNetworkServer.dump());
		}
	}
	
	
}
