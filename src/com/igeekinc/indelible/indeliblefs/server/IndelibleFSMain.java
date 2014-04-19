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
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.security.Security;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import sun.rmi.registry.RegistryImpl;
import sun.rmi.server.UnicastServerRef;

import com.igeekinc.indelible.indeliblefs.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManager;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.security.AuthenticatedServerImpl;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServerCore;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServerImpl;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.server.RemoteCASServerImpl;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServer;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.FatalLogMessage;
import com.igeekinc.util.logging.InfoLogMessage;
import com.igeekinc.util.logging.WarnLogMessage;
import com.igeekinc.util.remote.MultiHomeRMIClientSocketFactory;

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
    IndelibleFSServerImpl fsServerImpl;
    RemoteCASServerImpl casServerImpl;
    EntityAuthenticationServerCore localEntityAuthenticationServer;
    private Registry entityAuthenticationRegistry;
    private ObjectIDFactory oidFactory;
    
    public static void main(String [] args) throws IOException, AlreadyBoundException, InterruptedException
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
    throws IOException, AlreadyBoundException
    {
        logger = Logger.getLogger(getClass());
        try
        {
            EntityAuthenticationClient.startSearchForServers(); // Start looking for other servers out there
            IndelibleServerPreferences.initPreferences(null);
            setServerProperties(IndelibleServerPreferences.getProperties());

            configureLogging(serverProperties);
            
            if (sun.misc.VM.maxDirectMemory() < kMinimumDirectMemory)
            {
            	logger.fatal("Not enough directory memory allocated - minimum = "+kMinimumDirectMemory+" current = "+sun.misc.VM.maxDirectMemory());
            }
            else
            {
            	logger.info(new InfoLogMessage("Starting with direct memory {0}", new Serializable[]{sun.misc.VM.maxDirectMemory()}));
            }
            logger.warn(new WarnLogMessage("IndelibleServer registry started on port {0}", new Serializable[]{((UnicastServerRef)((RegistryImpl)serverRegistry).getRef()).getLiveRef().getPort()}));
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
            // Entity authentication server is bound without SSL for the moment.
            if (localEntityAuthenticationServer != null)
            {
                EntityAuthenticationClient.getEntityAuthenticationClient().trustServer(localEntityAuthenticationServer);
                EntityAuthenticationServerImpl entityAuthenticationServerImpl;
            	entityAuthenticationServerImpl = new EntityAuthenticationServerImpl(localEntityAuthenticationServer, 0, new MultiHomeRMIClientSocketFactory(), RMISocketFactory.getDefaultSocketFactory());
            	int registryPort = 0;
                if (serverProperties.getProperty(EntityAuthenticationServer.kEntityAuthenticationServerRandomPortPropertyName, "Y").equals("Y"))
                {
                	logger.error(new ErrorLogMessage("EntityAuthenticationServer starting with random registry port"));
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
					entityAuthenticationRegistry = LocateRegistry.createRegistry(registryPort);
					entityAuthenticationRegistry.bind(EntityAuthenticationServer.kIndelibleEntityAuthenticationServerRMIName, entityAuthenticationServerImpl);
					int registryPortUsed = ((UnicastServerRef)((RegistryImpl)entityAuthenticationRegistry).getRef()).getLiveRef().getPort();
					if (bonjour != null)
						bonjour.advertiseEntityAuthenticationServer(registryPortUsed);
					logger.error(new ErrorLogMessage("EntityAuthenticationServer registry port = {0}", new Serializable[]{registryPortUsed}));
				} catch (Exception e)
				{
					logger.error(new ErrorLogMessage("Caught exception initializing"), e);
				}
            }
            
            // Should have the CAS server and the entity authentication server and client configured by this point
            DataMoverSource.init(casServer.getOIDFactory());   // TODO - move this someplace logical
            DataMoverReceiver.init(casServer.getOIDFactory());
            
            fsManager = new IndelibleFSManager(casServer);
            for (EntityAuthenticationServer bindServer:EntityAuthenticationClient.getEntityAuthenticationClient().listTrustedServers())
            {
                setupFSManagerForEntityAuthenticationServer(bindServer);
            }
        } catch (Throwable t)
        {
            Logger.getLogger(getClass()).fatal(new ErrorLogMessage("Caught exception during IndelibleFS initialization, exiting..."), t);
        }
    }
    
    @SuppressWarnings("unchecked")
	void setupFSManagerForEntityAuthenticationServer(EntityAuthenticationServer curServer) throws RemoteException
    {
        EntityID entityAuthenticationServerID = curServer.getEntityID();
        fsServerImpl = new IndelibleFSServerImpl(fsManager, 0, new IndelibleEntityAuthenticationClientRMIClientSocketFactory(entityAuthenticationServerID), new IndelibleEntityAuthenticationClientRMIServerSocketFactory(entityAuthenticationServerID));
        casServerImpl = new RemoteCASServerImpl(casServer, 0, new IndelibleEntityAuthenticationClientRMIClientSocketFactory(entityAuthenticationServerID), new IndelibleEntityAuthenticationClientRMIServerSocketFactory(entityAuthenticationServerID));
        fsServerImpl.setCASServer(casServerImpl);
        AuthenticatedServerImpl<IndelibleFSServerImpl> fsServer = null;
		try
		{
			fsServer = (AuthenticatedServerImpl<IndelibleFSServerImpl>) serverRegistry.lookup(IndelibleFSClient.kIndelibleFSServerRMIName);
		} catch (NotBoundException e)
		{
			// Not a problem
		}
        boolean needsBinding = false;
        if (fsServer == null)
        {
        	fsServer = new AuthenticatedServerImpl<IndelibleFSServerImpl>(0, new MultiHomeRMIClientSocketFactory(), RMISocketFactory.getDefaultSocketFactory());
        	needsBinding = true;
        }
        fsServer.addServer(entityAuthenticationServerID, fsServerImpl);
        if (needsBinding)
        {
        	serverRegistry.rebind(IndelibleFSClient.kIndelibleFSServerRMIName, fsServer);
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
        if (IndelibleServerPreferences.getProperties().getProperty(IndelibleServerPreferences.kCreateVerboseLogFilesPropertyName, "N").equals("N")) //$NON-NLS-1$ //$NON-NLS-2$
            rollingLog.setThreshold(Level.INFO);
        else
            rollingLog.setThreshold(Level.toLevel(IndelibleServerPreferences.getProperties().getProperty(IndelibleServerPreferences.kVerboseLogFileLevelPropertyName, "INFO"), Level.INFO)); //$NON-NLS-1$
    }

	@Override
	protected boolean shouldCreateRegistry()
	{
		return true;
	}
}
