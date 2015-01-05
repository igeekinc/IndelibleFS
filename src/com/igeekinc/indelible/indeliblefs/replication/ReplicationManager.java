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
 
package com.igeekinc.indelible.indeliblefs.replication;

import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSServer;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerAddedEvent;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerListListener;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerRemovedEvent;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServer;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.InfoLogMessage;
import com.igeekinc.util.logging.WarnLogMessage;

public class ReplicationManager extends IndelibleServer implements IndelibleFSServerListListener
{
    public static final String kReplicationManagerMDPropertyName = "com.igeekinc.indeliblefs.ReplicationManagerMD";
    
    // These properties are set in the com.igeekinc.indeliblefs.ReplicationManagerMD (kReplicationManagerMDPropertyName) property set
    
    // List of servers that this volume is replicated to, comma separated
    // Servers are listed by Object ID
    public static final String kVolumeReplicationServersPropertyName = "com.igeekinc.indeliblefs.ReplicationServers";
    protected HashMap<EntityID, ReplicatedServerInfo>replicatedServers = new HashMap<EntityID, ReplicatedServerInfo>();
    protected HashMap<CASCollectionID, HashMap<EntityID, ReplicationVolumeInfo>> replicationInfo = new HashMap<CASCollectionID, HashMap<EntityID, ReplicationVolumeInfo>>();
    protected HashMap<EntityID, CASServerConnectionIF>connectionsForServers = new HashMap<EntityID, CASServerConnectionIF>();
    protected Logger logger;
    protected MonitoredProperties replicationProperties;
    protected String defaultServerAddress;
    protected int defaultServerPort;
    protected IndelibleFSServer[] defaultServer;	// The default server as attached to various authentication servers
    boolean addAll = true;	// Make this a settable property

    public ReplicationManager(MonitoredProperties replicationProperties, MonitoredProperties serverProperties) throws RemoteException
	{
    	this.replicationProperties = replicationProperties;
    	setServerProperties(serverProperties);
    	configureLogging(serverProperties);
    	String defaultServerStr = replicationProperties.getProperty("com.igeekinc.indeliblefs.replication.DefaultServer");
    	if (defaultServerStr != null)
    	{
    		String hostStr = defaultServerStr;
    		int port = 0;
    		try
    		{
    			int colonPos = hostStr.indexOf(":");
    			if (colonPos > 0)
    			{
    				String portStr = hostStr.substring(colonPos + 1);
    				port = Integer.parseInt(portStr);
    				hostStr = hostStr.substring(0, colonPos);
    				defaultServerAddress = hostStr;
    				defaultServerPort = port;
    				IndelibleFSClient.connectToServer(defaultServerAddress, port);
    				boolean connected = false;
    				while (!connected)
    				{
    					InetAddress defaultServerInetAddress = InetAddress.getByName(defaultServerAddress);
    					IndelibleFSServer [] checkServers = IndelibleFSClient.listServers();
    					for (IndelibleFSServer checkServer:checkServers)
    					{
    						if (defaultServerInetAddress.equals(checkServer.getServerAddress()) /* && checkServer.getServerPort() == port // Need to figure out how to make this work later*/)
    						{
    							connected = true;
    							defaultServer = new IndelibleFSServer[]{checkServer};	// Eventually we need to deal with multiple authentication server domains properly
    							break;
    						}
    					}
						if (!connected)
						{
							Logger.getLogger(getClass()).info(new InfoLogMessage("Waiting for default server {0}:{1} to become available", new Serializable[]{defaultServerAddress, (Integer)defaultServerPort}));
							Thread.sleep(10000);
						}
    				}
    				if (defaultServer == null || defaultServer.length == 0)
    					throw new InternalError("Could not connect to default server "+defaultServerAddress+":"+defaultServerPort);
    			}
    		} catch (Throwable e)
    		{
    			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception connecting to Indelible FS server {0}", new Serializable[]{defaultServerStr}), e);
    			throw new InternalError("Could not connect to default server "+defaultServerAddress+":"+defaultServerPort);
    		}
    	}
    	else
    	{
    		defaultServerAddress = null;
    		defaultServerPort = -1;
    	}
    	logger = Logger.getLogger(getClass());
    	IndelibleFSClient.addIndelibleFSServerListListener(this);
    	IndelibleFSServer [] currentServers = IndelibleFSClient.listServers();
		EntityID defaultServerID = null;
		if (defaultServer != null && defaultServer.length > 0)
		{
			try
			{
				defaultServerID = defaultServer[0].getServerID();
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				throw new InternalError("Could not get ID for default server");
			}
		}
		logger.warn(new WarnLogMessage("Adding current servers ({0})", new Serializable[]{currentServers.length}));
    	for (IndelibleFSServer curServer:currentServers)
    	{
    		logger.warn(new WarnLogMessage("Checking {0}", new Serializable[]{curServer.toString()}));

			EntityID curServerID;
			try
			{
				curServerID = curServer.getServerID();
				if (defaultServer != null && defaultServer.length > 0 && !defaultServerID.equals(curServerID))
				{
		    		logger.warn(new WarnLogMessage("Adding {0}",  new Serializable[]{curServer.toString()}));

	    			addServer(curServer);
				}
				else
				{
		    		logger.warn(new WarnLogMessage("{0} is the default server, skipping",  new Serializable[]{curServer.toString()}));
				}
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
    	}
    }

	public void indelibleFSServerAdded(IndelibleFSServerAddedEvent addedEvent)
    {
        IndelibleFSServer addedServer = addedEvent.getAddedServer();
        addServer(addedServer); 
    }

    private synchronized void addServer(IndelibleFSServer addedServer)
    {
        try
        {
        	CASServerConnectionIF serverConn = addedServer.openCASServer();
        	if (replicatedServers.containsKey(serverConn.getServerID()))
        	{
        		serverConn.close();
        		return;
        	}
        	logger.warn("Added server "+serverConn.getServerID());

        	ServerListener serverListener = new ServerListener(addedServer, this, serverConn);
        	serverConn.addListenerAfterID(serverListener, 0);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
    }

    boolean addCollection(EntityID serverID, CASServerConnectionIF serverConn, CASCollectionID curCollectionID, ReplicatedServerInfo serverInfo)
    		throws PermissionDeniedException, IOException, CollectionNotFoundException, AuthenticationFailureException
    {
    	logger.debug("Added collection "+curCollectionID);
    	CASCollectionConnection curCollectionConn = serverConn.openCollectionConnection(curCollectionID);

		Map<String, Serializable>allReplicationProperties = curCollectionConn.getMetaDataResource(kReplicationManagerMDPropertyName);
		if (allReplicationProperties == null)
			allReplicationProperties = new HashMap<String, Serializable>();
		@SuppressWarnings("unchecked")
		Map<String, Serializable>curReplicationProperties = (Map<String, Serializable>) allReplicationProperties.get(serverID.toString());
		if (curReplicationProperties == null && addAll && defaultServer != null && defaultServer.length > 0)
		{
			logger.debug("Replication properties not set, creating");
			curReplicationProperties = new HashMap<String, Serializable>();
			curReplicationProperties.put(kVolumeReplicationServersPropertyName, defaultServer[0].getServerID().toString());
			if (curReplicationProperties instanceof Serializable)
				allReplicationProperties.put(serverID.toString(), (Serializable)curReplicationProperties);
			else
				allReplicationProperties.put(serverID.toString(), new HashMap<String, Serializable>(curReplicationProperties));
			
			curCollectionConn.startTransaction();
			curCollectionConn.setMetaDataResource(kReplicationManagerMDPropertyName, allReplicationProperties);
			curCollectionConn.commit();
		}
		if (curReplicationProperties != null)
		{
			logger.debug("replication properties for "+curCollectionID+" = "+curReplicationProperties);
			HashMap<EntityID, ReplicationVolumeInfo> replicationInfoByServer = replicationInfo.get(curCollectionID);
			if (replicationInfoByServer == null)
			{
				replicationInfoByServer = new HashMap<EntityID, ReplicationVolumeInfo>();
				replicationInfo.put(curCollectionID, replicationInfoByServer);
			}
			ReplicationVolumeInfo replicationVolumeInfo = new ReplicationVolumeInfo(curCollectionID, 
					curCollectionConn, curReplicationProperties);
			EntityID [] replicationServers = replicationVolumeInfo.getReplicationServers();
			if (replicationServers.length > 0)
			{
				CASCollectionConnection replicateToConn = null;
				for (EntityID curReplicationServer:replicationServers)
				{
					if (!curReplicationServer.equals(serverConn.getServerID()))
					{
						CASServerConnectionIF replicateToServerConn = getConnectionForServer(curReplicationServer);
						if (replicateToServerConn != null)
						{
							try
							{
								replicateToConn = replicateToServerConn.openCollectionConnection(curCollectionID);
							} catch (CollectionNotFoundException e)
							{
								// Doesn't exist in the destination, create it
								logger.info("Creating replicated collection "+curCollectionID);
								replicateToConn = replicateToServerConn.addCollection(curCollectionID);
								replicateToConn.startTransaction();
								String [] mdNames = curCollectionConn.listMetaDataNames();
								for (String curMDName:mdNames)
								{
									Map<String, Serializable>curMD = curCollectionConn.getMetaDataResource(curMDName);
									replicateToConn.setMetaDataResource(curMDName, curMD);
								}
								replicateToConn.commit();
							}
							logger.error(new ErrorLogMessage("asking {0} to prepare for direct IO with {1}",
									serverConn.getServerID(), replicateToServerConn.getServerID()
							));
							serverConn.prepareForDirectIO(replicateToServerConn);
							logger.error(new ErrorLogMessage("prepare for direct I/O {0} <-> {1} finished successfully", 
									serverConn.getServerID(), replicateToServerConn.getServerID()
							));
							/*
							replicateToServerConn.addConnectedServer(serverConn.getServerID(), serverConn.getSecurityServerID());
							replicateToServerConn.addClientSessionAuthentication(serverConn.getSessionAuthentication());
							serverConn.addClientSessionAuthentication(replicateToServerConn.getSessionAuthentication());
							*/
						}
						else
						{
							logger.error(new ErrorLogMessage("Could not open connection to {0}", curReplicationServer));
						}
					}
				}
				if (replicateToConn != null)
				{
					serverInfo.addReplicatedVolume(curCollectionID);
					long startingID = replicateToConn.getLastReplicatedEventID(serverConn.getServerID(), curCollectionID) + 1;
					replicationInfoByServer.put(serverID, replicationVolumeInfo);
					logger.error(new ErrorLogMessage("Starting listening for collection {0}, starting event = {1}", curCollectionID, startingID));
					curCollectionConn.addListenerAfterID(new CollectionListener(curCollectionConn, replicateToConn, startingID), startingID);
					return true;
				}
			}
		}
		return false;
    }
    private CASServerConnectionIF getConnectionForServer(EntityID connectServer)
    {
    	synchronized(connectionsForServers)
    	{
    		CASServerConnectionIF returnConnection = connectionsForServers.get(connectServer);

    		if (returnConnection == null)
    		{
    			IndelibleFSServer [] servers = IndelibleFSClient.listServers();
    			for (IndelibleFSServer checkServer:servers)
    			{
    				try
    				{
    					if (checkServer.getServerID().equals(connectServer))
    					{
    						returnConnection = checkServer.openCASServer();
    						if (returnConnection.getServerID().equals(connectServer))
    						{
    		    				connectionsForServers.put(returnConnection.getServerID(), returnConnection);
    							break;
    						}
    						returnConnection.close();
    						returnConnection = null;
    					}
    				} catch (IOException e)
    				{
    					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    				} catch (PermissionDeniedException e)
    				{
    					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
    				}
    			}
    		}
    		return returnConnection;
    	}
    }

	private EntityID getEntityID()
	{
		return EntityAuthenticationClient.getEntityAuthenticationClient().getEntityID();
	}

	public synchronized void indelibleFSServerRemoved(IndelibleFSServerRemovedEvent removedEvent)
    {
		IndelibleFSServer removedServer = removedEvent.getRemovedServer();
        if (replicatedServers.containsKey(removedServer))
        {
            ReplicatedServerInfo removeServerInfo;
			try
			{
				removeServerInfo = replicatedServers.get(removedServer.getServerID());
	            IndelibleFSObjectID [] removeIDs = removeServerInfo.listReplicatedVolumes();
	            for (IndelibleFSObjectID curRemoveID:removeIDs)
	            {
	                replicationInfo.remove(curRemoveID);
	            }
			} catch (IOException e)
			{
				logger.error(new ErrorLogMessage("Got remote exception retrieving server ID"));
			}
        }
    }
    
    public void runLoop()
    {
        while (true)
        {
            try
            {
                Thread.sleep(100000);
            } catch (InterruptedException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            }
        }
    }

    public static final String kReplicationManagerEntityAuthenticationClientConfigFileName = "replicationManager-entityAuthenticationClientInfo";

    public static void main(String [] args) throws IOException, UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, AuthenticationFailureException, InterruptedException
    {
        BasicConfigurator.configure();
        IndelibleServerPreferences.initPreferences(null);
        MonitoredProperties serverProperties = IndelibleServerPreferences.getProperties();
        GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
        GeneratorID testBaseID = genIDFactory.createGeneratorID();
        ObjectIDFactory oidFactory = new ObjectIDFactory(testBaseID);
        
        File preferencesDir = new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName));
        File securityClientKeystoreFile = new File(preferencesDir, kReplicationManagerEntityAuthenticationClientConfigFileName);
        EntityAuthenticationClient.initializeEntityAuthenticationClient(securityClientKeystoreFile, oidFactory, serverProperties);
        EntityAuthenticationClient.startSearchForServers();
        EntityAuthenticationServer [] securityServers = new EntityAuthenticationServer[0];
        while(securityServers.length == 0)
        {
            securityServers = EntityAuthenticationClient.listEntityAuthenticationServers();
            if (securityServers.length == 0)
                Thread.sleep(1000);
        }
        
        IndelibleFSClient.start(null, serverProperties);

        DataMoverReceiver.init(oidFactory);
        DataMoverSource.init(oidFactory, new InetSocketAddress(0), null);
        
        File replicationPreferencesFile = new File(preferencesDir, "com.igeekinc.indelible.replicationserver.properties");
        MonitoredProperties replicationProperties = new MonitoredProperties(null);;
        if (replicationPreferencesFile.exists())
        	replicationProperties.load(new FileInputStream(replicationPreferencesFile));
        ReplicationManager replicationManager = new ReplicationManager(replicationProperties, serverProperties);
        replicationManager.runLoop();
    }

	@Override
    public String getServerLogFileName()
    {
        return "indelibleReplicationManager.log";
    }

	@Override
    public String getLogFileDir()
    {
        return serverProperties.getProperty(IndelibleServerPreferences.kLogFileDirectoryPropertyName);
    }

	@Override
	protected void storeProperties() throws IOException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void setEntityAuthenticationServerConfiguredProperty()
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean shouldAutoInitEntityAuthenticationServer()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean entityAuthenticationServerWasConfigured()
	{
		// TODO Auto-generated method stub
		return false;
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
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public File getAdditionalLoggingConfigFile(
			MonitoredProperties serverProperties)
	{
		return new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName),
		        "replicationMgrLoggingOptions.properties");
	}
}
