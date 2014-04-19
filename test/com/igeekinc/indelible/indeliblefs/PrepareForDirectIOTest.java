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
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.proxies.IndelibleFSServerProxy;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.junitext.iGeekTestCase;
import com.igeekinc.util.MonitoredProperties;

public class PrepareForDirectIOTest extends iGeekTestCase
{
	public void testPrepareForDirectIO() throws Exception
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
		IndelibleFSServerProxy[] servers = new IndelibleFSServerProxy[0];

		while(servers.length == 0)
		{
			servers = IndelibleFSClient.listServers();
			if (servers.length == 0)
				Thread.sleep(1000);
		}
		IndelibleFSServerProxy fsServer = servers[0];

		IndelibleFSClient.connectToServer("share1.igeekinc.com", 50901);
		IndelibleFSServerProxy remoteServer = null;

		while (remoteServer == null)
		{
			servers = IndelibleFSClient.listServers();
			for (IndelibleFSServerProxy checkServer:servers)
			{
				String checkHostName = checkServer.getServerAddress().getHostName();
				if (checkHostName.equals("share1.igeekinc.com") || checkHostName.equals("ec2-54-249-114-82.ap-northeast-1.compute.amazonaws.com"))
				{
					remoteServer = checkServer;
					break;
				}
			}
		}
		
		// TODO - This should be in the IndelibleFSClient or something
		GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
		GeneratorID testBaseID = genIDFactory.createGeneratorID();
		ObjectIDFactory oidFactory = new ObjectIDFactory(testBaseID);
		DataMoverSource.init(oidFactory);
		DataMoverReceiver.init(oidFactory);
		
		CASServerConnectionIF fsCASServer = fsServer.openCASServer();
		CASServerConnectionIF remoteCASServer = remoteServer.openCASServer();
		while (true)
		{
			long start = System.currentTimeMillis();
			System.out.println("PrepareForDirectIO started localhost->share1 at "+(new Date()));
			fsCASServer.prepareForDirectIO(remoteCASServer);
			long elapsed = System.currentTimeMillis() - start;
			System.out.println("PrepareForIO ended localhost->share1 at "+(new Date())+" ("+elapsed+") ms");
			
			start = System.currentTimeMillis();
			System.out.println("PrepareForDirectIO started share1->localhost at "+(new Date()));
			remoteCASServer.prepareForDirectIO(fsCASServer);
			elapsed = System.currentTimeMillis() - start;
			System.out.println("PrepareForIO ended share1->localhost at "+(new Date())+" ("+elapsed+") ms");
		}
	}
}
