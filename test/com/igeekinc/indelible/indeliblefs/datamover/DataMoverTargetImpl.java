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

import java.io.IOException;
import java.rmi.RemoteException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateParsingException;

import javax.net.ssl.SSLPeerUnverifiedException;

import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIClientSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.IndelibleEntityAuthenticationClientRMIServerSocketFactory;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastObject;
import com.igeekinc.indelible.indeliblefs.security.SSLUnicastServerRef2;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;

public class DataMoverTargetImpl extends SSLUnicastObject implements DataMoverTargetRemote
{
	private static final long	serialVersionUID	= -8678004932219765992L;
	DataMoverTargetMain main;
	DataMoverSession moverSession;
	protected DataMoverTargetImpl(DataMoverTargetMain main, DataMoverSession moverSession, int port,
			IndelibleEntityAuthenticationClientRMIClientSocketFactory csf,
			IndelibleEntityAuthenticationClientRMIServerSocketFactory ssf)
			throws RemoteException
	{
		super(port, csf, ssf);
		this.main = main;
		this.moverSession = moverSession;
	}

	@Override
	public SessionAuthentication addClient()
			throws SSLPeerUnverifiedException, CertificateParsingException,
			CertificateEncodingException, InvalidKeyException,
			UnrecoverableKeyException, IllegalStateException,
			NoSuchProviderException, NoSuchAlgorithmException,
			SignatureException, KeyStoreException, RemoteException
	{
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getClientEntityAuthenticationForThread();
        }
		return moverSession.addAuthorizedClient(authenticatedID);
	}

	@Override
	public CASIDDataDescriptor pullDescriptor(int length, int sequence)
			throws RemoteException
	{
		CASIDDataDescriptor localDescriptor = main.generateDescriptor(length, sequence);
		NetworkDataDescriptor networkDescriptor = moverSession.registerDataDescriptor(localDescriptor);
		return networkDescriptor;
	}

	@Override
	public void pushDescriptor(CASIDDataDescriptor pushData, int sequence)
			throws IOException, RemoteException
	{
		byte [] data = pushData.getData();
		if (!DataMoverTargetMain.verifyBuffer(data, pushData.getCASIdentifier().getHashID(), 0, data.length, sequence))
		{
			throw new IllegalArgumentException("Buffer "+sequence+" does not verify!");
		}
	}

	public void pushDescriptors(CASIDDataDescriptor [] pushData, int startSequence) throws IOException, RemoteException
	{
		for (int curDataNum = 0; curDataNum < pushData.length; curDataNum++)
		{
			byte [] data = pushData[curDataNum].getData();
			if (!DataMoverTargetMain.verifyBuffer(data, pushData[curDataNum].getCASIdentifier().getHashID(), 0, data.length, startSequence + curDataNum))
			{
				throw new IllegalArgumentException("Buffer "+(startSequence + curDataNum)+" does not verify!");
			}
		}
	}
	
	@Override
	public EntityAuthentication getClientEntityAuthentication()
			throws RemoteException
	{
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getClientEntityAuthenticationForThread();
        }
        return authenticatedID;
	}

	@Override
	public EntityAuthentication getServerEntityAuthentication()
			throws RemoteException
	{
        EntityAuthentication authenticatedID = null;
        if (ref instanceof SSLUnicastServerRef2)
        {
            SSLUnicastServerRef2 ref2 = (SSLUnicastServerRef2)ref;
            authenticatedID = ref2.getServerEntityAuthenticationForThread();
        }
        return authenticatedID;
	}
	@Override
	/**
	 * Add the session authentication from a client so that we can pull data from it
	 */
	public void addClientSessionAuthentication(SessionAuthentication sessionAuthenticationToAdd)
	{
		DataMoverReceiver.getDataMoverReceiver().addSessionAuthentication(sessionAuthenticationToAdd);
	}
}
