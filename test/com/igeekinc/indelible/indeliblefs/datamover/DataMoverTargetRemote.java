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
import java.rmi.Remote;
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
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;

public interface DataMoverTargetRemote extends Remote
{
	public SessionAuthentication addClient() throws SSLPeerUnverifiedException, CertificateParsingException, CertificateEncodingException, InvalidKeyException, UnrecoverableKeyException, IllegalStateException, NoSuchProviderException, NoSuchAlgorithmException, SignatureException, KeyStoreException, RemoteException;
	
	/**
	 * Pulls a descriptor from the target
	 * @param length
	 * @param sequence
	 * @return
	 * @throws RemoteException
	 */
	public CASIDDataDescriptor pullDescriptor(int length, int sequence) throws RemoteException;
	
	/**
	 * Pushes (writes) a descriptor to the target
	 * @param pushData
	 * @param sequence
	 * @throws RemoteException
	 * @throws IOException 
	 */
	public void pushDescriptor(CASIDDataDescriptor pushData, int sequence) throws IOException, RemoteException;
	
	public void pushDescriptors(CASIDDataDescriptor [] pushData, int startSequence) throws IOException, RemoteException;
	
	public EntityAuthentication getClientEntityAuthentication() throws RemoteException;

	public EntityAuthentication getServerEntityAuthentication() throws RemoteException;
	
	/**
	 * Add the session authentication from a client so that we can pull data from it
	 */
	public void addClientSessionAuthentication(SessionAuthentication sessionAuthenticationToAdd) throws RemoteException;
}
