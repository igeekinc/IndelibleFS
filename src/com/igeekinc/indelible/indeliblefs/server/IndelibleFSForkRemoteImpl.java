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

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemote;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSForkRemoteImpl extends UnicastRemoteObject implements
        IndelibleFSForkRemote
{
    private static final long serialVersionUID = 5212138386053735499L;
    protected IndelibleFSForkIF localFork;
    IndelibleFSServerConnectionImpl connection;
    Logger logger = Logger.getLogger(getClass());
    protected IndelibleFSForkRemoteImpl(IndelibleFSForkIF coreFork, IndelibleFSServerConnectionImpl connection) throws RemoteException
    {
        super(0, connection.getClientSocketFactory(), connection.getServerSocketFactory());
        this.localFork = coreFork;
        this.connection = connection;
        logger.debug("IndelibleFSForkRemoteImpl created with "+coreFork.getName());
        connection.addReference(this);
    }

    public IndelibleFSForkRemoteImpl(int port, RMIClientSocketFactory csf,
            RMIServerSocketFactory ssf) throws RemoteException
    {
        super(port, csf, ssf);
    }

    public IndelibleFSForkRemoteImpl(int port) throws RemoteException
    {
        super(port);
        // TODO Auto-generated constructor stub
    }

    public void appendDataDescriptor(NetworkDataDescriptor source) throws IOException,
            RemoteException
    {
    	try
    	{
    		localFork.appendDataDescriptor(source);
    	}
    	catch (Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in appendDataDescriptor"), e);
    		throw e;
    	}
    }

    public void flush() throws IOException, RemoteException
    {
    	try
    	{
    		localFork.flush();
    	}
    	catch (Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in flush"), e);
    		throw e;
    	}
    }

    public NetworkDataDescriptor getDataDescriptor(long offset, long length)
            throws IOException, RemoteException
    {
    	try
    	{
    		CASIDDataDescriptor localDescriptor = localFork.getDataDescriptor(offset, length);
    		return connection.registerDataDescriptor(localDescriptor);
    	}
    	catch(Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in getDataDescriptor"), e);
    		throw e;
    	}
    }


    public long length() throws IOException, RemoteException
    {
    	try
    	{
    		return localFork.length();
    	}
    	catch(Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in length"), e);
    		throw e;
    	}
    }

    public void writeDataDescriptor(long offset, NetworkDataDescriptor source)
            throws IOException, RemoteException
    {
    	try
    	{
    		localFork.writeDataDescriptor(offset, source);
    	}
    	catch(Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in writeDataDescriptor"), e);
    		throw e;
    	}
    }

    public long truncate(long truncateLength) throws IOException, RemoteException
    {
    	try
    	{
    		return localFork.truncate(truncateLength);
    	}
    	catch(Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in truncate"), e);
    		throw e;
    	}
    }
    
    public long extend(long extendLength) throws IOException, RemoteException
    {
    	try
    	{
    		return localFork.extend(extendLength);
    	}
    	catch(Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in truncate"), e);
    		throw e;
    	}
    }
    
    public CASIdentifier [] getSegmentIDs() throws IOException, RemoteException
    {
    	try
    	{
    		return localFork.getSegmentIDs();
    	}
    	catch(Error e)
    	{
    		logger.error(new ErrorLogMessage("Caught exception in truncate"), e);
    		throw e;
    	}
    }

	@Override
	public String getName() throws RemoteException
	{
		return localFork.getName();
	}
}
