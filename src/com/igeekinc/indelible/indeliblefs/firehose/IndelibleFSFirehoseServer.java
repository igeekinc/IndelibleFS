/*
 * Copyright 2002-2014 iGeek, Inc.
 * All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.igeekinc.indelible.indeliblefs.firehose;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.firehose.CommandMessage;
import com.igeekinc.firehose.CommandResult;
import com.igeekinc.firehose.CommandToProcess;
import com.igeekinc.firehose.FirehoseChannel;
import com.igeekinc.firehose.FirehoseTarget;
import com.igeekinc.firehose.SSLFirehoseChannel;
import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.CreateSymlinkInfo;
import com.igeekinc.indelible.indeliblefs.DeleteFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSObjectIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleNodeInfo;
import com.igeekinc.indelible.indeliblefs.MoveObjectInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManager;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSManagerConnection;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotInfo;
import com.igeekinc.indelible.indeliblefs.core.IndelibleSnapshotIterator;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.datamover.msgpack.NetworkDataDescriptorMsgPack;
import com.igeekinc.indelible.indeliblefs.datamover.msgpack.SessionAuthenticationMsgPack;
import com.igeekinc.indelible.indeliblefs.exceptions.CannotDeleteDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotDirectoryException;
import com.igeekinc.indelible.indeliblefs.exceptions.NotFileException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.CreateFileInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.CreateSymlinkInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.DeleteFileInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.GetChildNodeInfoReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.GetMetaDataResourceReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.GetMoverAddressesReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.GetSegmentIDsReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSClientInfoIF;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSCommandMessage;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSFirehoseServerIF;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSObjectHandleMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleFSObjectIDMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleNodeInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleSnapshotInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleVersionMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.ListDirectoryReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.ListMetaDataResourcesReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.ListSnapshotsReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.ListVersionsReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.ListVolumesReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.MoveObjectInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.NextSnapshotListItemsReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.NextVersionListItemsReply;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.security.remote.msgpack.EntityAuthenticationMsgPack;
import com.igeekinc.indelible.indeliblefs.security.remote.msgpack.EntityIDMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSFirehoseServer extends AuthenticatedFirehoseServer<IndelibleFSClientInfo> 
implements IndelibleFSFirehoseServerIF, AsyncCompletion<CommandResult, CommandToProcess>
{
	static
	{
		// Load the map and fail early if there's a mistake
		for (IndelibleFSServerCommand checkCommand:IndelibleFSServerCommand.values())
		{
			if (checkCommand != IndelibleFSServerCommand.kIllegalCommand)
				IndelibleFSFirehoseClient.getReturnClassForCommandCodeStatic(checkCommand.commandNum);	// This exercises both getReturnClassForCommandCodeStatic and getClassForCommandCode
		}
	}
	private int threadNum = 0;
	private IndelibleFSManager localServer;
	private int casServerPort;
	
	public IndelibleFSFirehoseServer(IndelibleFSManager localServer)
	{
		this.localServer = localServer;
	}

	public void setCASServerPort(int casServerPort)
	{
		this.casServerPort = casServerPort;
	}
	
	@Override
	public Thread createSelectLoopThread(Runnable selectLoopRunnable)
	{
		Thread returnThread = new Thread(selectLoopRunnable, "Indelible FS Server "+threadNum);
		threadNum++;
		return returnThread;
	}

	@Override
	protected Class<? extends CommandMessage> getClassForCommandCode(int commandCode)
	{
		return IndelibleFSFirehoseClient.getClassForCommandCode(commandCode);
	}

	@Override
	protected IndelibleFSClientInfo createClientInfo(FirehoseChannel channel)
	{
		try
		{
			return new IndelibleFSClientInfo((SSLFirehoseChannel)channel);
		} catch (SecurityException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		} catch (AuthenticationFailureException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		throw new IllegalArgumentException("Security exception");
	}

	@Override
	protected void processCommand(IndelibleFSClientInfo clientInfo,
			CommandToProcess commandToProcessBlock) throws Exception
	{
		/**
		 * OK, rather than having the switch statement from hell, IndelibleFSCommandMessages know how to call into IndelibleFSFirehoseServerIF (defined in
		 * IndelibleFSClient).  Note that Firehose never moves code between JVMs so that the execute will always be the code that was present in the
		 * server, not the code on the client.
		 */
		IndelibleFSCommandMessage<?, ?, ?> commandToProcess = (IndelibleFSCommandMessage<?, ?, ?>) commandToProcessBlock.getCommandToProcess();
		logger.debug("Executing command "+commandToProcess.getCommand());
		String originalName = Thread.currentThread().getName();
		try
		{
			Thread.currentThread().setName("IndelibleFSServer "+commandToProcess.getCommand());
			commandToProcess.executeAsync(this, clientInfo, this, commandToProcessBlock);
		}
		finally
		{
			Thread.currentThread().setName(originalName);
		}
	}

	@Override
	protected Class<? extends Object> getReturnClassForCommandCode(int commandCode)
	{
		return IndelibleFSFirehoseClient.getReturnClassForCommandCodeStatic(commandCode);
	}


	@Override
	public int getExtendedErrorCodeForThrowable(Throwable t)
	{
		return IndelibleFSFirehoseClient.getExtendedErrorCodeForThrowableStatic(t);
	}

	@Override
	public Throwable getExtendedThrowableForErrorCode(int errorCode)
	{
		return IndelibleFSFirehoseClient.getExtendedThrowableForErrorCodeStatic(errorCode);
	}

	/*
	 * Server methods - these are called by IndelibleFSCommandMessage.execute in processCommand 
	 */
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#addClientSessionAuthentication(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo, com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle, com.igeekinc.indelible.indeliblefs.security.SessionAuthentication)
	 */
	@Override
	public CommandResult addClientSessionAuthentication(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connection,
			SessionAuthentication sessionAuthenticationToAdd) throws IOException
	{
		if (getConnectionForHandle(clientInfo, connection) == null)
			throw new IOException("Connection closed");
		DataMoverReceiver.getDataMoverReceiver().addSessionAuthentication(sessionAuthenticationToAdd);
		return new CommandResult(0, null);
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#getSessionAuthentication(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo, com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	@Override
	public CommandResult getSessionAuthentication(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle) throws IOException
	{
		IndelibleFSManagerConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle).getManagerConnection();
		if (localConnection == null)
			throw new IOException("Connection closed");
		SessionAuthentication returnAuthentication = localConnection.getSessionAuthentication();
		SessionAuthenticationMsgPack returnAuthenticationMsgPack = null;
		if (returnAuthentication != null)
			returnAuthenticationMsgPack = new SessionAuthenticationMsgPack(returnAuthentication);
		return new CommandResult(0, returnAuthenticationMsgPack);
	}

	private IndelibleFSClientConnection getConnectionForHandle(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle)
	{
		return ((IndelibleFSClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#openConnection(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo)
	 */
	@Override
	public CommandResult openConnection(IndelibleFSClientInfoIF clientInfo) throws IOException
	{
		logger.error("Got open connection");
		IndelibleFSManagerConnection connection = localServer.open(((IndelibleFSClientInfo)clientInfo).getClientEntityAuthentication());
		IndelibleFSServerConnectionHandle connectionHandle = ((IndelibleFSClientInfo)clientInfo).createConnectionHandle(connection);
		return new CommandResult(0, connectionHandle);
	}
	
	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#closeConnection(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo, com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	@Override
	public CommandResult closeConnection(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle handle) throws IOException
	{
		IndelibleFSClientConnection connection = ((IndelibleFSClientInfo)clientInfo).removeConnectionHandle(handle);
		if (handle != null)
		{
			connection.close();
		}
		return new CommandResult(0, null);
	}
	
	public CommandResult testReverseConnection(IndelibleFSClientInfoIF clientInfo,
			NetworkDataDescriptorMsgPack descriptor) throws IOException
	{
		NetworkDataDescriptor testDescriptor = descriptor.getNetworkDataDescriptor();
		testDescriptor.getData();
		return new CommandResult(0, null);
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#getMoverAddresses(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo, com.igeekinc.indelible.oid.EntityID)
	 */
	@Override
	public CommandResult getMoverAddresses(IndelibleFSClientInfoIF clientInfo, EntityID securityServerID)
	{
		InetSocketAddress [] moverAddresses = DataMoverSource.getDataMoverSource().getListenNetworkAddresses(securityServerID);
		GetMoverAddressesReply getMoverAddressesReply = new GetMoverAddressesReply(moverAddresses);
		return new CommandResult(0, getMoverAddressesReply);
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#getClientEntityAuthentication(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo, com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	@Override
	public CommandResult getClientEntityAuthentication(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		return new CommandResult(0, new EntityAuthenticationMsgPack(((IndelibleFSClientInfo)clientInfo).getClientEntityAuthentication()));
	}

	/* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseServerIF#listVolumes(com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClientInfo, com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerConnectionHandle)
	 */
	@Override
	public CommandResult listVolumes(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSObjectID [] volumeList = localConnection.getManagerConnection().listVolumes();
		ListVolumesReply reply = new ListVolumesReply(volumeList);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult retrieveVolume(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectIDMsgPack retrieveVolumeID) throws IOException, VolumeNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF localVolume = localConnection.getManagerConnection().retrieveVolume(retrieveVolumeID.getIndelibleFSObjectID());
		IndelibleFSObjectHandle returnHandle =  localConnection.createHandle(localVolume);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(returnHandle));
	}

	@Override
	public CommandResult getMetaDataResource(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectHandle objectHandle, String mdResourceName) throws IOException,
			PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSObjectIF localObject = localConnection.getObjectForHandle(objectHandle);
		Map<String, Object> metaDataResource = localObject.getMetaDataResource(mdResourceName);
		return new CommandResult(0, new GetMetaDataResourceReply(metaDataResource));
	}

	@Override
	public CommandResult listMetaDataResources(
			IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle, IndelibleFSObjectHandle objectHandle)
			throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSObjectIF localObject = localConnection.getObjectForHandle(objectHandle);
		ListMetaDataResourcesReply reply = new ListMetaDataResourcesReply(localObject.listMetaDataResources());
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult releaseHandle(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectHandle [] handles)
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection != null)
		{
			for (IndelibleFSObjectHandle curHandle:handles)
				localConnection.removeObjectForHandle(curHandle);
		}
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult getRoot(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSVolumeHandle volumeHandle) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSVolumeIF volume = (IndelibleFSVolumeIF) localConnection.getObjectForHandle(volumeHandle);
		IndelibleDirectoryNodeIF root = volume.getRoot();
		IndelibleFSObjectHandle rootHandle = localConnection.createHandle(root);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(rootHandle));
	}

	@Override
	public CommandResult getObjectByPath(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle, IndelibleFSVolumeHandle volumeHandle, FilePath path)
			throws IOException, PermissionDeniedException, ObjectNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSVolumeIF volume = (IndelibleFSVolumeIF) localConnection.getObjectForHandle(volumeHandle);
		IndelibleFileNodeIF fileObject = volume.getObjectByPath(path);
		IndelibleFSObjectHandle objectHandle = localConnection.createHandle(fileObject);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(objectHandle));
	}

	@Override
	public CommandResult getObjectByID(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle, 
			IndelibleFSVolumeHandle volumeHandle, IndelibleFSObjectIDMsgPack id)
			throws IOException, PermissionDeniedException,
			ObjectNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSVolumeIF volume = (IndelibleFSVolumeIF) localConnection.getObjectForHandle(volumeHandle);
		IndelibleFileNodeIF fileObject = volume.getObjectByID(id.getIndelibleFSObjectID());
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(localConnection.createHandle(fileObject)));
	}

	@Override
	public CommandResult getObjectByVersionAndID(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSVolumeHandle volumeHandle, IndelibleFSObjectIDMsgPack id,
			IndelibleVersionMsgPack version, RetrieveVersionFlags flags) throws IOException,
			PermissionDeniedException, ObjectNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSVolumeIF volume = (IndelibleFSVolumeIF) localConnection.getObjectForHandle(volumeHandle);
		IndelibleFileNodeIF fileObject = volume.getObjectByID(id.getIndelibleFSObjectID(), version.getIndelibleVersion(), flags);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(localConnection.createHandle(fileObject)));
	}

	@Override
	public CommandResult createChildFile(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, String name, boolean exclusive)
			throws IOException, PermissionDeniedException, FileExistsException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = (IndelibleDirectoryNodeIF)localConnection.getObjectForHandle(parentHandle);
		CreateFileInfo createInfo = parent.createChildFile(name, exclusive);
		CreateFileInfoMsgPack reply = new CreateFileInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(createInfo.getDirectoryNode()), 
				(IndelibleFSFileHandle)localConnection.createHandle(createInfo.getCreatedNode()));
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult createChildFileWithInitialData(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, 
			String name,
			Map<String, CASIDDataDescriptor> initialDataMap,
			boolean exclusive) throws IOException, PermissionDeniedException,
			FileExistsException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = (IndelibleDirectoryNodeIF)localConnection.getObjectForHandle(parentHandle);
		CreateFileInfo createInfo = parent.createChildFile(name,initialDataMap, exclusive);
		CreateFileInfoMsgPack reply = new CreateFileInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(createInfo.getDirectoryNode()), 
				(IndelibleFSFileHandle)localConnection.createHandle(createInfo.getCreatedNode()));
		return new CommandResult(0, reply);
	}
	

	@Override
	public CommandResult createChildFileFromExistingFile(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, String name,
			IndelibleFSFileHandle source, boolean exclusive)
			throws IOException, PermissionDeniedException, FileExistsException, NotFileException, ObjectNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = localConnection.getObjectForHandle(parentHandle);
		IndelibleFileNodeIF sourceFile = localConnection.getObjectForHandle(source);
		CreateFileInfo createInfo = parent.createChildFile(name, sourceFile, exclusive);
		CreateFileInfoMsgPack reply = new CreateFileInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(createInfo.getDirectoryNode()), 
				(IndelibleFSFileHandle)localConnection.createHandle(createInfo.getCreatedNode()));
		return new CommandResult(0, reply);
	}
	

	@Override
	public CommandResult createChildSymlink(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, String name,
			String targetPath, boolean exclusive) throws IOException, PermissionDeniedException, FileExistsException, ObjectNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = localConnection.getObjectForHandle(parentHandle);
		CreateSymlinkInfo createInfo = parent.createChildSymlink(name, targetPath, exclusive);
		CreateSymlinkInfoMsgPack reply = new CreateSymlinkInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(createInfo.getDirectoryNode()), 
				(IndelibleFSSymbolicLinkHandle)localConnection.createHandle(createInfo.getCreatedNode()));
		return new CommandResult(0, reply);
	}
	
	@Override
	public CommandResult listDirectory(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle directoryHandle) throws IOException,
			PermissionDeniedException
	{
		Log4JStopWatch callWatch = new Log4JStopWatch("listDirectory");
		try
		{
			IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
			if (localConnection == null)
				throw new IOException("Connection closed");	
			IndelibleDirectoryNodeIF localObject = localConnection.getObjectForHandle(directoryHandle);
			String[] childNames = localObject.list();
			byte [] namesBuffer = packer.write(childNames);
			CASIDMemoryDataDescriptor returnLocalDescriptor = new CASIDMemoryDataDescriptor(namesBuffer);
			NetworkDataDescriptor returnNetworkDescriptor = getConnectionForHandle(clientInfo, connectionHandle).getManagerConnection().registerDataDescriptor(returnLocalDescriptor);
			ListDirectoryReply reply = new ListDirectoryReply(childNames.length, returnNetworkDescriptor);
			return new CommandResult(0, reply);
		}
		finally
		{
			callWatch.stop();
		}
	}

	
	
	@Override
	public CommandResult getNumChildren(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle directoryHandle) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleDirectoryNodeIF localObject = localConnection.getObjectForHandle(directoryHandle);
		Integer returnNumChildren = localObject.getNumChildren();
		CommandResult commandResult = new CommandResult(0, returnNumChildren);
		return commandResult;
	}

	@Override
	public CommandResult getChildNode(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle directoryHandle, String name)
			throws IOException, PermissionDeniedException,
			ObjectNotFoundException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleDirectoryNodeIF localObject = localConnection.getObjectForHandle(directoryHandle);
		IndelibleFileNodeIF child = localObject.getChildNode(name);
		IndelibleFSFileHandle childHandle = localConnection.createHandle(child);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(childHandle));
	}

	@Override
	public CommandResult totalLength(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSFileHandle fileHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFileNodeIF localObject = localConnection.getObjectForHandle(fileHandle);
		long totalLength = localObject.totalLength();
		return new CommandResult(0, totalLength);
	}

	@Override
	public CommandResult getFork(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSFileHandle fileHandle, String forkName, boolean createIfNecessary)
			throws IOException, ForkNotFoundException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFileNodeIF localObject = localConnection.getObjectForHandle(fileHandle);
		IndelibleFSForkIF localFork = localObject.getFork(forkName, createIfNecessary);
		IndelibleFSForkHandle forkHandle = localConnection.createHandle(localFork);
		return new CommandResult(0, forkHandle);
	}

	@Override
	public CommandResult appendDataDescriptor(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle,
			NetworkDataDescriptor networkDataDescriptor) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		localFork.appendDataDescriptor(networkDataDescriptor);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult writeDataDescriptor(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle, long offset,
			NetworkDataDescriptor networkDataDescriptor) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		localFork.writeDataDescriptor(offset, networkDataDescriptor);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult readDataDescriptor(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle, long offset, long length) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		CASIDDataDescriptor returnLocalDescriptor = localFork.getDataDescriptor(offset, length);
		NetworkDataDescriptor returnDescriptor = getConnectionForHandle(clientInfo, connectionHandle).getManagerConnection().registerDataDescriptor(returnLocalDescriptor);
		return new CommandResult(0, new NetworkDataDescriptorMsgPack (returnDescriptor));
	}

	@Override
	public CommandResult startTransaction(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		localConnection.getManagerConnection().startTransaction();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult commitTransaction(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleVersion commitVersion = localConnection.getManagerConnection().commit();
		return new CommandResult(0, new IndelibleVersionMsgPack(commitVersion));
	}

	@Override
	public CommandResult commitTransactionAndSnapshot(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			HashMap<String, Serializable> snapshotMetadata) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleVersion commitVersion = localConnection.getManagerConnection().commitAndSnapshot(snapshotMetadata);
		return new CommandResult(0,  new IndelibleVersionMsgPack(commitVersion));
	}

	@Override
	public CommandResult rollbackTransaction(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		localConnection.getManagerConnection().rollback();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult inTransaction(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		return new CommandResult(0, localConnection.getManagerConnection().inTransaction());
	}

	@Override
	public CommandResult createVolume(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			Properties volumeProperties) throws IOException,
			PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSVolumeIF createVolume = localConnection.getManagerConnection().createVolume(volumeProperties);
		IndelibleFSVolumeHandle createHandle = localConnection.createHandle(createVolume);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(createHandle));
	}

	@Override
	public CommandResult deleteVolume(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectIDMsgPack deleteVolumeID) throws VolumeNotFoundException, PermissionDeniedException, IOException 
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		localConnection.getManagerConnection().deleteVolume(deleteVolumeID.getIndelibleFSObjectID());
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult truncate(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle, long truncateLength)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		long result = localFork.truncate(truncateLength);
		return new CommandResult(0, result);
	}

	@Override
	public CommandResult flush(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		localFork.flush();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult createChildDirectory(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, String name)
			throws IOException, PermissionDeniedException, FileExistsException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = (IndelibleDirectoryNodeIF)localConnection.getObjectForHandle(parentHandle);
		CreateFileInfo createInfo = parent.createChildDirectory(name);
		CreateFileInfoMsgPack reply = new CreateFileInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(createInfo.getDirectoryNode()), 
				(IndelibleFSDirectoryHandle)localConnection.createHandle(createInfo.getCreatedNode()));
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult deleteChildFile(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, String name)
			throws IOException, PermissionDeniedException,
			CannotDeleteDirectoryException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = (IndelibleDirectoryNodeIF)localConnection.getObjectForHandle(parentHandle);
		DeleteFileInfo deleteInfo = parent.deleteChild(name);
		DeleteFileInfoMsgPack reply = new DeleteFileInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(deleteInfo.getDirectoryNode()), deleteInfo.deleteSucceeded());
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult deleteChildDirectory(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle parentHandle, String name)
			throws IOException, PermissionDeniedException,
			NotDirectoryException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = (IndelibleDirectoryNodeIF)localConnection.getObjectForHandle(parentHandle);
		DeleteFileInfo deleteInfo = parent.deleteChildDirectory(name);
		DeleteFileInfoMsgPack reply = new DeleteFileInfoMsgPack((IndelibleFSDirectoryHandle)localConnection.createHandle(deleteInfo.getDirectoryNode()), deleteInfo.deleteSucceeded());
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult deleteObjectByPath(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSVolumeHandle volumeHandle,
			FilePath deletePath) throws IOException, ObjectNotFoundException, PermissionDeniedException, NotDirectoryException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF volume = (IndelibleFSVolumeIF)localConnection.getObjectForHandle(volumeHandle);
		DeleteFileInfo deleteInfo = volume.deleteObjectByPath(deletePath);
		IndelibleFSDirectoryHandle directoryHandle = (IndelibleFSDirectoryHandle) localConnection.createHandle(deleteInfo.getDirectoryNode());
		return new CommandResult(0, new DeleteFileInfoMsgPack(directoryHandle, deleteInfo.deleteSucceeded()));
	}

	@Override
	public CommandResult extendFork(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle, long extendLength)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSForkIF fork = localConnection.getObjectForHandle(forkHandle);
		long extendedLength = fork.extend(extendLength);
		return new CommandResult(0, extendedLength);
	}

	@Override
	public CommandResult getCurrentVersion(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectHandle objectHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSObjectIF object = localConnection.getObjectForHandle(objectHandle);
		IndelibleVersion version = object.getCurrentVersion();
		return new CommandResult(0, new IndelibleVersionMsgPack(version));
	}

	@Override
	public CommandResult getSnapshotInfo(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSVolumeHandle volume,
			IndelibleVersionMsgPack retrieveSnapshotVersion) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF localVolume = localConnection.getObjectForHandle(volume);
		IndelibleSnapshotInfo snapshotInfo = localVolume.getInfoForSnapshot(retrieveSnapshotVersion.getIndelibleVersion());
		return new CommandResult(0, new IndelibleSnapshotInfoMsgPack(snapshotInfo));
	}

	@Override
	public CommandResult addSnapshot(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSVolumeHandle volume,
			IndelibleSnapshotInfoMsgPack snapshotInfo) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF localVolume = localConnection.getObjectForHandle(volume);
		localVolume.addSnapshot(snapshotInfo.getIndelibleSnapshotInfo());
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult getVersion(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectHandle objectHandle,
			IndelibleVersion version, RetrieveVersionFlags flags)
			throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSObjectIF localObject = localConnection.getObjectForHandle(objectHandle);
		IndelibleFSObjectIF versionObject = localObject.getVersion(version, flags);
		IndelibleFSObjectHandle versionHandle = localConnection.createHandle(versionObject);
		return new CommandResult(0, new IndelibleFSObjectHandleMsgPack(versionHandle));
	}

	@Override
	public CommandResult length(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		long length = localFork.length();
		return new CommandResult(0, length);
	}

	
	@Override
	public CommandResult getSegmentIDs(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		CASIdentifier [] segmentIDs = localFork.getSegmentIDs();
		GetSegmentIDsReply reply = new GetSegmentIDsReply(segmentIDs);
		return new CommandResult(0, reply);
	}

	
	@Override
	public CommandResult getName(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSForkHandle forkHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");	
		IndelibleFSForkIF localFork = localConnection.getObjectForHandle(forkHandle);
		String name = localFork.getName();
		return new CommandResult(0, name);
	}

	@Override
	public CommandResult moveObject(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectHandle volumeHandle, FilePath sourcePath,
			FilePath destinationPath) throws IOException,
			ObjectNotFoundException, PermissionDeniedException,
			FileExistsException, NotDirectoryException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF volume = (IndelibleFSVolumeIF)localConnection.getObjectForHandle(volumeHandle);
		MoveObjectInfo moveInfo = volume.moveObject(sourcePath, destinationPath);
		DeleteFileInfoMsgPack sourceInfo = new DeleteFileInfoMsgPack((IndelibleFSDirectoryHandle) localConnection.createHandle(moveInfo.getSourceInfo().getDirectoryNode()), moveInfo.getSourceInfo().deleteSucceeded());
		CreateFileInfoMsgPack destInfo = new CreateFileInfoMsgPack((IndelibleFSDirectoryHandle) localConnection.createHandle(moveInfo.getDestInfo().getDirectoryNode()), 
				localConnection.createHandle(moveInfo.getDestInfo().getCreatedNode()));
				
		MoveObjectInfoMsgPack returnInfo = new MoveObjectInfoMsgPack(sourceInfo, destInfo);
		return new CommandResult(0, returnInfo);
	}

	@Override
	public CommandResult listVersions(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSObjectHandle fileHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSObjectIF localObject = localConnection.getObjectForHandle(fileHandle);
		IndelibleVersionIterator iterator = localObject.listVersions();
		IndelibleVersionIteratorHandle iteratorHandle = localConnection.createHandle(iterator);
		ArrayList<IndelibleVersion>returnVersionsList = new ArrayList<IndelibleVersion>();
		while (returnVersionsList.size() < ListVersionsReply.kMaxReturnVersions && iterator.hasNext())
		{
			returnVersionsList.add(iterator.next());
		}
		
		IndelibleVersion [] returnVersions = returnVersionsList.toArray(new IndelibleVersion[returnVersionsList.size()]);
		boolean hasMore = iterator.hasNext();
		ListVersionsReply reply = new ListVersionsReply(iteratorHandle, returnVersions, hasMore);
		return new CommandResult(0, reply);
	}
	
	@Override
	public CommandResult nextVersionListItems(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleVersionIteratorHandle handle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");

		IndelibleVersionIterator localIterator = localConnection.getObjectForHandle(handle);
		ArrayList<IndelibleVersion>returnVersionsList = new ArrayList<IndelibleVersion>();
		while (returnVersionsList.size() < ListVersionsReply.kMaxReturnVersions && localIterator.hasNext())
		{
			returnVersionsList.add(localIterator.next());
		}
		
		IndelibleVersion [] returnVersions = returnVersionsList.toArray(new IndelibleVersion[returnVersionsList.size()]);
		boolean hasMore = localIterator.hasNext();
		NextVersionListItemsReply reply = new NextVersionListItemsReply(returnVersions, hasMore);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult listSnapshots(IndelibleFSClientInfoIF clientInfo, 
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSVolumeHandle volumeHandle) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF localObject = localConnection.getObjectForHandle(volumeHandle);
		IndelibleSnapshotIterator iterator = localObject.listSnapshots();
		IndelibleSnapshotIteratorHandle iteratorHandle = localConnection.createHandle(iterator);
		ArrayList<IndelibleSnapshotInfo>returnSnapshotsList = new ArrayList<IndelibleSnapshotInfo>();
		while (returnSnapshotsList.size() < ListSnapshotsReply.kMaxReturnVersions && iterator.hasNext())
		{
			returnSnapshotsList.add(iterator.next());
		}
		
		IndelibleSnapshotInfo [] returnInfo = returnSnapshotsList.toArray(new IndelibleSnapshotInfo[returnSnapshotsList.size()]);
		boolean hasMore = iterator.hasNext();
		ListSnapshotsReply reply = new ListSnapshotsReply(iteratorHandle, returnInfo, hasMore);
		return new CommandResult(0, reply);
	}
	
	@Override
	public CommandResult nextSnapshotListItems(IndelibleFSClientInfoIF clientInfo, IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleSnapshotIteratorHandle handle) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		
		IndelibleSnapshotIterator localIterator = localConnection.getObjectForHandle(handle);
		ArrayList<IndelibleSnapshotInfo>returnInfoList = new ArrayList<IndelibleSnapshotInfo>();
		while (returnInfoList.size() < ListSnapshotsReply.kMaxReturnVersions && localIterator.hasNext())
		{
			returnInfoList.add(localIterator.next());
		}
		
		IndelibleSnapshotInfo [] returnInfo = returnInfoList.toArray(new IndelibleSnapshotInfo[returnInfoList.size()]);
		boolean hasMore = localIterator.hasNext();
		NextSnapshotListItemsReply reply = new NextSnapshotListItemsReply(returnInfo, hasMore);
		return new CommandResult(0, reply);
	}
	
	@Override
	public void completed(CommandResult result, CommandToProcess attachment)
	{
		try
		{
			sendReplyAndPayload(attachment.getChannel(), attachment.getCommandToProcess().getCommandCode(), attachment.getCommandSequence(), result);
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
	}

	@Override
	public void failed(Throwable exc, CommandToProcess attachment)
	{
		if (logger.isDebugEnabled())
			logger.debug(new ErrorLogMessage("Command "+attachment+" failed with exception"), exc);
		commandFailed(attachment, exc);
	}

	@Override
	public CommandResult lastModified(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSFileHandle fileHandle) throws IOException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFileNodeIF localFile = localConnection.getObjectForHandle(fileHandle);
		
		return new CommandResult(0, localFile.lastModified());
	}

	@Override
	public CommandResult getVolumeName(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connection,
			IndelibleFSVolumeHandle volumeHandle) throws IOException, PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connection);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSVolumeIF localVolume = localConnection.getObjectForHandle(volumeHandle);
		String volumeName = localVolume.getVolumeName();
		return new CommandResult(0, volumeName);
	}

	@Override
	public CommandResult getChildNodeInfo(IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connectionHandle,
			IndelibleFSDirectoryHandle directoryHandle,
			String[] mdToRetrieve) throws IOException,
			PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connectionHandle);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleDirectoryNodeIF parent = (IndelibleDirectoryNodeIF)localConnection.getObjectForHandle(directoryHandle);
		IndelibleNodeInfo [] nodeInfo = parent.getChildNodeInfo(mdToRetrieve);
		IndelibleNodeInfoMsgPack [] nodeInfoMsgPack = new IndelibleNodeInfoMsgPack[nodeInfo.length];
		for (int curChildInfoNum = 0; curChildInfoNum < nodeInfo.length; curChildInfoNum++)
		{
			nodeInfoMsgPack[curChildInfoNum] = new IndelibleNodeInfoMsgPack(nodeInfo[curChildInfoNum]);
		}
		byte [] nodeInfoBytes = packer.write(nodeInfoMsgPack);
		CASIDMemoryDataDescriptor returnLocalDescriptor = new CASIDMemoryDataDescriptor(nodeInfoBytes);
		NetworkDataDescriptor returnNetworkDescriptor = getConnectionForHandle(clientInfo, connectionHandle).getManagerConnection().registerDataDescriptor(returnLocalDescriptor);

		GetChildNodeInfoReply reply = new GetChildNodeInfoReply(returnNetworkDescriptor);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult setMetaDataResource(
			IndelibleFSClientInfoIF clientInfo,
			IndelibleFSServerConnectionHandle connection,
			IndelibleFSObjectHandle fileHandle, String mdResourceName,
			Map<String, Object> resource) throws IOException,
			PermissionDeniedException
	{
		IndelibleFSClientConnection localConnection = getConnectionForHandle(clientInfo, connection);
		if (localConnection == null)
			throw new IOException("Connection closed");
		IndelibleFSObjectIF object = localConnection.getObjectForHandle(fileHandle);
		IndelibleFSObjectIF newObject = object.setMetaDataResource(mdResourceName, resource);
		IndelibleFSObjectHandle returnHandle = localConnection.createHandle(newObject);
		return new CommandResult(0,  new IndelibleFSObjectHandleMsgPack(returnHandle));
	}

	@Override
	protected void closeAndAbortChannel(FirehoseChannel channelToClose)
	{
		super.closeAndAbortChannel(channelToClose);
		closeChannel(channelToClose);
	}

	@Override
	protected void closeChannel(FirehoseChannel channelToClose)
	{
		// Now, discard everything we're hanging on to for the channel
		IndelibleFSClientInfo closeClientInfo = removeChannel(channelToClose);
		if (closeClientInfo != null)
		{
			closeClientInfo.closeConnections();
		}
	}

	@Override
	public CommandResult getCASServerPort(IndelibleFSClientInfoIF clientInfo)
	{
		return new CommandResult(0, (Integer)casServerPort);
	}	
	
	@Override
	public boolean removeTarget(FirehoseTarget removeTarget) 
	{
		logger.error(new ErrorLogMessage("Removing target "+removeTarget));
		try
		{
			boolean removed = super.removeTarget(removeTarget);
			return removed;
		}
		finally
		{
			synchronized(targets)
			{
				if (targets.size() == 0)
				{
					logger.error(new ErrorLogMessage("All EntityAuthenticationServer targets have been removed - exiting!"));
					System.exit(1);
				}
			}
		}
	}

	@Override
	public CommandResult getServerID(IndelibleFSClientInfoIF clientInfo)
	{
		EntityIDMsgPack returnID = new EntityIDMsgPack(localServer.getServerID());
		CommandResult returnResults = new CommandResult(0, returnID);
		return returnResults;
	}
}
