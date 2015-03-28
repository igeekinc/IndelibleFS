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
package com.igeekinc.indelible.indeliblefs.uniblock.firehose;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.igeekinc.firehose.CommandResult;
import com.igeekinc.firehose.CommandToProcess;
import com.igeekinc.firehose.FirehoseChannel;
import com.igeekinc.firehose.SSLFirehoseChannel;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.datamover.msgpack.NetworkDataDescriptorMsgPack;
import com.igeekinc.indelible.indeliblefs.datamover.msgpack.SessionAuthenticationMsgPack;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.firehose.AuthenticatedFirehoseServer;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseClient;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSServerCommand;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleVersionIteratorHandle;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.GetMoverAddressesReply;
import com.igeekinc.indelible.indeliblefs.firehose.msgpack.IndelibleVersionMsgPack;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.security.remote.msgpack.EntityIDMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.LocalCASServer;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentExists;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.indeliblefs.uniblock.firehose.proxies.CASServerConnectionProxy;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.BulkReleaseReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASCollectionEventIteratorReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASCollectionQueuedEventMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASIdentifierMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASServerCommandMessage;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASServerEventIteratorReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CASStoreInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.CreateNewCollectionReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.DataVersionInfoMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.GetMetaDataResourceReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.IndelibleFSTransactionMsgPack;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.ListCollectionsReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.ListMetaDataNamesReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.NextCASServerEventListItemsReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.NextCollectionEventListItemsReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.NextVersionListItemsReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.PollForCollectionEventsReply;
import com.igeekinc.indelible.indeliblefs.uniblock.msgpack.VersionIteratorReply;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.indelible.oid.msgpack.ObjectIDMsgPack;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.datadescriptor.DataDescriptor;
import com.igeekinc.util.logging.ErrorLogMessage;

public class CASServerFirehoseServer extends AuthenticatedFirehoseServer<CASServerClientInfo> 
implements CASServerFirehoseServerIF, AsyncCompletion<CommandResult, CommandToProcess>
{
	private int threadNum = 0;
	private LocalCASServer<?> localCASServer;
	static
	{
		// Load the map and fail early if there's a mistake
		for (CASServerCommand checkCommand:CASServerCommand.values())
		{
			if (checkCommand != CASServerCommand.kIllegalCommand)
				CASServerFirehoseClient.getReturnClassForCommandCodeStatic(checkCommand.commandNum);	// This exercises both getReturnClassForCommandCodeStatic and getClassForCommandCode
		}
	}
	
	public CASServerFirehoseServer(LocalCASServer<?> localCASServer)
	{
		this.localCASServer = localCASServer;
	}
	
	private CASServerClientConnection getConnectionForHandle(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		return ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
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
	public int getExtendedErrorCodeForThrowable(Throwable t)
	{
		return IndelibleFSFirehoseClient.getExtendedErrorCodeForThrowableStatic(t);
	}

	@Override
	public CommandResult addClientSessionAuthentication(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			SessionAuthentication sessionAuthenticationToAdd)
	{
		CASServerConnection connection = getConnectionForHandle(clientInfo, connectionHandle).getLocalConnection();
		connection.addClientSessionAuthentication(sessionAuthenticationToAdd);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult addCollection(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, CASCollectionID addCollectionID) throws IOException
	{
		CASServerClientConnection connection = getConnectionForHandle(clientInfo, connectionHandle);
		CASCollectionConnection collectionConnection = connection.getLocalConnection().addCollection(addCollectionID);
		CASCollectionConnectionHandle collectionConnectionHandle = connection.createCollectionConnectionHandle(collectionConnection);
		return new CommandResult(0, collectionConnectionHandle);
	}

	@Override
	public CommandResult addConnectedServer(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, EntityID serverID, EntityID securityServerID)
	{
		CASServerConnection connection = getConnectionForHandle(clientInfo, connectionHandle).getLocalConnection();
		connection.addConnectedServer(serverID, securityServerID);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult commit(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle) throws IOException
	{
		CASServerConnection connection = getConnectionForHandle(clientInfo, connectionHandle).getLocalConnection();
		IndelibleFSTransaction returnTransaction = connection.commit();
		IndelibleFSTransactionMsgPack returnTransactionMsgPack = new IndelibleFSTransactionMsgPack(returnTransaction);
		return new CommandResult(0, returnTransactionMsgPack);
	}

	@Override
	public CommandResult createNewCollection(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle) throws IOException
	{
		CASServerClientConnection connection = getConnectionForHandle(clientInfo, connectionHandle);
		CASCollectionConnection collectionConnection = connection.getLocalConnection().createNewCollection();
		CASCollectionConnectionHandle collectionConnectionHandle = connection.createCollectionConnectionHandle(collectionConnection);
		CreateNewCollectionReply reply = new CreateNewCollectionReply(collectionConnection.getCollectionID(), collectionConnectionHandle);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult listCollections(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		CASServerConnectionIF connection = getConnectionForHandle(clientInfo, connectionHandle).getLocalConnection();
		CASCollectionID [] collectionIDs = connection.listCollections();
		ListCollectionsReply reply = new ListCollectionsReply(collectionIDs);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult openCollectionConnection(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, CASCollectionID collectionID) throws CollectionNotFoundException, IOException
	{
		CASServerClientConnection connection = getConnectionForHandle(clientInfo, connectionHandle);
		CASCollectionConnection returnConnection = connection.getLocalConnection().openCollectionConnection(collectionID);
		CASCollectionConnectionHandle returnHandle = connection.createCollectionConnectionHandle(returnConnection);
		return new CommandResult(0, returnHandle);
	}

	@Override
	public CommandResult eventsAfterIDIterator(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, long startingID)
	{
		CASServerClientConnection connection = getConnectionForHandle(clientInfo, connectionHandle);
		IndelibleEventIterator iterator = connection.getLocalConnection().eventsAfterID(startingID);
		CASServerEventIteratorReply reply = createCASServerEventIteratorHandleAndReply(connection, iterator);
		return new CommandResult(0, reply);
	}

	private CASServerEventIteratorReply createCASServerEventIteratorHandleAndReply(CASServerClientConnection connection, IndelibleEventIterator iterator)
	{
		IndelibleEventIteratorHandle iteratorHandle = connection.createIteratorHandle(iterator);
		ArrayList<IndelibleEvent>returnEventsList = new ArrayList<IndelibleEvent>();
		while (returnEventsList.size() < CASServerEventIteratorReply.kMaxReturnEvents && iterator.hasNext())
		{
			returnEventsList.add(iterator.next());
		}
		
		IndelibleEvent [] returnEvents = returnEventsList.toArray(new IndelibleEvent[returnEventsList.size()]);
		boolean hasMore = iterator.hasNext();
		CASServerEventIteratorReply reply = new CASServerEventIteratorReply(iteratorHandle, returnEvents, hasMore);
		return reply;
	}

	@Override
	public CommandResult eventsAfterTimeIterator(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, long timestamp)
	{
		CASServerClientConnection connection = getConnectionForHandle(clientInfo, connectionHandle);
		IndelibleEventIterator iterator = connection.getLocalConnection().eventsAfterTime(timestamp);
		CASServerEventIteratorReply reply = createCASServerEventIteratorHandleAndReply(connection, iterator);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult openCASServerConnection(CASServerClientInfoIF clientInfo)
	{
		CASServerClientInfo casServerClientInfo = (CASServerClientInfo)clientInfo;
		CASServerConnection localConnection = (CASServerConnection) localCASServer.open(casServerClientInfo.getClientEntityAuthentication());
		CASServerConnectionHandle connectionHandle = casServerClientInfo.createConnectionHandle(localConnection);
		return new CommandResult(0, connectionHandle);
	}

	@Override
	public CommandResult retrieveMetaData(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, String name) throws IOException
	{
		CASServerClientInfo casServerClientInfo = (CASServerClientInfo)clientInfo;
		CASServerConnection localConnection = (CASServerConnection) localCASServer.open(casServerClientInfo.getClientEntityAuthentication());
		DataDescriptor localDescriptor = localConnection.retrieveMetaData(name);
		NetworkDataDescriptor networkDescriptor = localConnection.getDataMoverSession().registerDataDescriptor(localDescriptor);
		return new CommandResult(0, new NetworkDataDescriptorMsgPack(networkDescriptor));
	}

	@Override
	public CommandResult rollback(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		CASServerClientInfo casServerClientInfo = (CASServerClientInfo)clientInfo;
		CASServerConnection localConnection = (CASServerConnection) localCASServer.open(casServerClientInfo.getClientEntityAuthentication());
		localConnection.rollback();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult setupReverseMoverConnection(CASServerClientInfoIF clientInfo, CASServerConnectionProxy connection, EntityID securityServerID,
			InetAddress connectToAddress, int connectToPort) throws IOException, AuthenticationFailureException
	{
		DataMoverSource.getDataMoverSource().openReverseConnection(securityServerID, connectToAddress, connectToPort);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult startTransaction(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle) throws IOException
	{
		CASServerClientInfo casServerClientInfo = (CASServerClientInfo)clientInfo;
		CASServerConnection localConnection = (CASServerConnection) localCASServer.open(casServerClientInfo.getClientEntityAuthentication());
		IndelibleVersion returnVersion = localConnection.startTransaction();
		return new CommandResult(0, new IndelibleVersionMsgPack(returnVersion));
	}

	@Override
	public CommandResult storeMetaData(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, String name,
			NetworkDataDescriptor metaData) throws IOException
	{
		CASServerClientInfo casServerClientInfo = (CASServerClientInfo)clientInfo;
		CASServerConnection localConnection = (CASServerConnection) localCASServer.open(casServerClientInfo.getClientEntityAuthentication());
		localConnection.storeMetaData(name, metaData);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult testReverseConnection(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			NetworkDataDescriptor testNetworkDescriptor) throws IOException
	{
		logger.error(new ErrorLogMessage("Starting testReverseConnection"));
		if (testNetworkDescriptor.getHostPorts().length > 0)
		{
			for (InetSocketAddress curHostPort:testNetworkDescriptor.getHostPorts())
			{
				logger.error(new ErrorLogMessage("testReverseConnection addr:{0}:{1}", new Object[]{
						curHostPort.getAddress(), curHostPort.getPort()
				}));
			}
		}
		else
		{
			logger.error(new ErrorLogMessage("testReverseConnection local only descriptor"));
		}
		testNetworkDescriptor.getData();
		logger.error(new ErrorLogMessage("testReverseConnection finished successfully"));
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult getMoverAddresses(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, EntityID securityServerID)
	{
		InetSocketAddress [] moverAddresses = DataMoverSource.getDataMoverSource().getListenNetworkAddresses(securityServerID);
		GetMoverAddressesReply reply = new GetMoverAddressesReply(moverAddresses);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult getMoverID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
        EntityID moverID = DataMoverReceiver.getDataMoverReceiver().getEntityID();
        ObjectIDMsgPack reply = new ObjectIDMsgPack(moverID);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult getTestDescriptor(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		CASServerClientInfo casServerClientInfo = (CASServerClientInfo)clientInfo;
		CASServerClientConnection clientCollectionConnection = casServerClientInfo.getConnectionForHandle(connectionHandle);
		CASServerConnection localConnection = clientCollectionConnection.getLocalConnection();
		byte [] testBuffer = new byte[32*1024];
		CASIDDataDescriptor testDescriptor = new CASIDMemoryDataDescriptor(testBuffer);
		NetworkDataDescriptor testNetworkDescriptor = localConnection.getDataMoverSession().registerDataDescriptor(testDescriptor);
		return new CommandResult(0, new NetworkDataDescriptorMsgPack(testNetworkDescriptor));

	}

	@Override
	public Thread createSelectLoopThread(Runnable selectLoopRunnable)
	{
		Thread returnThread = new Thread(selectLoopRunnable, "CAS Server "+threadNum);
		threadNum++;
		return returnThread;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends CASServerCommandMessage> getClassForCommandCode(int commandCode)
	{
		return CASServerFirehoseClient.getClassForCommandCode(commandCode);
	}

	@Override
	protected CASServerClientInfo createClientInfo(FirehoseChannel channel)
	{
		try
		{
			return new CASServerClientInfo((SSLFirehoseChannel)channel);
		} catch (AuthenticationFailureException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		throw new IllegalArgumentException("Security exception");
	}

	@Override
	protected void processCommand(CASServerClientInfo clientInfo, CommandToProcess commandToProcessBlock) throws Exception
	{
		CASServerCommandMessage<?, ?, ?> commandToProcess = (CASServerCommandMessage<?, ?, ?>) commandToProcessBlock.getCommandToProcess();
		logger.debug("Executing command "+commandToProcess.getCommand());
		String originalName = Thread.currentThread().getName();
		try
		{
			Thread.currentThread().setName("CASServer "+commandToProcess.getCommand());
			commandToProcess.executeAsync(this, clientInfo, this, commandToProcessBlock);
		}
		finally
		{
			Thread.currentThread().setName(originalName);
		}		
	}
	
	private static HashMap<Integer, Class<? extends Object>>returnClassMap = new HashMap<Integer, Class<? extends Object>>();

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })	// Warnings?  We don't need no stinkin' warnings.

	protected Class<? extends Object> getReturnClassForCommandCode(int commandCode)
	{
		synchronized(returnClassMap)
		{
			Class<? extends Object> returnClass = returnClassMap.get(commandCode);
			if (returnClass == null)
			{
				Class<? extends CASServerCommandMessage>commandClass = getClassForCommandCode(commandCode);
				try
				{
					returnClass = commandClass.getConstructor().newInstance().getResultClass();
					returnClassMap.put(commandCode, returnClass);
				} catch (Throwable e)
				{
					Logger.getLogger(IndelibleFSFirehoseClient.class).error(new ErrorLogMessage("Caught exception"), e);
					throw new InternalError(IndelibleFSServerCommand.getCommandForNum(commandCode)+" not configured");
				} 
			}
			return returnClass;
		}
	}

	@Override
	public CommandResult getLastEventID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		long lastEventID = connection.getLocalConnection().getLastEventID();
		return new CommandResult(0, lastEventID);
	}

	@Override
	public CommandResult getLastReplicatedEventID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, EntityID sourceServerID,
			CASCollectionID collectionID)
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		long lastEventID = connection.getLocalConnection().getLastReplicatedEventID(sourceServerID, collectionID);
		return new CommandResult(0, lastEventID);
	}

	@Override
	public CommandResult getServerEventsAfterIDIterator(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, long startingEventID, int timeToWait) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		IndelibleEventIterator iterator = connection.getLocalConnection().getServerEventsAfterEventID(startingEventID, timeToWait);
		CASServerEventIteratorReply reply = createCASServerEventIteratorHandleAndReply(connection, iterator);
		return new CommandResult(0, reply);	
	}

	@Override
	public CommandResult getSessionAuthentication(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		SessionAuthentication returnAuthentication = connection.getLocalConnection().getSessionAuthentication();
		return new CommandResult(0, new SessionAuthenticationMsgPack(returnAuthentication));
	}

	@Override
	public CommandResult getCollectionLastEventID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnection, CASCollectionConnectionHandle connectionHandle)
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnection);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(connectionHandle);
		long lastReplicatedEventID = collectionConnection.getLocalConnection().getLastEventID();
		return new CommandResult(0, lastReplicatedEventID);
	}

	@Override
	public CommandResult getSecurityServerID(CASServerClientInfoIF clientInfo) throws IOException
	{
		EntityID securityServerID = localCASServer.getSecurityServerID();
		return new CommandResult(0, new EntityIDMsgPack(securityServerID));
	}

	@Override
	public CommandResult getCollectionLastReplicatedEventID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle, CASCollectionConnectionHandle connectionHandle,
			EntityID sourceServerID, CASCollectionID collectionID)
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(connectionHandle);
		long lastReplicatedEventID = collectionConnection.getLocalConnection().getLastReplicatedEventID(sourceServerID, collectionID);
		return new CommandResult(0, lastReplicatedEventID);
	}

	@Override
	public CommandResult nextEventListItems(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			IndelibleEventIteratorHandle handle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		IndelibleEventIterator localIterator = connection.getEventIteratorForHandle(handle);
		ArrayList<IndelibleEvent>returnEventsList = new ArrayList<IndelibleEvent>();
		while (returnEventsList.size() < NextCASServerEventListItemsReply.kMaxReturnEvents && localIterator.hasNext())
		{
			returnEventsList.add(localIterator.next());
		}
		
		IndelibleEvent [] returnEvents = returnEventsList.toArray(new IndelibleEvent[returnEventsList.size()]);
		boolean hasMore = localIterator.hasNext();
		NextCASServerEventListItemsReply reply = new NextCASServerEventListItemsReply(returnEvents, hasMore);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult collectionEventsAfterIDIterator(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, long startingID) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleEventIterator iterator = collectionConnection.getLocalConnection().eventsAfterID(startingID);
		CASCollectionEventIteratorReply reply = createCollectionConnectionEventIteratorHandleAndReply(collectionConnection, iterator);
		return new CommandResult(0, reply);	
	}

	@Override
	public CommandResult collectionEventsAfterTimeIterator(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, long timestamp) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleEventIterator iterator = collectionConnection.getLocalConnection().eventsAfterTime(timestamp);
		CASCollectionEventIteratorReply reply = createCollectionConnectionEventIteratorHandleAndReply(collectionConnection, iterator);
		return new CommandResult(0, reply);	
	}

	@Override
	public CommandResult nextCollectionEventListItems(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, IndelibleEventIteratorHandle handle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleEventIterator iterator = collectionConnection.getEventIteratorForHandle(handle);
		ArrayList<IndelibleEvent>returnEventsList = new ArrayList<IndelibleEvent>();
		while (returnEventsList.size() < NextCollectionEventListItemsReply.kMaxReturnEvents && iterator.hasNext())
		{
			returnEventsList.add(iterator.next());
		}
		
		IndelibleEvent [] returnEvents = returnEventsList.toArray(new IndelibleEvent[returnEventsList.size()]);
		boolean hasMore = iterator.hasNext();
		NextCollectionEventListItemsReply reply = new NextCollectionEventListItemsReply(returnEvents, hasMore);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult retrieveSegmentByCASIdentifier(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, CASIdentifier segmentID) throws IOException, SegmentNotFound
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		CASIDDataDescriptor localDescriptor = collectionConnection.getLocalConnection().retrieveSegment(segmentID);
		NetworkDataDescriptor networkDescriptor = connection.getLocalConnection().getDataMoverSession().registerDataDescriptor(localDescriptor);
		return new CommandResult(0, new NetworkDataDescriptorMsgPack(networkDescriptor));
	}

	@Override
	public CommandResult retrieveSegmentByObjectID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID objectID) throws IOException, SegmentNotFound
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		DataVersionInfo localDataVersionInfo = collectionConnection.getLocalConnection().retrieveSegment(objectID);
		NetworkDataDescriptor networkDescriptor = connection.getLocalConnection().getDataMoverSession().registerDataDescriptor(localDataVersionInfo.getDataDescriptor());
		return new CommandResult(0, new DataVersionInfoMsgPack(networkDescriptor, localDataVersionInfo.getVersion()));
	}

	@Override
	public CommandResult retrieveSegmentByObjectIDAndVersion(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID objectID, IndelibleVersion indelibleVersion, RetrieveVersionFlags retrieveVersionFlags) throws IOException,
			SegmentNotFound
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		DataVersionInfo localDataVersionInfo = collectionConnection.getLocalConnection().retrieveSegment(objectID, indelibleVersion, retrieveVersionFlags);
		NetworkDataDescriptor networkDescriptor = connection.getLocalConnection().getDataMoverSession().registerDataDescriptor(localDataVersionInfo.getDataDescriptor());
		return new CommandResult(0, new DataVersionInfoMsgPack(networkDescriptor, localDataVersionInfo.getVersion()));
	}

	@Override
	public CommandResult storeSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, NetworkDataDescriptor networkDataDescriptor) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		CASStoreInfo returnInfo = collectionConnection.getLocalConnection().storeSegment(networkDataDescriptor);
		return new CommandResult(0, new CASStoreInfoMsgPack(returnInfo));
	}

	@Override
	public CommandResult releaseSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID releaseID) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		boolean released = collectionConnection.getLocalConnection().releaseSegment(releaseID);
		return new CommandResult(0, released);
	}

	@Override
	public CommandResult bulkReleaseSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, CASSegmentID[] releaseIDs) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		boolean [] statuses = collectionConnection.getLocalConnection().bulkReleaseSegment(releaseIDs);
		BulkReleaseReply reply = new BulkReleaseReply(statuses);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult startCollectionReplicatedTransaction(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, TransactionCommittedEvent transactionEvent) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().startReplicatedTransaction(transactionEvent);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult startCollectionTransaction(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().startTransaction();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult rollbackCollectionTransaction(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().rollback();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult commitCollectionTransaction(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleFSTransaction commitTransaction = collectionConnection.getLocalConnection().commit();
		return new CommandResult(0, new IndelibleFSTransactionMsgPack(commitTransaction));
	}

	@Override
	public CommandResult verifySegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID segmentID, IndelibleVersion indelibleVersion,
			RetrieveVersionFlags flags) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		boolean verified = collectionConnection.getLocalConnection().verifySegment(segmentID, indelibleVersion, flags);
		return new CommandResult(0, new Boolean(verified));
	}

	@Override
	public CommandResult storeReplicatedSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID replicateSegmentID, IndelibleVersion replicateVersion,
			NetworkDataDescriptor sourceDescriptor, CASCollectionEvent replicatedEvent) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().storeReplicatedSegment(replicateSegmentID, replicateVersion, sourceDescriptor, replicatedEvent);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult repairSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID checkSegmentID, IndelibleVersion transactionVersion, DataVersionInfo masterData)
			throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().repairSegment(checkSegmentID, transactionVersion, masterData);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult replicateMetaDataResource(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, NetworkDataDescriptor sourceMetaData, CASCollectionEvent curCASEvent) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().replicateMetaDataResource(sourceMetaData, curCASEvent);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult storeVersionedSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID id, NetworkDataDescriptor segmentDescriptor)
			throws IOException, SegmentExists
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().storeVersionedSegment(id, segmentDescriptor);
		return new CommandResult(0, null);

	}

	@Override
	public CommandResult releaseVersionedSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle serverConnectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID id, IndelibleVersion version) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(serverConnectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		boolean released = collectionConnection.getLocalConnection().releaseVersionedSegment(id, version);
		return new CommandResult(0, new Boolean(released));
	}

	@Override
	public CommandResult retrieveCASIdentifier(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, CASSegmentID casSegmentID) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		CASIdentifier returnIdentifier = collectionConnection.getLocalConnection().retrieveCASIdentifier(casSegmentID);
		return new CommandResult(0, new CASIdentifierMsgPack(returnIdentifier));
	}

	@Override
	public CommandResult getMetaDataForReplication(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		CASIDDataDescriptor metaDataDescriptor = collectionConnection.getLocalConnection().getMetaDataForReplication();
		NetworkDataDescriptor networkDescriptor = connection.getLocalConnection().getDataMoverSession().registerDataDescriptor(metaDataDescriptor);
		return new CommandResult(0, new NetworkDataDescriptorMsgPack(networkDescriptor));
	}

	@Override
	public CommandResult nextVersionListItems(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, IndelibleVersionIteratorHandle handle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleVersionIterator localIterator = collectionConnection.getVersionIteratorForHandle(handle);
		ArrayList<IndelibleVersion>returnEventsList = new ArrayList<IndelibleVersion>();
		while (returnEventsList.size() < NextVersionListItemsReply.kMaxReturnEvents && localIterator.hasNext())
		{
			returnEventsList.add(localIterator.next());
		}
		
		IndelibleVersion [] returnEvents = returnEventsList.toArray(new IndelibleVersion[returnEventsList.size()]);
		boolean hasMore = localIterator.hasNext();
		NextVersionListItemsReply reply = new NextVersionListItemsReply(returnEvents, hasMore);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult listVersionsForSegment(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID id) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleVersionIterator localIterator = collectionConnection.getLocalConnection().listVersionsForSegment(id);
		VersionIteratorReply reply = createVersionIteratorHandleAndReply(collectionConnection, localIterator);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult listVersionsForSegmentInRange(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, ObjectID id, IndelibleVersion first, IndelibleVersion last) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleVersionIterator localIterator = collectionConnection.getLocalConnection().listVersionsForSegmentInRange(id, first, last);
		VersionIteratorReply reply = createVersionIteratorHandleAndReply(collectionConnection, localIterator);
		return new CommandResult(0, reply);
	}

	private VersionIteratorReply createVersionIteratorHandleAndReply(CASCollectionClientConnection collectionConnection,
			IndelibleVersionIterator localIterator)
	{
		ArrayList<IndelibleVersion>returnEventsList = new ArrayList<IndelibleVersion>();
		while (returnEventsList.size() < VersionIteratorReply.kMaxReturnEvents && localIterator.hasNext())
		{
			returnEventsList.add(localIterator.next());
		}
		
		IndelibleVersion [] returnEvents = returnEventsList.toArray(new IndelibleVersion[returnEventsList.size()]);
		boolean hasMore = localIterator.hasNext();
		IndelibleVersionIteratorHandle returnHandle = collectionConnection.createIteratorHandle(localIterator);
		VersionIteratorReply reply = new VersionIteratorReply(returnHandle, returnEvents, hasMore);
		return reply;
	}

	@Override
	public CommandResult getEventsForTransaction(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, IndelibleFSTransaction transaction) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleEventIterator localIterator = collectionConnection.getLocalConnection().getEventsForTransaction(transaction);
		CASCollectionEventIteratorReply reply = createCollectionConnectionEventIteratorHandleAndReply(collectionConnection, localIterator);
		return new CommandResult(0, reply);
	}
	
	private CASCollectionEventIteratorReply createCollectionConnectionEventIteratorHandleAndReply(CASCollectionClientConnection collectionConnection, IndelibleEventIterator iterator)
	{
		IndelibleEventIteratorHandle iteratorHandle = collectionConnection.createIteratorHandle(iterator);
		ArrayList<IndelibleEvent>returnEventsList = new ArrayList<IndelibleEvent>();
		while (returnEventsList.size() < CASCollectionEventIteratorReply.kMaxReturnEvents && iterator.hasNext())
		{
			returnEventsList.add(iterator.next());
		}
		
		IndelibleEvent [] returnEvents = returnEventsList.toArray(new IndelibleEvent[returnEventsList.size()]);
		boolean hasMore = iterator.hasNext();
		CASCollectionEventIteratorReply reply = new CASCollectionEventIteratorReply(iteratorHandle, returnEvents, hasMore);
		return reply;
	}

	@Override
	public CommandResult getTransactionEventsAfterEventID(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, long eventID, int timeToWait) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		IndelibleEventIterator localIterator = collectionConnection.getLocalConnection().getTransactionEventsAfterEventID(eventID, timeToWait);
		CASCollectionEventIteratorReply reply = createCollectionConnectionEventIteratorHandleAndReply(collectionConnection, localIterator);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult getMetaDataResource(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, String resourceName) throws IOException, PermissionDeniedException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		Map<String, Serializable>resource = collectionConnection.getLocalConnection().getMetaDataResource(resourceName);
		GetMetaDataResourceReply reply = new GetMetaDataResourceReply(resource);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult setMetaDataResource(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle, String mdResourceName, Map<String, Serializable> resource) throws PermissionDeniedException, IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.getLocalConnection().setMetaDataResource(mdResourceName, resource);
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult closeCASServerConnection(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle)
	{
		// TODO Auto-generated method stub
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult listMetaDataNames(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, CASCollectionConnectionHandle collectionConnectionHandle)
			throws IOException, PermissionDeniedException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		String [] metaDataNames = collectionConnection.getLocalConnection().listMetaDataNames();
		ListMetaDataNamesReply reply = new ListMetaDataNamesReply(metaDataNames);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult startListeningForCollection(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle,
			CASCollectionConnectionHandle collectionConnectionHandle) throws IOException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionClientConnection collectionConnection = connection.getCollectionConnectionForHandle(collectionConnectionHandle);
		collectionConnection.listenForEvents();
		return new CommandResult(0, null);
	}

	@Override
	public CommandResult pollForCollectionEvents(CASServerClientInfoIF clientInfo, CASServerConnectionHandle connectionHandle, int maxEvents,
			long timeout) throws IOException, InterruptedException
	{
		CASServerClientConnection connection = ((CASServerClientInfo)clientInfo).getConnectionForHandle(connectionHandle);
		CASCollectionQueuedEventMsgPack [] events = connection.pollCollectionEventQueue(maxEvents, timeout);
		PollForCollectionEventsReply reply = new PollForCollectionEventsReply(events);
		return new CommandResult(0, reply);
	}

	@Override
	public CommandResult getServerID(CASServerClientInfoIF clientInfo) throws IOException
	{
		EntityIDMsgPack returnID = new EntityIDMsgPack(localCASServer.getServerID());
		CommandResult returnResults = new CommandResult(0, returnID);
		return returnResults;
	}
	
	
}
