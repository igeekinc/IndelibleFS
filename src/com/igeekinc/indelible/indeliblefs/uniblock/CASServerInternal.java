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
 
package com.igeekinc.indelible.indeliblefs.uniblock;

import java.io.IOException;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.datadescriptor.DataDescriptor;

public interface CASServerInternal<C extends CASServerConnectionIF> extends LocalCASServer<C>
{
    public ObjectIDFactory getOIDFactory();
    void close(C closeConnection);
	public CASStoreManager getCASStoreManager();
	void finalizeVersion(IndelibleVersion version);
	
    IndelibleVersion startTransaction(C connection) throws IOException;
    IndelibleVersion startReplicatedTransaction(C casServerConnection, TransactionCommittedEvent transaction) throws IOException;
    IndelibleFSTransaction commit(C connection) throws IOException;
    void rollback(C connection);
    public CASCollectionConnection openCollection(C connection, CASCollectionID id) throws CollectionNotFoundException;
    public CASCollectionConnection createNewCollection(C connection) throws IOException;
    public CASCollectionID [] listCollections(C connection);
    public DataDescriptor retrieveMetaData(C connection, String name) throws IOException;
    public void storeMetaData(C connection, String name, DataDescriptor metaData) throws IOException;
    
    public void logCASCollectionEvent(C casServerConnection, ObjectID source, CASCollectionEvent event, IndelibleFSTransaction transaction) throws IOException;
	public void logReplicatedCASCollectionEvent(C casServerConnection, ObjectID source, CASCollectionEvent casEvent, IndelibleFSTransaction transaction);
	
	public void logServerEvent(C casServerConnection, ObjectID source, CASServerEvent serverEvent, IndelibleFSTransaction transaction);
	public void logReplicatedServerEvent(C casServerConnection, ObjectID source, CASServerEvent serverEvent, IndelibleFSTransaction transaction);
	
	public CASCollectionConnection addCollection(C casServerConnection, CASCollectionID addCollectionID) throws IOException;
	public void addConnectedServer(C casServerConnection, EntityID serverID, EntityID securityServerID);
	public CASCollectionEvent[] getCollectionEventsAfterEventID(C serverConnection, CASCollectionID id, long lastEventID, int maxToReturn) throws IOException;
	public CASCollectionEvent [] getCollectionEventsAfterTimestamp(C connection, CASCollectionID collectionID, long timestamp, int numToReturn) throws IOException;
    public CASCollectionEvent [] getCollectionEventsAfterEventIDForTransaction(C connection, CASCollectionID collectionID, long startingEventID, int numToReturn, IndelibleFSTransaction transaction) throws IOException;
	public CASServerEvent[] getServerEventsAfterEventID(C serverConnection, long lastEventID, int maxToReturn) throws IOException;
	public CASServerEvent[] getServerEventsAfterTimestamp(C connection, long timestamp, int numToReturn) throws IOException;
	public CASServerEvent [] getServerEventsAfterEventIDForTransaction(C connection, CASCollectionID collectionID, long startingEventID, int numToReturn, IndelibleFSTransaction transaction) throws IOException;
	public long getLastServerEventID(C casServerConnection);
	public long getVersionIDForVersion(C connection, IndelibleVersion version);
	public void deleteCollection(C casServerConnection, CASCollectionID id) throws CollectionNotFoundException, IOException;    
}
