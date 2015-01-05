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
 
package com.igeekinc.indelible.indeliblefs.uniblock.dbcas;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersionIterator;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.CASSegmentIDIterator;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerInternal;
import com.igeekinc.indelible.indeliblefs.uniblock.CASStoreInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.SegmentInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.dbcas.statements.DBCASServerConnectionStatements;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentExists;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.CASSegmentID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.objectcache.LRUQueue;

class SegmentCollectionID
{
	ObjectID id;
	long transactionID;
	int internalCollectionID;
	
	public SegmentCollectionID(ObjectID id, IndelibleVersion version, int internalCollectionID)
	{
		this.id = id;
		this.transactionID = version.getVersionID();	// Time & date may change on the version but the transaction ID is fixed
		this.internalCollectionID = internalCollectionID;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + internalCollectionID;
		result = prime * result
				+ (int) (transactionID ^ (transactionID >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SegmentCollectionID other = (SegmentCollectionID) obj;
		if (id == null)
		{
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (internalCollectionID != other.internalCollectionID)
			return false;
		if (transactionID != other.transactionID)
			return false;
		return true;
	}
}
public class DBCASServerConnection extends CASServerConnection
{
    private IndelibleFSTransaction replicatedTransaction;	// Set if we're handling a replicated transaction
	private TransactionCommittedEvent	replicatedTransactionEvent;
	private DBCASServerConnectionStatements statements;
	// For the moment, the cache is being kept on the connection for transaction integrity
	// After we transition to true immutable objects, the cache can be moved in to the DBCASServer and shared
	private LRUQueue<SegmentCollectionID, CASIdentifier>recentCache = new LRUQueue<SegmentCollectionID, CASIdentifier>(1024);
    protected DBCASServerConnection(DBCASServer server, Connection dbConnection, EntityID openingEntity)
    {
        super(server, openingEntity);
        statements = new DBCASServerConnectionStatements(dbConnection);
    }
    
    protected DBCASServerConnection(DBCASServer server, Connection dbConnection, EntityAuthentication openingEntity)
    {
        super(server, openingEntity);
        statements = new DBCASServerConnectionStatements(dbConnection);
    }
    
    public CASServerInternal getServer()
    {
        return server;
    }

	public DBCASServerConnectionStatements getStatements()
	{
		return statements;
	}

	public CASStore getStoreAtIndex(int storeNum)
	{
		return ((DBCASServer)server).getStoreAtIndex(storeNum);
	}

	public CASStore getCASStoreForWriting(long bytesToWrite) throws IOException
	{
		return ((DBCASServer)server).getCASStoreForWriting(bytesToWrite);
	}

	CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID, int internalCollectionID) throws IOException, SegmentNotFound
	{
		return ((DBCASServer)server).retrieveSegment(this, segmentID, internalCollectionID);
	}
	
	public Future<CASIDDataDescriptor> retrieveSegmentAsync(CASIdentifier segmentID, int internalCollectionID) throws IOException, SegmentNotFound
	{
		return ((DBCASServer)server).retrieveSegmentAsync(this, segmentID, internalCollectionID);
	}

	public <A> void retrieveSegmentAsync(CASIdentifier segmentID, int internalCollectionID, AsyncCompletion<CASIDDataDescriptor, A>completionHandler,
			A attachment) throws IOException, SegmentNotFound
	{
		((DBCASServer)server).retrieveSegmentAsync(this, segmentID, internalCollectionID, completionHandler, attachment);
	}
	
	public Future<DataVersionInfo> retrieveSegmentAsync(ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException, SegmentNotFound
	{
		return ((DBCASServer)server).retrieveSegmentAsync(this, segmentID, version, flags, internalCollectionID);
	}

	public <A> void retrieveSegmentAsync(ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID, AsyncCompletion<DataVersionInfo, A>completionHandler,
			A attachment) throws IOException, SegmentNotFound
	{
		((DBCASServer)server).retrieveSegmentAsync(this, segmentID, version, flags, internalCollectionID, completionHandler, attachment);
	}
	
	CASStoreInfo storeSegment(CASIDDataDescriptor segmentDescriptor, int internalCollectionID) throws IOException
	{
		CASStoreInfo returnInfo = ((DBCASServer)server).storeSegment(this, segmentDescriptor, internalCollectionID);
		return returnInfo;
	}


	void storeVersionedSegment(ObjectID id, CASIDDataDescriptor segmentDescriptor, int internalCollectionID) throws IOException, SegmentExists
	{
		((DBCASServer)server).storeSegment(this, id, getVersionForTransaction(), segmentDescriptor, internalCollectionID);
	}
	
	<A> void storeVersionedSegmentAsync(ObjectID id, CASIDDataDescriptor segmentDescriptor, int internalCollectionID, AsyncCompletion<Void, A>completionHandler,
			A attachment) throws IOException
	{
		((DBCASServer)server).storeSegmentAsync(this, id, getVersionForTransaction(), segmentDescriptor, internalCollectionID, completionHandler, attachment);
	}

	<A> void storeSegmentAsync(CASIDDataDescriptor segmentDescriptor, int internalCollectionID, AsyncCompletion<CASStoreInfo, A>completionHandler,
			A attachment) throws IOException
	{
		((DBCASServer)server).storeSegmentAsync(this, segmentDescriptor, internalCollectionID, completionHandler, attachment);
	}
	
	DataVersionInfo retrieveSegment(ObjectID segmentID, IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		CASIdentifier casIdentifier = null;
		SegmentCollectionID retrieveKey = null;
		if (version.getVersionID() > 0)
		{
			retrieveKey = new SegmentCollectionID(segmentID, version, internalCollectionID);
			casIdentifier = recentCache.get(retrieveKey);
		}
		DataVersionInfo returnInfo = null;
		if (casIdentifier == null)
		{
			returnInfo = ((DBCASServer)server).retrieveSegment(this, segmentID, version, flags, internalCollectionID);
			if (retrieveKey != null)
				recentCache.put(retrieveKey, returnInfo.getDataDescriptor().getCASIdentifier());
		}
		else
		{
			CASIDDataDescriptor returnDescriptor = null;
			try
			{
				returnDescriptor = ((DBCASServer)server).retrieveSegment(this, casIdentifier, internalCollectionID);
			} catch (SegmentNotFound e)
			{
				// TODO Try retrieving the segment from another server
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
			if (returnDescriptor != null)
				returnInfo = new DataVersionInfo(returnDescriptor, version);
		}
		return returnInfo;
	}

	public SegmentInfo retrieveSegmentInfo(
			DBCASCollectionConnection connection, ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		SegmentInfo returnInfo = ((DBCASServer)server).retrieveSegmentInfo(this, segmentID, version, flags, internalCollectionID);
		return returnInfo;
	}	
	
	public boolean verifySegment(
			DBCASCollectionConnection connection, ObjectID segmentID,
			IndelibleVersion version, RetrieveVersionFlags flags, int internalCollectionID) throws IOException
	{
		return ((DBCASServer)server).verifySegment(this, segmentID, version, flags, internalCollectionID);
	}	

	public void repairSegment(DBCASCollectionConnection connection, ObjectID repairSegmentID, IndelibleVersion transactionVersion, DataVersionInfo masterData, int internalCollectionID) throws IOException
	{
		((DBCASServer)server).repairSegment(this, repairSegmentID, transactionVersion, masterData, internalCollectionID);
	}
	void storeMetaData(CASIDDataDescriptor metaDataDescriptor, int internalCollectionID)  throws IOException
	{
		((DBCASServer)server).storeMetaData(this, metaDataDescriptor, internalCollectionID);
	}

	void removeMetaData(int internalCollectionID) throws IOException
	{
		((DBCASServer)server).removeMetaData(this, internalCollectionID);
	}
	
	CASIDDataDescriptor retrieveMetaData(int internalCollectionID) throws IOException
	{
		return ((DBCASServer)server).retrieveMetaData(this, internalCollectionID);
	}

	boolean releaseSegment(ObjectID releaseID, int internalCollectionID) throws IOException
	{
		// TODO - Fix this to figure out the version!
		boolean released = ((DBCASServer)server).releaseSegment(this, releaseID, internalCollectionID);
		/*if (released)
		{
			SegmentCollectionID releaseKey = new SegmentCollectionID(releaseID, null, internalCollectionID);
			recentCache.remove(releaseKey);
		}*/
		return released;
	}

	boolean releaseVersionedSegment(ObjectID releaseID, IndelibleVersion version, int internalCollectionID) throws IOException
	{
		boolean released = ((DBCASServer)server).releaseVersionedSegment(this, releaseID, version, internalCollectionID);
		if (released)
		{
			SegmentCollectionID releaseKey = new SegmentCollectionID(releaseID, version, internalCollectionID);
			recentCache.remove(releaseKey);
		}
		return released;
	}

	CASIdentifier retrieveCASIdentifier(CASSegmentID segmentID, int internalCollectionID) throws IOException
	{
		return ((DBCASServer)server).retrieveCASIdentifier(this, segmentID, internalCollectionID);
	}

	public void setReplicatedTransactionEvent(TransactionCommittedEvent replicatedTransactionEvent)
	{
		this.replicatedTransaction = replicatedTransactionEvent.getTransaction();
		this.replicatedTransactionEvent = replicatedTransactionEvent;
	}
	
	public IndelibleFSTransaction getReplicatedTransaction()
	{
		return replicatedTransaction;
	}

	public TransactionCommittedEvent getReplicatedTransactionEvent()
	{
		return replicatedTransactionEvent;
	}

	public long getLastReplicatedEvent(EntityID sourceServerID, CASCollectionID collectionID) throws IOException
	{
		try
		{
			return ((DBCASServer)server).getLastReplicatedEventID(this, sourceServerID, collectionID);
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IOException("Could not retrieve last replicated ID due to SQLException");
		}
	}

	@Override
	protected void fireIndelibleEvent(IndelibleEvent eventToFire)
	{
		// TODO Auto-generated method stub
		super.fireIndelibleEvent(eventToFire);
	}
	
	public CASSegmentIDIterator listSegmentIDs(int internalCollectionID) throws IOException
	{
		return ((DBCASServer)server).listSegmentIDs(this, internalCollectionID);
	}

	@Override
	public synchronized void rollback()
	{
		recentCache.clear();	// Don't know what's committed or not so just dump it all
		super.rollback();
	}

	public IndelibleVersionIterator listVersionsForSegment(ObjectID id, int internalCollectionID) throws IOException
	{
		return  ((DBCASServer)server).listVersionsForSegment(this, id, internalCollectionID);
	}
}
