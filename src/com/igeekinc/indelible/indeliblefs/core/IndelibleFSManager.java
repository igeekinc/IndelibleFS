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
 
package com.igeekinc.indelible.indeliblefs.core;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventListener;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.SessionAuthentication;
import com.igeekinc.indelible.indeliblefs.server.IndelibleFSServerInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.uniblock.CASServerInternal;
import com.igeekinc.indelible.indeliblefs.uniblock.CollectionCreatedEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.CollectionNotFoundException;
import com.igeekinc.indelible.oid.CASCollectionID;
import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSManager
{
    
    protected Hashtable<IndelibleFSObjectID, CASCollectionID> volumes;
    protected CASServerInternal storageServer;
    protected Connection dbConnection;
    protected IndelibleFSServerInfo info;
    protected CASServerConnectionIF workConnection;
    protected long lastVersionTime;
    protected int lastVersionUniquifier;
    
    public IndelibleFSManager(CASServerInternal storageServer)
    throws IOException, SQLException
    {
        volumes = new Hashtable<IndelibleFSObjectID, CASCollectionID> ();
        this.storageServer = storageServer;
        storageServer.setSecurityServerID(EntityAuthenticationClient.getEntityAuthenticationClient().listTrustedServers()[0].getEntityID());
        workConnection = storageServer.open(EntityAuthenticationClient.getEntityAuthenticationClient().getEntityID());
        if (storageServer instanceof com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServer)
        	dbConnection = ((com.igeekinc.indelible.indeliblefs.uniblock.dbcas.DBCASServerConnection)workConnection).getStatements().getDBConnection();
        if (dbConnection != null)
        {
            dbConnection.setAutoCommit(false);
            try
            {
                PreparedStatement checkStatment = dbConnection.prepareStatement("select count(*) from propentries");
                checkStatment.execute();
            }
            catch (SQLException e)
            {
                dbConnection.rollback();
                initDB();
                dbConnection.commit();
            }
        }
        else
        {
        	throw new InternalError("No database connection!");
        }
        updateVolumeList();
        workConnection.addListener(new IndelibleEventListener()
		{
			
			@Override
			public void indelibleEvent(IndelibleEvent event)
			{
				if (event instanceof CollectionCreatedEvent)
				{
					try
					{
						updateVolumeList();
					} catch (IOException e)
					{
						// TODO Auto-generated catch block
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}
				}
			}
		});
    }

    private synchronized void updateVolumeList() throws IOException
    {
        CASCollectionID [] collections = workConnection.listCollections();
        for (int curVolumeNum = 0; curVolumeNum < collections.length; curVolumeNum++)
        {
            try
            {
                CASCollectionConnection curCollectionConnection = workConnection.getCollectionConnection(collections[curVolumeNum]);
                IndelibleFSObjectID curVolumeID = retrieveVolumeIDFromCollection(curCollectionConnection, workConnection);
                if (curVolumeID != null)
                    volumes.put(curVolumeID, curCollectionConnection.getCollection().getID());
                
            } 
            catch (CollectionNotFoundException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (IOException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (ClassNotFoundException e)
            {
                throw new IOException("Unknown class ");
            } catch (SQLException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (PermissionDeniedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
        }
    }
    
    private IndelibleFSObjectID retrieveVolumeIDFromCollection(
            CASCollectionConnection curCollection, CASServerConnectionIF workConnection2) throws IOException, ClassNotFoundException, SQLException, PermissionDeniedException
    {
    	HashMap<String, Serializable>fsCASMD = curCollection.getMetaDataResource(IndelibleFSCASVolume.kVolumeMetaDataPropertyName);
        if (fsCASMD != null)
        {
            Object curObject = fsCASMD.get(IndelibleFSCASVolume.kVolumeInfoPropertyName);
            if (curObject instanceof IndelibleFSCASVolume)
            {
                IndelibleFSVolume curVolume = (IndelibleFSVolume)curObject;
                return curVolume.getObjectID();
            }
        }
        return null;
    }

    private IndelibleFSVolume retrieveVolumeFromCollection(CASCollectionID curCollectionID, IndelibleFSManagerConnection workConnection) 
    throws IOException, ClassNotFoundException, SQLException, CollectionNotFoundException, PermissionDeniedException
    {
        CASCollectionConnection curCollection = workConnection.getCASConnection().getCollectionConnection(curCollectionID);
        return retrieveVolumeFromCollection(curCollection, workConnection);
    }
    
    private IndelibleFSVolume retrieveVolumeFromCollection(CASCollectionConnection curCollection, IndelibleFSManagerConnection workConnection) 
    throws IOException, ClassNotFoundException, SQLException, PermissionDeniedException
    {
    	HashMap<String, Serializable>fsCASMD = curCollection.getMetaDataResource(IndelibleFSCASVolume.kVolumeMetaDataPropertyName);
        if (fsCASMD != null)
        {
            Object curObject = fsCASMD.get(IndelibleFSCASVolume.kVolumeInfoPropertyName);
            if (curObject instanceof IndelibleFSCASVolume)
            {
                IndelibleFSCASVolume curVolume = (IndelibleFSCASVolume)curObject;
                curVolume.setConnection(workConnection);
                curVolume.setCollection(curCollection);
                return curVolume;
            }
        }
        return null;
    }
    private void initDB()
    throws SQLException
    {
        Statement initStmt = dbConnection.createStatement();
        initStmt.execute("create table propentries (id char(60), resourceName varchar(256), propertyname varchar(256), binvalue bytea, transactionid int8)");
        initStmt.execute("create index propidindex on propentries(id)");
    }
    
    public IndelibleFSManagerConnection open(EntityID authenticatedID)
    {
        CASServerConnection newCASConnection = (CASServerConnection) storageServer.open(authenticatedID);
        IndelibleFSManagerConnection returnConnection = new IndelibleFSManagerConnection(this, newCASConnection);
        return returnConnection;
    }
    
    public IndelibleFSManagerConnection open(EntityAuthentication authenticatedID)
    {
        CASServerConnection newCASConnection = (CASServerConnection) storageServer.open(authenticatedID.getEntityID());
        try
        {
        	SessionAuthentication sessionAuthentication = newCASConnection.getDataMoverSession().addAuthorizedClient(authenticatedID);
            IndelibleFSManagerConnection returnConnection = new IndelibleFSManagerConnection(this, newCASConnection);
            returnConnection.setSessionAuthentication(sessionAuthentication);
            return returnConnection;
        }
        catch(Throwable t)
        {
        	throw new InternalError("Could not authorize client");
        }


    }
    

    public void close(IndelibleFSManagerConnection closeConnection)
    {
        closeConnection.getCASConnection().close();
    }
    
    protected synchronized IndelibleFSVolume createVolume(IndelibleFSManagerConnection connection, Properties volumeProperties)
    throws IOException, PermissionDeniedException
    {
        IndelibleFSObjectID volumeID = (IndelibleFSObjectID)storageServer.getOIDFactory().getNewOID(IndelibleFSCASVolume.class);
        CASServerConnectionIF createConnection = connection.getCASConnection();
        boolean madeTransaction = false;
        if (!connection.inTransaction())
        {
            connection.startTransaction();
            madeTransaction = true;
        }
        IndelibleFSCASVolume newVolume = null;
        boolean completed = false;
        try
        {
            CASCollectionConnection volumeCollectionConnection = createConnection.createNewCollection();

            try
            {
                newVolume = new IndelibleFSCASVolume(volumeID, connection.getVersion(), volumeCollectionConnection, connection);
            } catch (SQLException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
                throw new IOException("Problem connection with database");
            }
            newVolume.setUserProperties(volumeProperties);
            newVolume.writeVolumeInfo();
            Hashtable<IndelibleFSObjectID, CASCollectionID> volumeSetToUpdate = connection.getVolumesForTransaction();
            if (volumeSetToUpdate == null)
                volumeSetToUpdate = volumes;
            volumeSetToUpdate.put(volumeID, volumeCollectionConnection.getCollection().getID());
            if (madeTransaction)
                connection.commit();
            completed = true;
        }
        finally
        {
            if (!completed && madeTransaction)
                connection.rollback();
        }
        return newVolume;
    }


    
    protected synchronized IndelibleFSVolume retrieveVolume(IndelibleFSManagerConnection connection, IndelibleFSObjectID retrieveVolumeID)
    throws VolumeNotFoundException
    {
        Hashtable<IndelibleFSObjectID, CASCollectionID> volumeSetToCheck = connection.getVolumesForTransaction();
        if (volumeSetToCheck == null)
            volumeSetToCheck = volumes;
        
        CASCollectionID collectionID = volumeSetToCheck.get(retrieveVolumeID);
        if (collectionID == null)
            throw new VolumeNotFoundException(retrieveVolumeID, "Volume ID not found");
        IndelibleFSVolume returnVolume = null;
        try
        {
            returnVolume = retrieveVolumeFromCollection(collectionID, connection);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (ClassNotFoundException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (SQLException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (CollectionNotFoundException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
        if (returnVolume == null)
            throw new VolumeNotFoundException(retrieveVolumeID, "Not found");
        return returnVolume;
    }
    
    protected synchronized IndelibleFSObjectID [] listVolumes(IndelibleFSManagerConnection connection)
    {
        Hashtable<IndelibleFSObjectID, CASCollectionID> volumeSetToList = connection.getVolumesForTransaction();
        if (volumeSetToList == null)    // Must not be in a transaction at the moment
            volumeSetToList = volumes;
        Set<IndelibleFSObjectID> volumeIDsSet = volumeSetToList.keySet();
        IndelibleFSObjectID [] returnIDs = new IndelibleFSObjectID[volumeIDsSet.size()];
        returnIDs = volumeIDsSet.toArray(returnIDs);
        return returnIDs;
    }
    
    @SuppressWarnings("unchecked")
    protected synchronized void startTransaction(IndelibleFSManagerConnection connection) throws IOException
    {
        connection.getCASConnection().startTransaction();
        connection.setVolumesForTransaction((Hashtable<IndelibleFSObjectID, CASCollectionID>)volumes.clone());
    }
    
    protected synchronized IndelibleVersion commit(IndelibleFSManagerConnection connection) throws IOException
    {
        IndelibleFSVolume [] volumesToFlush = connection.getVolumesToFlushAndClear();
        for (IndelibleFSVolume curFlushVolume:volumesToFlush)
        {
        	curFlushVolume.flush();
        }
        IndelibleVersion returnVersion = connection.getVersion();
        connection.getCASConnection().commit();
        updateVolumeList();
        connection.setVolumesForTransaction(null);
        return returnVersion;
    }
    

	public IndelibleVersion commitAndSnapshot(IndelibleFSManagerConnection connection, HashMap<String, Serializable>snapshotMetadata) throws IOException, PermissionDeniedException
	{
        IndelibleFSVolume [] volumesToFlush = connection.getVolumesToFlushAndClear();
        for (IndelibleFSVolume curFlushVolume:volumesToFlush)
        {
        	curFlushVolume.flush();
        }

        IndelibleVersion snapshotVersion = connection.getVersion();
        connection.getCASConnection().finalizeVersion(snapshotVersion);
		for (IndelibleFSVolume curVolume:volumesToFlush)
        	curVolume.addSnapshot(new IndelibleSnapshotInfo(snapshotVersion, snapshotMetadata));
        connection.getCASConnection().commit();
        updateVolumeList();
        connection.setVolumesForTransaction(null);
        return snapshotVersion;
	}
	
    protected void rollback(IndelibleFSManagerConnection connection) throws IOException
    {
    	// We need to synchronize on the connection first to avoid deadlocks
    	synchronized(connection)
    	{
    		synchronized(this)
    		{
    			IndelibleFSVolume [] volumesToFlush = connection.getVolumesToFlushAndClear();
    			for (IndelibleFSVolume curFlushVolume:volumesToFlush)
    			{
    				curFlushVolume.clearDirtyList();
    			}
    			connection.getCASConnection().rollback();
    			updateVolumeList();
    			connection.setVolumesForTransaction(null);
    		}
    	}
    }

    public EntityID getServerID()
    {
        return storageServer.getServerID();
    }

    public EntityID getSecurityServerID()
    {
        return storageServer.getSecurityServerID();
    }

}
