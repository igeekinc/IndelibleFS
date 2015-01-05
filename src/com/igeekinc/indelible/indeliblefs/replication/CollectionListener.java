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

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSTransaction;
import com.igeekinc.indelible.indeliblefs.core.IndelibleVersion;
import com.igeekinc.indelible.indeliblefs.core.RetrieveVersionFlags;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEvent;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventIterator;
import com.igeekinc.indelible.indeliblefs.events.IndelibleEventListener;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionConnection;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASCollectionSegmentEvent;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.DataVersionInfo;
import com.igeekinc.indelible.indeliblefs.uniblock.TransactionCommittedEvent;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.util.logging.DebugLogMessage;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.logging.InfoLogMessage;
import com.igeekinc.util.perf.MBPerSecondLog4jStopWatch;

class CollectionListener implements IndelibleEventListener
{
	Logger logger;
	CASCollectionConnection sourceConnection, destinationConnection;
	long firstEventID;
	
	public CollectionListener(CASCollectionConnection sourceConnection, CASCollectionConnection destinationConnection, long firstEventID)
	{
		this.sourceConnection = sourceConnection;
		this.destinationConnection = destinationConnection;
		logger = Logger.getLogger(getClass());
		this.firstEventID = firstEventID;
	}
	
	@Override
	public void indelibleEvent(IndelibleEvent event)
	{
		if (firstEventID > event.getEventID())
		{
			logger.info("Got event "+event.getEventID()+" before first expected eventID "+firstEventID+", skipping...");
			return;
		}
		if (event instanceof TransactionCommittedEvent)
		{
			TransactionCommittedEvent transactionEvent = (TransactionCommittedEvent)event;
			IndelibleFSTransaction sourceTransaction = transactionEvent.getTransaction();
			ArrayList<Future<Void>>futures = new ArrayList<Future<Void>>();
			boolean inTransaction = false;
			try
			{
				logger.info("processing transaction "+event.toString());
				IndelibleEventIterator eventsIterator = sourceConnection.getEventsForTransaction(sourceTransaction);
				destinationConnection.startReplicatedTransaction(transactionEvent);
				IndelibleVersion transactionVersion = transactionEvent.getTransaction().getVersion();
				inTransaction = true;
				while (eventsIterator.hasNext())
				{
					Log4JStopWatch eventNextWatch = new Log4JStopWatch("CollectionListener.indelibleEvent.nextEvent");
					IndelibleEvent curEvent = eventsIterator.next();
					eventNextWatch.stop();
					logger.info("processing event "+curEvent+" for transaction "+transactionEvent.getTransaction());
					if (curEvent instanceof CASCollectionEvent)
					{
						CASCollectionEvent curCASEvent = (CASCollectionEvent)curEvent;
						switch(curCASEvent.getEventType())
						{
						case kMetadataModified:
							CASIDDataDescriptor sourceMetaData = sourceConnection.getMetaDataForReplication();
							if (sourceMetaData != null)
							{
								destinationConnection.replicateMetaDataResource(sourceMetaData, curCASEvent);
								logger.info(new InfoLogMessage("Replicated metadata"));
							}
							else
							{
								logger.error(new ErrorLogMessage("Could not retrieve metadata"));
							}
							break;
						case kSegmentCreated:
							Log4JStopWatch segmentCreatedWatch = new Log4JStopWatch("CollectionListener.indelibleEvent.segmentCreated");
							ObjectID replicateSegmentID = ((CASCollectionSegmentEvent)curCASEvent).getSegmentID();
							MBPerSecondLog4jStopWatch retrieveSegmentWatch = new MBPerSecondLog4jStopWatch("CollectionListener.indelibleEvent.retrieveSegment");
							DataVersionInfo sourceInfo = sourceConnection.retrieveSegment(replicateSegmentID, transactionVersion, RetrieveVersionFlags.kExact);
							if (sourceInfo != null)
							{
								retrieveSegmentWatch.bytesProcessed(sourceInfo.getDataDescriptor().getLength());
								retrieveSegmentWatch.stop();
								MBPerSecondLog4jStopWatch storeSegmentWatch = new MBPerSecondLog4jStopWatch("CollectionListener.indelibleEvent.storeReplicatedSegment");
								Future<Void>storeFuture = destinationConnection.storeReplicatedSegmentAsync(replicateSegmentID, sourceInfo.getVersion(), sourceInfo.getDataDescriptor(), curCASEvent);
								futures.add(storeFuture);
								storeSegmentWatch.bytesProcessed(sourceInfo.getDataDescriptor().getLength());
								storeSegmentWatch.stop();
								logger.info(new ErrorLogMessage("Replicated segment "+replicateSegmentID));
							}
							else
							{
								logger.error(new ErrorLogMessage("Could not retrieve data for segment {0}", replicateSegmentID));
							}
							segmentCreatedWatch.stop();
							break;
						case kSegmentReleased:
							break;
						case kTransactionCommited:
							break;
						default:
							break;
						}
					}
					// Clean up futures that have finished
					while ((futures.size() > 0 && futures.get(0).isDone()) || futures.size() > 16)
					{
						Future<Void>curFuture = futures.remove(0);
						try
						{
							// Either it's ready to go or the queue is full so we're going to wait until it's done
							curFuture.get();
						} catch (InterruptedException e)
						{
							Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						} catch (ExecutionException e)
						{
							throw e.getCause();
						}
					}
				}
				// Wait for all of our commands to finish before moving to the commit
				while(futures.size() > 0)
				{
					Future<Void>curFuture = futures.remove(0);
					try
					{
						// Wait until it's done
						curFuture.get();
					} catch (InterruptedException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					} catch (ExecutionException e)
					{
						throw e.getCause();
					}
				}
				destinationConnection.commit();
				inTransaction = false;
			} catch (Throwable e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
			finally
			{
				if (inTransaction)
				{
					try
					{
						destinationConnection.rollback(); // Must have had some kind of an exception
					} catch (IOException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
					}	
				}
			}
		}
		else
		{
			logger.debug(new DebugLogMessage("Ignoring non-transaction event ", event.getEventID()));
		}
	}	
}