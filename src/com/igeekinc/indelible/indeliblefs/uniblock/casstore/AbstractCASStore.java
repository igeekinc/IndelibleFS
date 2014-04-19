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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.EventObject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.BasicDataDescriptorFactory;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.exceptions.SegmentNotFound;
import com.igeekinc.util.ChangeModel;
import com.igeekinc.util.CheckCorrectDispatchThread;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.logging.ErrorLogMessage;

class CASStoreChangeModel extends ChangeModel
{

	public CASStoreChangeModel(CheckCorrectDispatchThread checker)
	{
		super(checker);
	}
	
	public void addStatusChangeListener(CASStoreStatusChangedListener listener)
	{

	}

	public void removeStatusChangeListener(CASStoreStatusChangedListener listener)
	{
		
	}
}

class DummyStoreFuture implements Future<Void>
{
	private Throwable exception;
	public DummyStoreFuture(Throwable exception)
	{
		this.exception = exception;
	}
	
	@Override
	public boolean cancel(boolean paramBoolean)
	{
		return false;
	}

	@Override
	public boolean isCancelled()
	{
		return false;
	}

	@Override
	public boolean isDone()
	{
		return true;
	}

	@Override
	public Void get() throws InterruptedException, ExecutionException
	{
		if (exception != null)
			throw new ExecutionException(exception);
		return null;
	}

	@Override
	public Void get(long paramLong, TimeUnit paramTimeUnit)
			throws InterruptedException, ExecutionException, TimeoutException
	{
		if (exception != null)
			throw new ExecutionException(exception);
		return null;
	}
}

class DummyRetrieveFuture implements Future<CASIDDataDescriptor>
{
	private CASIDDataDescriptor casIDDataDescriptor;
	private Throwable exception;

	public DummyRetrieveFuture(CASIDDataDescriptor casIDDataDescriptor, Throwable exception)
	{
		this.casIDDataDescriptor = casIDDataDescriptor;
		this.exception = exception;
	}
	
	@Override
	public boolean cancel(boolean paramBoolean)
	{
		return false;
	}

	@Override
	public boolean isCancelled()
	{
		return false;
	}

	@Override
	public boolean isDone()
	{
		return true;
	}

	@Override
	public CASIDDataDescriptor get() throws InterruptedException, ExecutionException
	{
		if (exception != null)
			throw new ExecutionException(exception);
		return casIDDataDescriptor;
	}

	@Override
	public CASIDDataDescriptor get(long paramLong, TimeUnit paramTimeUnit)
			throws InterruptedException, ExecutionException, TimeoutException
	{
		if (exception != null)
			throw new ExecutionException(exception);
		return casIDDataDescriptor;
	}
}
/**
 * Base class that can be used for all stores - handles cas status and events
 *
 */
public abstract class AbstractCASStore extends BasicDataDescriptorFactory implements CASStore
{
	private ChangeModel changeModel;
	private CASStoreStatus status;
	protected Logger	logger;
	
	public AbstractCASStore(CheckCorrectDispatchThread checker)
	{
		logger = Logger.getLogger(getClass());
		changeModel = new ChangeModel(checker);
	}

	public CASStoreStatus getStatus()
	{
		return status;
	}

	public void setStatus(CASStoreStatus status)
	{
		this.status = status;
		EventObject eventToFire = new CASStoreStatusChangedEvent(this, status);
		try
		{
			changeModel.fireEventOnCorrectThread(eventToFire, CASStoreStatusChangedListener.class, 
					CASStoreStatusChangedListener.class.getMethod("casStoreStatusChangedEvent", CASStoreStatusChangedEvent.class));
		} catch (SecurityException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		} catch (RuntimeException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		} catch (NoSuchMethodException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
	}

	@Override
	public void addStatusChangeListener(CASStoreStatusChangedListener listener)
	{
		changeModel.addEventListener(listener);
	}

	@Override
	public void removeStatusChangeListener(CASStoreStatusChangedListener listener)
	{
		changeModel.removeEventListener(listener);
	}

	@Override
	public Future<CASIDDataDescriptor> retrieveSegmentAsync(CASIdentifier segmentID) throws IOException, SegmentNotFound
	{
		DummyRetrieveFuture returnFuture;
		try
		{
			CASIDDataDescriptor returnCASIDDataDescriptor = retrieveSegment(segmentID);
			returnFuture = new DummyRetrieveFuture(returnCASIDDataDescriptor, null);
		}
		catch (Throwable t)
		{
			returnFuture = new DummyRetrieveFuture(null, t);
		}
		return returnFuture;
	}

	
	@Override
	public <A> void retrieveSegmentAsync(CASIdentifier segmentID,
			AsyncCompletion<CASIDDataDescriptor, A> completionHandler,
			A attachment) throws IOException, SegmentNotFound
	{
		try
		{
			CASIDDataDescriptor returnCASIDDataDescriptor = retrieveSegment(segmentID);
			completionHandler.completed(returnCASIDDataDescriptor, attachment);
		}
		catch(Throwable t)
		{
			completionHandler.failed(t, attachment);
		}
	}

	@Override
	public Future<Void> storeSegmentAsync(CASIDDataDescriptor segmentDescriptor)
			throws IOException
	{
		DummyStoreFuture returnFuture;
		try
		{
			storeSegment(segmentDescriptor);
			returnFuture = new DummyStoreFuture(null);
		}
		catch (Throwable t)
		{
			returnFuture = new DummyStoreFuture(t);
		}
		return returnFuture;
	}

	@Override
	public <A> void storeSegmentAsync(CASIDDataDescriptor segmentDescriptor,
			AsyncCompletion<Void, A> completionHandler, A attachment)
			throws IOException
	{
		try
		{
			storeSegment(segmentDescriptor);
			completionHandler.completed(null, attachment);
		}
		catch(Throwable t)
		{
			completionHandler.failed(t, attachment);
		}
	}
	
	@Override
	public void repairSegment(CASIdentifier casID,
			CASIDDataDescriptor dataDescriptor) throws IOException
	{
		throw new UnsupportedOperationException();
	}
}
