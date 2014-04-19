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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.util.async.AsyncCompletion;
import com.igeekinc.util.datadescriptor.DummyFuture;

public class S3DataDescriptor implements CASIDDataDescriptor
{
	private CASIdentifier casIdentifier;
	private S3Object s3Object;
	private Long length;
	private transient byte [] localBuffer = null;
	
	public S3DataDescriptor(CASIdentifier identifier, S3Object s3Object)
	{
		this.casIdentifier = casIdentifier;
		this.s3Object = s3Object;
	}
	
	private static int kBufferSize = 1024*1024;
	private static int kMaxLocalBufferSize = 16*1024*1024;
	@Override
	public void writeData(OutputStream destinationStream) throws IOException
	{
		S3ObjectInputStream inputStream = s3Object.getObjectContent();
		byte [] buffer = new byte[kBufferSize];
		
		int bytesRead;
		while((bytesRead = inputStream.read(buffer)) > 0)
		{
			destinationStream.write(buffer);
		}
	}

	@Override
	public void writeData(FileOutputStream destinationStream)
			throws IOException
	{
		writeData((OutputStream)destinationStream);
	}

	@Override
	public InputStream getInputStream() throws IOException
	{
		return s3Object.getObjectContent();
	}

	@Override
	public byte[] getData() throws IOException
	{
		if (localBuffer != null)
			return localBuffer;
		localBuffer = new byte[(int)getLength()];
		int bufOffset = 0;
		int bytesReadNow;
		while((bytesReadNow = getInputStream().read(localBuffer, bufOffset, localBuffer.length - bufOffset)) > 0)
		{
			bufOffset += bytesReadNow;
		}
		if (bufOffset != getLength())
			throw new IOException("Could not read all bytes");
		s3Object.close();
		return localBuffer;
	}

	@Override
	public ByteBuffer getByteBuffer() throws IOException
	{
		byte [] returnBytes = getData();
		return ByteBuffer.wrap(returnBytes);
	}

	@Override
	public int getData(byte[] destination, int destOffset, long srcOffset,
			int length, boolean release) throws IOException
	{
		ByteBuffer dataBuffer = ByteBuffer.wrap(destination);
		dataBuffer.position(destOffset);
		return getData(dataBuffer, srcOffset, length, release);
	}

	
	@Override
	public int getData(ByteBuffer destination, long srcOffset, int length,
			boolean release) throws IOException
	{
		if (getLength() < kMaxLocalBufferSize)
		{
			if (localBuffer == null)
			{
				getData();
			}
			destination.put(localBuffer, (int)srcOffset, length);
			s3Object.close();
			return length;
		}
		else
		{
			throw new IOException("Object too large");
		}
	}

	@Override
	public long getLength()
	{
		if (length == null)
			length = s3Object.getObjectMetadata().getContentLength();
			
		return length;
	}

	@Override
	public boolean isShareableWithLocalProcess()
	{
		return false;
	}

	@Override
	public boolean isShareableWithRemoteProcess()
	{
		return false;
	}

	@Override
	public boolean isAccessible()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean descriptorContainsData()
	{
		return false;
	}

	@Override
	public void close() throws IOException
	{
		// Nothing to do
	}

	@Override
	public CASIdentifier getCASIdentifier()
	{
		return casIdentifier;
	}

	@Override
	public synchronized Future<Integer> getDataAsync(ByteBuffer destination, long srcOffset,
			int length, boolean release) throws IOException
	{
		int bytesRead = 0;
		Throwable caughtException = null;
		try
		{
			bytesRead = getData(destination, srcOffset, length, release);
		}
		catch (Throwable t)
		{
			caughtException = t;
		}
		return new DummyFuture(bytesRead, caughtException);
	}

	@Override
	public <A> void getDataAsync(ByteBuffer destination, long srcOffset,
			int length, boolean release, A attachment,
			AsyncCompletion<Integer, ? super A> handler) throws IOException
	{
		try
		{
			int bytesRead = getData(destination, srcOffset, length, release);
			handler.completed(bytesRead, attachment);
		}
		catch (Throwable t)
		{
			handler.failed(t, attachment);
		}
	}
}
