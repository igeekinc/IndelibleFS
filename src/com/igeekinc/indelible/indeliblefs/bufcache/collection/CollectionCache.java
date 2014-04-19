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
 
package com.igeekinc.indelible.indeliblefs.bufcache.collection;

import java.io.IOException;

import com.igeekinc.indelible.indeliblefs.bufcache.AbstractBufferCache;
import com.igeekinc.indelible.indeliblefs.bufcache.Buffer;
import com.igeekinc.indelible.oid.CASSegmentID;

public class CollectionCache extends AbstractBufferCache<CASSegmentID>
{

	@Override
	public Buffer getBuffer(CASSegmentID id, int size) throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Buffer getBuffer(int size) throws IOException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() throws IOException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void flushBuffer(Buffer bufferToFlush) throws IOException
	{
		// TODO Auto-generated method stub

	}

}
