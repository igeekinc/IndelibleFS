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
import java.util.ArrayList;
import java.util.Iterator;

public class IndelibleSnapshotListIterator implements IndelibleSnapshotIterator
{
	private static final long	serialVersionUID	= -2027847164889054918L;
	ArrayList<IndelibleSnapshotInfo>list;
	private transient Iterator<IndelibleSnapshotInfo>iterator;
	public IndelibleSnapshotListIterator(ArrayList<IndelibleSnapshotInfo>list)
	{
		this.list = list;
		this.iterator = list.iterator();
	}
	@Override
	public boolean hasNext()
	{
		return iterator.hasNext();
	}

	@Override
	public IndelibleSnapshotInfo next()
	{
		return iterator.next();
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	public void close()
	{
		// no-op
	}
	
	 private void readObject(java.io.ObjectInputStream stream)
		     throws IOException, ClassNotFoundException
     {
		 stream.defaultReadObject();
		 iterator = list.iterator();
     }
}
