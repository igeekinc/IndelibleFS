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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.mapdb.Serializer;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;

public class CASIdentifierSerializer implements Serializable, Serializer<CASIdentifier>
{
	private static final long	serialVersionUID	= -8733544706384384131L;

	@Override
	public void serialize(DataOutput out, CASIdentifier value)
			throws IOException
	{
		out.write(value.getBytes());
	}

	@Override
	public CASIdentifier deserialize(DataInput in, int available)
			throws IOException
	{
		byte [] valueBytes = new byte[CASIdentifier.kCASIdentifierSize];
		in.readFully(valueBytes);
		return new CASIdentifier(valueBytes);
	}

}
