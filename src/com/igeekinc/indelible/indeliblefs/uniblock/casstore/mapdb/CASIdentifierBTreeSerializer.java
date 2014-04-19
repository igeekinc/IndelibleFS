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

import org.mapdb.BTreeKeySerializer;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;

public class CASIdentifierBTreeSerializer extends BTreeKeySerializer<Object> implements Serializable 
{
	private static final long	serialVersionUID	= -2063823176030368851L;
	protected final CASIdentifierSerializer defaultSerializer;

    CASIdentifierBTreeSerializer() {
        this.defaultSerializer = new CASIdentifierSerializer();
    }

    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
        for(int i = start;i<end;i++){
            defaultSerializer.serialize(out,(CASIdentifier)keys[i]);
        }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException{
        Object[] ret = new Object[size];
        for(int i=start; i<end; i++){
            ret[i] = defaultSerializer.deserialize(in,-1);
        }
        return ret;
    }
}

