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

import com.igeekinc.indelible.oid.CASSegmentID;

public class IndelibleForkChange
{
    enum ChangeType
    {
        kForkEntryAdd, kForkEntryDelete
    }
    
    private ChangeType changeType;
    private int index;
    private CASSegmentID changeID;
    private IndelibleVersion version;
    
    public IndelibleForkChange(ChangeType changeType, int index, CASSegmentID changeID, IndelibleVersion version)
    {
        this.changeType = changeType;
        this.index = index;
        this.changeID = changeID;
        this.version = version;
    }

    public ChangeType getChangeType()
    {
        return changeType;
    }

    public int getOrder()
    {
        return index;
    }

    public CASSegmentID getChangeID()
    {
        return changeID;
    }

    public IndelibleVersion getVersion()
    {
        return version;
    }
}
