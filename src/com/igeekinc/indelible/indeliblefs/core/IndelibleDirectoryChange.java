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

import com.igeekinc.indelible.oid.IndelibleFSObjectID;

enum ChangeType
{
    kDirectoryEntryAdd, kDirectoryEntryDelete
}

/**
 * Used to log change on a directory node for faster updates at flush time
 * @author David L. Smith-Uchida
 *
 */
public class IndelibleDirectoryChange
{
    private ChangeType changeType;
    private String name;
    private IndelibleFSObjectID childID;
    private IndelibleVersion version;
    
    public IndelibleDirectoryChange(ChangeType changeType, String name, IndelibleFSObjectID childID, IndelibleVersion version)
    {
        this.changeType = changeType;
        this.name = name;
        this.childID = childID;
        this.version = version;
    }

    public ChangeType getChangeType()
    {
        return changeType;
    }

    public String getName()
    {
        return name;
    }

    public IndelibleFSObjectID getChildID()
    {
        return childID;
    }

    public IndelibleVersion getVersion()
    {
        return version;
    }
}
