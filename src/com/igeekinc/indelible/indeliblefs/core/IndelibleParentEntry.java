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

import java.io.Serializable;

import com.igeekinc.indelible.oid.IndelibleFSObjectID;

public class IndelibleParentEntry implements Serializable
{
    private static final long serialVersionUID = 7729730709024365304L;
    private String nameInParent;
    private IndelibleFSObjectID parentID;
    
    public IndelibleParentEntry(IndelibleDirectoryNode parent, String nameInParent)
    {
        this.parentID = parent.getObjectID();
        this.nameInParent = nameInParent;
    }
    public IndelibleParentEntry(IndelibleFSObjectID parentID, String nameInParent)
    {
        this.parentID = parentID;
        this.nameInParent = nameInParent;
    }
    public String getNameInParent()
    {
        return nameInParent;
    }
    public IndelibleFSObjectID getParentID()
    {
        return parentID;
    }
}
