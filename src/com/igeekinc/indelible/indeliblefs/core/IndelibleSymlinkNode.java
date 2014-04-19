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

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleSymlinkNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;

public class IndelibleSymlinkNode extends IndelibleFileNode implements IndelibleSymlinkNodeIF
{
	private static final long serialVersionUID = 4008934319874208185L;
	private transient String targetPath = null;
    protected IndelibleSymlinkNode(IndelibleFSVolume volume, IndelibleFSObjectID objectID, IndelibleVersion version) throws IOException
    {
        super(volume, objectID, version);
    }

	@Override
	public synchronized IndelibleFSForkIF getFork(String name,
			boolean createIfNecessary) throws IOException,
			ForkNotFoundException
	{
		throw new IllegalArgumentException("Cannot read/write forks on a symbolic link");
	}

	@Override
	public synchronized IndelibleFSForkIF createOrReplaceFork(String name,
			IndelibleFSForkIF sourceFork) throws IOException
	{
		throw new IllegalArgumentException("Cannot read/write forks on a symbolic link");
	}
    
    /* (non-Javadoc)
	 * @see com.igeekinc.indelible.indeliblefs.core.IndelibleSymlinkNodeIF#getTargetPath()
	 */
    @Override
	public String getTargetPath() throws IOException
    {
    	if (targetPath == null)
    	{
    		try
			{
				IndelibleFSForkIF symlinkFork = super.getFork(kSymlinkForkName, false);
				int targetPathLength = (int)symlinkFork.length();
				if (targetPathLength > kMaxPathLen)
					targetPathLength = kMaxPathLen;	// We shouldn't be able to get here unless somebody has been doing naughty things
				byte [] targetPathUTF8 = new byte[targetPathLength];
				int bytesRead = symlinkFork.read(0, targetPathUTF8);
				if (bytesRead != targetPathLength)
					throw new IOException("Short read retrieving target path");
				targetPath = new String(targetPathUTF8, "UTF-8");
			} catch (ForkNotFoundException e)
			{
				throw new IOException("Could not retrieve sym link fork", e);
			}	
    	}
    	
    	return targetPath;
    }
    
    public void setTargetPath(String targetPath) throws IOException
    {
    	if (targetPath == null)
    		throw new IllegalArgumentException("targetPath cannot be null");
        try
		{
			IndelibleFSForkIF symlinkFork = super.getFork(kSymlinkForkName, true);
			byte [] targetPathBytes = targetPath.getBytes("UTF-8");
			if (targetPathBytes.length > kMaxPathLen)
				throw new IllegalArgumentException("targetPath length of "+targetPath.length()+" exceeds max length of "+kMaxPathLen);
			symlinkFork.append(targetPathBytes);
		} catch (ForkNotFoundException e)
		{
			throw new IOException("Could not create sym link fork", e);
		}
    }
}
