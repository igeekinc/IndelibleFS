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
 
package com.igeekinc.indelible.indeliblefs.io;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.rmi.RemoteException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleDirectoryNodeRemote;
import com.igeekinc.util.ClientFile;
import com.igeekinc.util.ClientFileMetaData;
import com.igeekinc.util.FileLikeFilenameFilter;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.exceptions.ForkNotFoundException;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSClientFile extends ClientFile
{
    private IndelibleFileNodeIF core;
    
    private static final long serialVersionUID = 1841477277272975675L;

    public IndelibleFSClientFile(ClientFile parent, String fileName) throws PermissionDeniedException
    {
        super(parent, fileName);
        if (!(parent instanceof IndelibleFSClientFile))
            throw new IllegalArgumentException("Parent must be IndelibleFSClientFile");
        if (!parent.isDirectory())
            throw new IllegalArgumentException("Parent must be a directory");
        try
        {
            core = null;
            core = ((IndelibleDirectoryNodeIF)((IndelibleFSClientFile)parent).core).getChildNode(fileName);
        } catch (RemoteException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (ObjectNotFoundException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
    }

    public IndelibleFSClientFile(IndelibleFSVolume inVolume, IndelibleFileNodeIF root)
    {
        super(inVolume, FilePath.getFilePath("/"), "/");
        this.core = root;

    }
    @Override
    public FileChannel getForkChannel(String forkName, boolean writeable)
            throws ForkNotFoundException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FileChannel getForkChannel(String forkName, boolean noCache, boolean writeable)
            throws ForkNotFoundException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getForkInputStream(String streamName)
            throws ForkNotFoundException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getForkInputStream(String streamName, boolean noCache)
            throws ForkNotFoundException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String[] getForkNames()
    {
        try
        {
            return core.listForkNames();
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        return new String[0];
    }

    @Override
    public OutputStream getForkOutputStream(String streamName)
            throws ForkNotFoundException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OutputStream getForkOutputStream(String streamName,
            boolean noCache) throws ForkNotFoundException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientFileMetaData getMetaData() throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getNumForks()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ClientFile[] listClientFiles(FilenameFilter filter)
    {
        if (core instanceof IndelibleDirectoryNodeRemote)
        {
        	IndelibleDirectoryNodeIF dirNode = (IndelibleDirectoryNodeIF)core;
            try
            {
                String [] children = dirNode.list();
                ClientFile [] returnChildren = new ClientFile[children.length];
                for (int curChildNum = 0; curChildNum < children.length; curChildNum++)
                {
                    returnChildren[curChildNum] = new IndelibleFSClientFile(this, children[curChildNum]);
                }
                return returnChildren;
            } catch (RemoteException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (IOException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (PermissionDeniedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
        }
        return new ClientFile[0];
    }

    @Override
    public void setMetaData(ClientFileMetaData newMetaData) throws IOException
    {
        // TODO Auto-generated method stub
    }

    public String[] list(FileLikeFilenameFilter filter) throws IOException
    {
        if (core instanceof IndelibleDirectoryNodeRemote)
        {
        	IndelibleDirectoryNodeIF dirNode = (IndelibleDirectoryNodeIF)core;
            try
            {
                String [] checkChildren = dirNode.list();
                ArrayList<String>returnList = new ArrayList<String>();
                for (String curCheckChild:checkChildren)
                {
                    if (filter.accept(this, curCheckChild))
                        returnList.add(curCheckChild);
                }
                String [] children = new String[returnList.size()];
                children = returnList.toArray(children);
                return children;
            } catch (RemoteException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (IOException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (PermissionDeniedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
        }
        return new String[0];
    }

    @Override
    public boolean exists()
    {
        if (core != null)
            return true;
        return false;
    }

    @Override
    public boolean isDirectory()
    {
    	if (core != null)
    		return core.isDirectory();
    	return false;
    }

    @Override
    public boolean isFile()
    {
    	if (core != null)
    		return core.isFile();
    	return true;
    }

    @Override
    public long length()
    {
        try
        {
            return core.totalLength();
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        return 0L;
    }

    @Override
    public String[] list()
    {
        if (core instanceof IndelibleDirectoryNodeIF)
        {
        	IndelibleDirectoryNodeIF dirNode = (IndelibleDirectoryNodeIF)core;
            try
            {
                String [] children = dirNode.list();
                return children;
            } catch (RemoteException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (IOException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (PermissionDeniedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
        }
        return new String[0];
    }

    @Override
    public File[] listFiles()
    {
        // TODO Auto-generated method stub
        return super.listFiles();
    }

    @Override
    public File[] listFiles(FileFilter filter)
    {
        // TODO Auto-generated method stub
        return super.listFiles(filter);
    }
    
    
}
