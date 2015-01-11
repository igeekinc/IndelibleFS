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

import java.io.IOException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.util.ClientFile;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.User;
import com.igeekinc.util.Volume;
import com.igeekinc.util.VolumeBootInfo;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSVolume extends Volume
{
    private static final long serialVersionUID = 2544626418541684228L;
    IndelibleFSVolumeIF indelibleFSVolume;
    
    public IndelibleFSVolume(IndelibleFSVolumeIF indelibleFSVolume)
    {
        this.indelibleFSVolume = indelibleFSVolume;
    }
    
    @Override
    public void allowDismount() throws IOException
    {
        // Always allowed
    }

    @Override
    public void enablePermissions() throws IOException
    {
        // always enabled
    }

    @Override
    public long filesInUse()
    {
        // always zero
        return 0;
    }

    @Override
    public long freeSpace()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public VolumeBootInfo getBootInfo() throws IOException
    {
        // No boot info
        return null;
    }

    @Override
    public ClientFile getClientFile(ClientFile parent, String fileName)
            throws IOException
    {
        try
		{
			return new IndelibleFSClientFile(parent, fileName);
		} catch (PermissionDeniedException e)
		{
			throw new IOException("Permission denied");
		}
    }

    @Override
    public ClientFile getClientFile(String basePath, String relativePath)
            throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getDeviceName()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getFsType()
    {
        return "indeliblefs";
    }

    @Override
    public ClientFile getRelativeClientFile(String relativePathName)
            throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientFile getRelativeClientFile(FilePath partialPath)
            throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientFile getRoot()
    {
        try
        {
            IndelibleDirectoryNodeIF rootNode = indelibleFSVolume.getRoot();
            IndelibleFSClientFile returnFile = new IndelibleFSClientFile(this, rootNode);
            return returnFile;
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        return null;
    }

    @Override
    public ClientFile getTrashDirectory(User user)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getVolumeName()
    {
        try
        {
            return indelibleFSVolume.getVolumeName();
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        return "";
    }

    @Override
    public void inhibitDismount() throws IOException
    {
        // Nothing to do
    }

    @Override
    public boolean isBootVolume()
    {
        // Never the boot volume
        return false;
    }

    @Override
    public boolean isBootable() throws IOException
    {
        // Never bootable
        return false;
    }

    @Override
    public boolean isExternal()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isOnline()
    {
        // Always true
        return true;
    }

    @Override
    public boolean isReadOnly()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRemote()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRemovable()
    {
        // Always false
        return false;
    }

    @Override
    public void makeBootable(VolumeBootInfo newBootInfo) throws IOException
    {
        // Nothing to do (yet)
    }

    @Override
    public long totalSpace()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    public Object getVolumeID()
    {
    	return indelibleFSVolume.getVolumeID();
    }

    public IndelibleFSVolumeIF getIndelibleFSRemoteVolume()
    {
        return indelibleFSVolume;
    }
}
