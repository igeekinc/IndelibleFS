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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.dbfs;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

class FSCASSerialStorageDirectoryFilter implements FileFilter
{

    public boolean accept(File pathname)
    {
        String name = pathname.getName();
        if (name.length() == FSCASSerialStorage.kDigitsPerDirLevel)
        {
            for (char checkChar:name.toCharArray())
            {
                if (!Character.isDigit(checkChar))
                    return false;
            }
            return true;
        }
        else
        {
            return false;
        }
    }
}

class CASSegSerialFilter implements FilenameFilter
{
    public boolean accept(File dir, String name)
    {
        return (name.endsWith(".casseg"));
    }
}

/**
 * FSCASSerialStorageDirectory handles a tree of directories storing CAS segments by their serial ID (not the CAS id).  The serial ID
 * basically just increments by one each time a new segment is added.  We will not worry about wrapping at this time or freeing space.
 * 
 * A leaf directory will hold up to 1000 files (0-999).  Each interior node directory will hold up to 1000 sub directories.  Splits are done 
 * at the root.  E.g.
 * 
 *          Root
 *   Segment0-Segment999
 *   
 *   becomes
 *                  Root
 *      000                     001     
 *  Segment0-Segment999    Segment1000
 *  
 *   becomes
 *                  Root
 *      000   ...               999
 *      
 *   becomes
 *                  Root
 *      000                     001
 *   000 ... 999            000
 *                      Segment 1000000
 *                      
 */

class FilenameComparator implements Comparator<String>
{

	@Override
	public int compare(String o1, String o2)
	{
		if (o1.endsWith(".casseg") && o2.endsWith("casseg"))
		{
			o1 = o1.substring(0, o1.length() - 7);
			o2 = o2.substring(0, o2.length() - 7);
			long o1Long = Long.parseLong(o1);
			long o2Long = Long.parseLong(o2);
			long diff = (o1Long - o2Long);
			if (diff < 0)
				return -1;
			if (diff > 0)
				return 1;
			return 0;
		}
		throw new IllegalArgumentException("Missing .casseg o1 = "+o1+" o2 = "+o2);
	}
	
}
public class FSCASSerialStorageDirectory
{
    File directory;
    int level;
    int numSegmentFiles = 0;
    int height = -1;
    HashMap<String, FSCASSerialStorageDirectory>childDirMap;
    
    public FSCASSerialStorageDirectory(File directory, int level)
    {
        this.directory = directory;
        this.level = level;
    }

	protected synchronized void loadMap()
	{
		if (childDirMap == null)
		{
			childDirMap = new HashMap<String, FSCASSerialStorageDirectory>();
			File [] childDirs = directory.listFiles(new FSCASSerialStorageDirectoryFilter());
			for (File curChildDir:childDirs)
			{
				FSCASSerialStorageDirectory curChildNode = new FSCASSerialStorageDirectory(curChildDir, level+1);
				childDirMap.put(curChildDir.getName(), curChildNode);
			}
			numSegmentFiles = childDirMap.size();
		}
	}

    public void addFile(File casSegmentFile)
    {
    	loadMap();
        numSegmentFiles++;
    }
    
    public int getNumSegmentFiles()
    {
    	loadMap();
        return numSegmentFiles;
    }
    
    /**
     * @return - how many levels from the bottom of the tree this node is (0 == leaf node holding segment files)
     */
    public int height()
    {
    	loadMap();
        if (height < 0)
        {
            // Uninitialized - figure out our height.  This will be unchanged as splits are handled by pushing the root node
            // down one level (which will change its depth, but not its height in the tree)
            if (childDirMap.size() == 0)
            {
                height = 0;
            }
            else
            {
                FSCASSerialStorageDirectory aChild = childDirMap.entrySet().iterator().next().getValue();   // All children 
                height = aChild.height() + 1;
            }
        }
        return height;
    }

    public synchronized FSCASSerialStorageDirectory getChildDir(String curDirName)
    {
    	loadMap();
        FSCASSerialStorageDirectory returnDir = childDirMap.get(curDirName);
		return returnDir;
    }
    
    public File getDirectory()
    {
        return directory;
    }

    public synchronized FSCASSerialStorageDirectory addChildDir(String dirName)
    {
    	loadMap();
        FSCASSerialStorageDirectory newNode = childDirMap.get(dirName);
        if (newNode == null)
        {
            File newChildDir = new File(directory, dirName);

            if (newChildDir.exists())
            {
                if (newChildDir.isDirectory())
                {
                    // Hmmm...we missed this somehow?  Just make a node and add it I guess
                    newNode = new FSCASSerialStorageDirectory(newChildDir, level + 1);
                    childDirMap.put(dirName, newNode);
                    return newNode;
                }
                else
                {
                    throw new InternalError(newChildDir.getAbsolutePath()+" should be a directory but is not");
                }
            }
            else
            {
                throw new InternalError(newChildDir.getAbsolutePath()+" should exist but does not");
            }
        }
        return newNode;
    }
    
    public synchronized FSCASSerialStorageDirectory firstDir()
    {
    	loadMap();
    	FSCASSerialStorageDirectory returnFile = null;
    	if (childDirMap.size() > 0)
    	{
    		String [] childDirNames = childDirMap.keySet().toArray(new String[childDirMap.size()]);
    		Arrays.sort(childDirNames);
    		returnFile = childDirMap.get(childDirNames[0]);
    	}
    	return returnFile;
    }
    
    public synchronized File firstFile()
    {
    	loadMap();
    	File returnFile = null;
    	String [] segmentFiles = directory.list(new CASSegSerialFilter());
    	if (segmentFiles.length > 0)
    	{
    		Arrays.sort(segmentFiles, new FilenameComparator());
    		returnFile = new File(directory, segmentFiles[0]);
    	}
    	return returnFile;
    }
    
    public synchronized FSCASSerialStorageDirectory lastDir()
    {
    	loadMap();
    	FSCASSerialStorageDirectory returnFile = null;
    	if (childDirMap.size() > 0)
    	{
    		String [] childDirNames = childDirMap.keySet().toArray(new String[childDirMap.size()]);
    		Arrays.sort(childDirNames);
    		returnFile = childDirMap.get(childDirNames[childDirNames.length - 1]);
    	}
    	return returnFile;
    }
    
    public synchronized File lastFile()
    {
    	loadMap();
    	File returnFile = null;
    	String [] segmentFiles = directory.list(new CASSegSerialFilter());
    	if (segmentFiles.length > 0)
    	{
    		Arrays.sort(segmentFiles, new FilenameComparator());
    		returnFile = new File(directory, segmentFiles[segmentFiles.length - 1]);
    	}
    	return returnFile;
    }
    
}
