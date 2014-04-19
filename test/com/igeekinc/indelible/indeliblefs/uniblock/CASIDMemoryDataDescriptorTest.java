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
 
package com.igeekinc.indelible.indeliblefs.uniblock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.datadescriptor.BasicDataDescriptor;

import junit.framework.TestCase;

public class CASIDMemoryDataDescriptorTest extends TestCase
{
	private static final int	kOneMegabyte	= 1024*1024;
	private static final int	kTestFileSize	= kOneMegabyte * 1024;
	
	public void testFileInputPerformance()  throws IOException
    {
    	File testFile = File.createTempFile("tfip", "dat");
    	TestFilesTool.createTestFile(testFile, kTestFileSize);	// 1GB
    	FileInputStream testStream = new FileInputStream(testFile);
    	long startTime = System.currentTimeMillis();
    	long bytesRead = 0;
    	while (bytesRead < kTestFileSize)
    	{
    		CASIDMemoryDataDescriptor testDescriptor = new CASIDMemoryDataDescriptor(testStream, kOneMegabyte);
    		bytesRead += testDescriptor.getLength();
    	}
    	testStream.close();
    	long endTime = System.currentTimeMillis();
    	long elapsed = endTime - startTime;
    	System.out.println("Time to read 1GB = "+elapsed+" ms");
    	double bytesPerSecond = bytesRead / elapsed * 1000.0;
    	System.out.println("Bytes per second = "+bytesPerSecond);
    	testFile.delete();
    }
    
    public void testMappedFilePerformance()  throws IOException
    {
    	File testFile = File.createTempFile("tfip", "dat");
    	TestFilesTool.createTestFile(testFile, kTestFileSize);	// 1GB
    	FileChannel testStream = new FileInputStream(testFile).getChannel();
    	long startTime = System.currentTimeMillis();
    	long bytesRead = 0;
    	while (bytesRead < kTestFileSize)
    	{
    		CASIDMemoryDataDescriptor testDescriptor = new CASIDMemoryDataDescriptor(testStream, bytesRead, kOneMegabyte);
    		bytesRead += testDescriptor.getLength();
    	}
    	testStream.close();
    	long endTime = System.currentTimeMillis();
    	long elapsed = endTime - startTime;
    	System.out.println("Time to read 1GB = "+elapsed+" ms");
    	double bytesPerSecond = bytesRead / elapsed * 1000.0;
    	System.out.println("Bytes per second = "+bytesPerSecond);
    	testFile.delete();
    }
}
