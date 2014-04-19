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
 
package com.igeekinc.indelible.indeliblefs.bufcache;

import java.io.File;

import junit.framework.TestCase;

import com.igeekinc.indelible.indeliblefs.bufcache.filebufcache.FileBufferCache;

public class FileBufferCacheTest extends TestCase
{
    public void testAlpha()
    throws Exception
    {
        File testDir = new File(System.getProperty("com.igeekinc.tests.testtemp"));
        File testFile = new File(testDir, "FileBufferCacheTestData");
        FileBufferCache bufferCache = new FileBufferCache(testFile, null, false);
        
        for (long curBufNum = 0; curBufNum < 128; curBufNum ++)
        {
            long curOffset = curBufNum * 1024 * 1024;
            Buffer curBuffer = bufferCache.getBuffer(curOffset, 1024*1024);
            curBuffer.forWriting();
            for (int curByteNum = 0; curByteNum < 1024*1024; curByteNum++)
                	curBuffer.setByte(curByteNum, (byte)curByteNum);
            curBuffer.release();
        }
        bufferCache.flush();
    }
}
