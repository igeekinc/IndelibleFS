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
 
package com.igeekinc.indelible.indeliblefs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.igeekinc.indelible.indeliblefs.core.IndelibleFSFork;

public class IndelibleFSForkInputStream extends InputStream
{
    protected IndelibleFSForkIF inputFork;
    protected long offset, mark;
    protected boolean closed = false;
    
    public IndelibleFSForkInputStream(IndelibleFSForkIF inputFork)
    {
        this.inputFork = inputFork;
        offset = 0;
        closed = false;
    }

    public void close() throws IOException
    {
        closed = true;
        inputFork = null;
    }


    public synchronized void mark(int readlimit)
    {
        mark = offset;
    }


    public boolean markSupported()
    {
        return true;
    }


    public int read(byte[] b, int off, int len) throws IOException
    {
        if (closed)
            throw new IOException("Stream is closed");
        int bytesRead = inputFork.read(offset, b, off, len);
        if (bytesRead > 0)
            offset += bytesRead;
        return bytesRead;
    }


    public int read(byte[] b) throws IOException
    {
        if (closed)
            throw new IOException("Stream is closed");
        return read(b, 0, b.length);
    }


    public synchronized void reset() throws IOException
    {
        mark = 0;
    }


    public long skip(long n) throws IOException
    {
        long oldOffset = offset;
        offset += n;
        long forkLength = inputFork.length();
        if (offset > forkLength)
            offset = forkLength;
        return offset-oldOffset;
    }


    public int read() throws IOException
    {
        if (closed)
            throw new IOException("Stream is closed");
        byte [] littleBuf = new byte[1];
        try
        {
            inputFork.read(offset, littleBuf);
        } catch (EOFException e)
        {
            return -1;
        }
        offset++;
        return (int)littleBuf[0];
    }
}
