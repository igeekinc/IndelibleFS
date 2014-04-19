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

import java.io.IOException;

public interface BufferCache<I>
{
    /**
     * Returns the buffer containing the data at offset in the backing store of offset and size size.
     * 
     * @param offset
     * @param size
     * @return
     * @throws IOException
     */
    public Buffer getBuffer(I id, int size) throws IOException;
    
    /**
     * Allocates space using the Allocator and returns a buffer for it if possible
     * @param size
     * @return
     * @throws IOException
     */
    public Buffer getBuffer(int size) throws IOException;
    /**
     * Flushes all of the dirty buffers to the backing store
     * @throws IOException
     */
    public void flush() throws IOException;
    
    /**
     * Flushes the specified buffer to disk (normally called through the
     * Buffer's flush() method.
     * @param bufferToFlush
     * @throws IOException
     */
    void flushBuffer(Buffer bufferToFlush) throws IOException;
}
