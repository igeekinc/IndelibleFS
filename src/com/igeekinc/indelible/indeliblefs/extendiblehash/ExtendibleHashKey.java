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
 
package com.igeekinc.indelible.indeliblefs.extendiblehash;

import com.igeekinc.util.BitTwiddle;

public class ExtendibleHashKey implements Comparable<ExtendibleHashKey>
{
    private byte [] keyBytes;
    public ExtendibleHashKey(byte [] inKeyBytes)
    {
        keyBytes = new byte[inKeyBytes.length];
        System.arraycopy(inKeyBytes, 0, keyBytes, 0, inKeyBytes.length);
    }
    
    public byte [] getBytes()
    {
         byte [] returnBytes = new byte[keyBytes.length];
         System.arraycopy(keyBytes, 0, returnBytes, 0, keyBytes.length);
         return(returnBytes);
    }
    
    
    public int hashCode()
    {
        int hashCode = 0;
        for (int curOffset = 0; curOffset < keyBytes.length; curOffset+= 4)
        {
            int curChunk = 0;
            if (curOffset + 4 <= keyBytes.length)
                curChunk = BitTwiddle.javaByteArrayToInt(keyBytes, curOffset);
            else
            {
                while (curOffset < keyBytes.length)
                {
                    curChunk |= keyBytes[curOffset];
                    curChunk = curChunk << 8;
                    curOffset++;
                }
            }
            hashCode ^= curChunk;
        }
        return hashCode;
    }
    public String toString()
    {
        StringBuffer returnBuf = new StringBuffer(keyBytes.length*2);
        for (int curByteNum = 0; curByteNum < keyBytes.length; curByteNum++)
        {
            returnBuf.append(BitTwiddle.toHexString(keyBytes[curByteNum], 2));
        }
        return returnBuf.toString();
    }
    
    
    public int compareTo(ExtendibleHashKey compareKey)
    {
        if (compareKey.keyBytes.length != keyBytes.length)
            throw new IllegalArgumentException("Key lengths do not match");
        for (int curCompareByteNum = 0; curCompareByteNum < keyBytes.length; curCompareByteNum++)
        {
            int myByte = ((int)keyBytes[curCompareByteNum]) & 0xff;
            int compareByte = ((int)compareKey.keyBytes[curCompareByteNum]) & 0xff;
            int retVal = myByte - compareByte;
            if (retVal != 0)
                return retVal;
        }
        return 0;
    }
    
    public boolean equals(Object obj)
    {
        if (compareTo((ExtendibleHashKey)obj) == 0)
            return true;
        return false;
    }
    
    public int getSize()
    {
        return keyBytes.length;
    }
}
