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

import java.util.Iterator;

import com.igeekinc.indelible.indeliblefs.bufcache.Buffer;
import com.igeekinc.util.BitTwiddle;


/**
 * Implements a Hashtable inside of a Buffer.  
 * Rationale: By making the hashtable structure usable without deserializing
 * the buffer it's possible to swap large tables in and out rapidly
 * The buffer is split into the hashtable section which contains pointers into
 * the entries section.  Hashtable is implemented as a hashtable with chaining.
 * Separation of the hashtable from the entries allows for a lower hashtable utilization
 * without large numbers of unused entries.
 * 
 * @author David Smith-Uchida
 *
 */

abstract class EntriesIterator<T> implements Iterator<T>
{
    BufferHashtable table;
    short entriesSeen, curEntry;
    boolean curEntryRemoved = false;
    
    EntriesIterator(BufferHashtable inTable)
    {
        table = inTable;
        curEntry = -1;
        entriesSeen = 0;
        if (table.size() == 0)
            return;

    }
    public boolean hasNext()
    {
        if (entriesSeen < table.size())
            return true;
        return false;
    }
    public T next()
    {
        if (entriesSeen == table.size())
            return null;
        curEntry++;
        // Roll forward to first used entry
        while (table.getNext(curEntry) == BufferHashtable.kUnusedEntry)
            curEntry++;
        T returnObj = getReturnBytes(curEntry);
        entriesSeen++;
        curEntryRemoved = false;
        return returnObj;
        
    }
    
    protected abstract T getReturnBytes(short entryNum);
    
    public void remove()
    {
        if (curEntryRemoved)
            throw new IllegalStateException("current entry already removed");
       if (curEntry == -1)	// We haven't moved yet
           throw new IllegalStateException("next not called yet");
       byte [] removeKeyBytes = table.getEntryKey(curEntry);
       ExtendibleHashKey removeKey = new ExtendibleHashKey(removeKeyBytes);
       table.remove(removeKey);
       entriesSeen--;
       curEntryRemoved = true;
    }
}

class KeyIterator extends EntriesIterator<ExtendibleHashKey>
{
    KeyIterator(BufferHashtable inTable)
    {
        super(inTable);
    }
    
    protected ExtendibleHashKey getReturnBytes(short entryNum)
    {
        ExtendibleHashKey returnKey = new ExtendibleHashKey(table.getEntryKey(entryNum));
        return (returnKey);
    }
}

class ValuesIterator extends EntriesIterator<byte []>
{
    ValuesIterator(BufferHashtable inTable)
    {
        super(inTable);
    }
    
    protected byte[] getReturnBytes(short entryNum)
    {
        return table.getEntryValue(entryNum);
    }
}
public class BufferHashtable
{
    /*
     * Internal routines for dealing with an entry in a buffer hashtable
     * All values are stored in the buffer, so we just have static routines
     * to read/write the entries that reside there
     * 
     * 
     * Header format
     *  signature - 5 bytes = hShTb
     *  keysize - 1 byte
     *  recordsize - 2 bytes
     *  length - 4 bytes (includes header)
     *  tableSize - 4 bytes (in entries) size of the hash table portion
     *  maxEntries - 4 bytes (in entries) number of entry slots available
     *  numEntries - 4 bytes (in entries) number of entry slots used
     *  type - 4 bytes
     *
     * Hash table format
     *  2 bytes - entry number
     * 
     * Entry format:
     *  2 bytes - next chained record (-1 = end of chain, -2 = record unused)
     *  keySize bytes [] - key
     *  recordSize bytes [] - record
     */

    static final short kEndOfChain = -1;
    static final short kUnusedEntry = -2;
    private int numEntries, recordSize, keySize, type;
    private Buffer buffer;
    private int entrySize;
    private int tableSize, maxEntries;	// in entries
    private short nextEntryIndex;
    
    private static int kKeyOffset = 2;
    
    private static final int kUnused = 0;
    private static final int kUsed = 1;
    private static final int kHeaderSize = 16;
    private static final byte [] kSignature = {'h', 'S', 'h','T','b'};
    
    private static final int kHeaderSignatureOffset = 0;
    private static final int kSignatureSize = 5;
    private static final int kHeaderKeySizeOffset = kHeaderSignatureOffset + kSignatureSize;
    private static final int kHeaderRecordSizeOffset = 6;
    private static final int kHeaderLengthOffset = 8;
    private static final int kHeaderTypeOffset = 12;
    
    private short getHashVal(int hashTableIndex)
    {
        short returnVal;
        byte [] hashValBytes = new byte[2];
        
        int hashTableOffset = kHeaderSize + hashTableIndex * 2;
        buffer.forReading();
        try
        {
            buffer.getBytes(hashTableOffset, hashValBytes, 0, 2);
        }
        finally
        {
            buffer.release();
        }
        returnVal = BitTwiddle.javaByteArrayToShort(hashValBytes, 0);
        return(returnVal);
    }
    
    private void setHashVal(int hashTableIndex, short newHashVal)
    {
        byte [] hashValBytes = new byte[2];
        BitTwiddle.shortToJavaByteArray(newHashVal, hashValBytes, 0);
        int hashTableOffset = kHeaderSize + hashTableIndex * 2;
        buffer.forWriting();
        try
        {
            buffer.setBytes(hashTableOffset, hashValBytes);
        }
        finally
        {
            buffer.release();
        }
        return;
    }
    
    private int offset(short entryNum)
    {
        return(entryNum * entrySize + kHeaderSize + tableSize * 2);
    }
    
    short getNext(short entryNum)
    {
        int offset = offset(entryNum);
        byte [] nextBytes = new byte[2];
        buffer.forReading();
        try
        {
            buffer.getBytes(offset, nextBytes, 0, 2);
        }
        finally
        {
            buffer.release();
        }
        short returnNext = BitTwiddle.javaByteArrayToShort(nextBytes, 0);
        return returnNext;
    }
    
    private void setNext(short entryNum, short newNext)
    {
        int offset = offset(entryNum);
        byte [] nextBytes = new byte[2];
        BitTwiddle.shortToJavaByteArray(newNext, nextBytes, 0);
        buffer.forWriting();
        try
        {
            buffer.setBytes(offset, nextBytes);
        }
        finally
        {
            buffer.release();
        }
    }
    
    byte [] getEntryKey(short entryNum)
    {
        byte [] returnBytes = new byte[keySize];
        int offset = offset(entryNum) + kKeyOffset;
        buffer.forReading();
        try
        {
            buffer.getBytes(offset, returnBytes, 0, recordSize);
        }
        finally
        {
            buffer.release();
        }
        return(returnBytes);
    }
    
    
    private void setEntryKey(short entryNum, byte [] keyVal)
    {
        int offset = offset(entryNum) + kKeyOffset;
        buffer.forWriting();
        try
        {
            buffer.setBytes(offset, keyVal, 0,keySize);   
        }
        finally
        {
            buffer.release();
        }
    }
    
    byte [] getEntryValue(short entryNum)
    {
        byte [] returnBytes = new byte[recordSize];
        int offset = offset(entryNum);
        offset = offset + kKeyOffset + keySize;
        buffer.forReading();
        try
        {
            buffer.getBytes(offset, returnBytes, 0, recordSize);
        }
        finally
        {
            buffer.release();
        }
        return(returnBytes);
    }
    
    private void setEntryValue(short entryNum, byte [] entryVal)
    {
        int offset = offset(entryNum);
        offset = offset + kKeyOffset + keySize;
        buffer.forWriting();
        try
        {
            buffer.setBytes(offset, entryVal, 0, recordSize);
        }
        finally
        {
            buffer.release();
        }
    }
    
    private void headerToBuffer()
    {
        buffer.forWriting();
        try
        {
            buffer.setBytes(kHeaderSignatureOffset, kSignature);
            buffer.setByte(kHeaderKeySizeOffset, (byte)keySize);
            byte [] recordSizeBytes = new byte[2];
            BitTwiddle.shortToJavaByteArray((short)recordSize, recordSizeBytes, 0);
            buffer.setBytes(kHeaderRecordSizeOffset, recordSizeBytes);
            byte [] lengthBytes = new byte[4];
            BitTwiddle.intToJavaByteArray(buffer.getSize(), lengthBytes, 0);
            buffer.setBytes(kHeaderLengthOffset, lengthBytes);
            byte [] typeBytes = new byte[4];
            BitTwiddle.intToJavaByteArray(type, typeBytes, 0);
            buffer.setBytes(kHeaderTypeOffset, typeBytes);
        }
        finally
        {
            buffer.release();
        }
    }

    private short nextEntryIndex()
    {
        if (numEntries == maxEntries)
            return kEndOfChain;
        short returnIndex, startIndex;
        startIndex = nextEntryIndex;
        while (getNext(nextEntryIndex) != kUnusedEntry)
        {
            nextEntryIndex = (short)((nextEntryIndex + 1) % maxEntries);
            if (startIndex == nextEntryIndex) // We've been here before
                throw new InternalError("No unused entries but numEntries != maxEntries");
        }
        returnIndex = nextEntryIndex;
        if (numEntries != maxEntries)
        {
            while (getNext(nextEntryIndex) != kUnusedEntry)
            {
                nextEntryIndex = (short)((nextEntryIndex + 1) % maxEntries);
                if (startIndex == nextEntryIndex) // We've been here before
                    throw new InternalError("No unused entries but numEntries != maxEntries");
            }
        }
        return returnIndex;
        
    }
    public BufferHashtable(int inRecordSize, int inKeySize, Buffer inBuffer, int inType)
    {
        buffer = inBuffer;
        recordSize = inRecordSize;
        keySize = inKeySize;
        type = inType;
        entrySize = 2+keySize+recordSize;
        int availableSpace = inBuffer.getSize() - kHeaderSize;
        maxEntries = availableSpace/entrySize;
        tableSize = (int)(maxEntries/.75);	// Just allocate extra space in the hash table
        // try to get the table size sort of prime
        while (tableSize % 2 == 0 || tableSize % 3 == 0 || tableSize % 5 == 0 || tableSize % 7 == 0)
            tableSize++;
        // Mark all hash table entries as not in use (end of chain)
        for (int curHashIndex = 0; curHashIndex < tableSize; curHashIndex++)
            setHashVal(curHashIndex, kEndOfChain);
        maxEntries = (availableSpace - (tableSize * 2)) / entrySize;
        // Mark all entries as not in use
        for (short curEntryNum = 0; curEntryNum < maxEntries; curEntryNum++)
        {
            setNext(curEntryNum, kUnusedEntry);
        }
        numEntries = 0;
        nextEntryIndex = 0;
    }
    
    public BufferHashtable(Buffer inBuffer)
    {
        
    }
    
    public void put(ExtendibleHashKey key, byte [] record)
    throws HashTableFullException
    {
        int hashCode = key.hashCode();
        int index = (hashCode & 0x7FFFFFFF) % tableSize;
        short prevVal = kEndOfChain;
        byte[] keyBytes = key.getBytes();
        
        // Now, figure out where it lives in the hashtable
        short nextVal = getHashVal(index);
        // Find the end of the chain
        while (nextVal != kEndOfChain)
        {
            prevVal = nextVal;
            byte [] checkBytes = getEntryKey(nextVal);
            // This key already exists - update the record
            if (keysEqual(checkBytes, keyBytes))
            {
                setEntryValue(nextVal, record);
                return;
            }
            nextVal = getNext(nextVal);
        }
        short entryIndex = nextEntryIndex();
        if (entryIndex == kUnusedEntry)
            throw new HashTableFullException();

        setEntryKey(entryIndex, keyBytes);
        setEntryValue(entryIndex, record);
        setNext(entryIndex, (short)kEndOfChain);	// End of chain
        // No entry, make the first
        if (prevVal == kEndOfChain)
        {
            setHashVal(index, entryIndex);
        }
        else
        {
            // Add to the end of the chain
            setNext(prevVal, entryIndex);
        }
        
        numEntries++;
    }
    
    public byte [] get(ExtendibleHashKey getKey)
    {
        byte [] returnBytes = null;
        short entryIndex = getIndex(getKey);
        if (entryIndex != kEndOfChain)
            return(getEntryValue(entryIndex));
        return (returnBytes);
    }
    
    short getIndex(ExtendibleHashKey getKey)
    {
        int hashCode = getKey.hashCode();
        int hashIndex = (hashCode & 0x7FFFFFFF) % tableSize;
        short entryIndex = getHashVal(hashIndex);
        byte [] checkBytes = getKey.getBytes();
        while(entryIndex != kEndOfChain)
        {
            byte [] entryKey = getEntryKey(entryIndex);
            if (keysEqual(checkBytes, entryKey))
                break;
            entryIndex = getNext(entryIndex);	// Follow the chain
        }
        return entryIndex;
    }
    
    /**
     * @param checkBytes
     * @param entryKey
     * @return
     */
    private boolean keysEqual(byte[] checkBytes, byte[] entryKey)
    {
        boolean match;
        match = true;
        for (int curCompareByteNum = 0; curCompareByteNum < entryKey.length; curCompareByteNum++)
        {
            if (entryKey[curCompareByteNum] != checkBytes[curCompareByteNum])
            {
                match = false;
                break;
            }
        }
        return match;
    }

    public boolean containsKey(ExtendibleHashKey getKey)
    {
        if (getIndex(getKey) > kEndOfChain )
            return true;
        return false;
    }
    
    public byte [] remove(ExtendibleHashKey removeKey)
    {
        int hashCode = removeKey.hashCode();
        int hashIndex = (hashCode & 0x7FFFFFFF) % tableSize;
        short removeIndex = getHashVal(hashIndex);
        short prevIndex = kEndOfChain;
        byte [] checkBytes = removeKey.getBytes();
        while(removeIndex != kEndOfChain)
        {
            byte [] entryKey = getEntryKey(removeIndex);
            if (keysEqual(checkBytes, entryKey))
                break;
            prevIndex = removeIndex;
            removeIndex = getNext(removeIndex);	// Follow the chain
        }
        if (removeIndex != kEndOfChain)
        {
            byte [] returnBytes = getEntryValue(removeIndex);
            // Remove from chain
            short nextIndex = getNext(removeIndex);
            if (prevIndex == kEndOfChain)	// First entry
            {
                setHashVal(hashIndex, nextIndex);
            }
            else
            {
                setNext(prevIndex, nextIndex);
            }
            // Mark as unused
            setNext(removeIndex, (short)kUnusedEntry);
            numEntries --;
            return returnBytes;
        }
        else
        {
            return null;
        }
    }
    public int size()
    {
        return numEntries;
    }
    
    public Iterator<ExtendibleHashKey> getKeyIterator()
    {
        return new KeyIterator(this);
    }
    
    public Iterator<byte []> getValuesIterator()
    {
        return new ValuesIterator(this);
    }
    public int getMaxEntries()
    {
        return maxEntries;
    }
}
