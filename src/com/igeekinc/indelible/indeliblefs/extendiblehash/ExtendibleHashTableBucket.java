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

import java.io.IOException;
import java.util.Iterator;

import com.igeekinc.indelible.indeliblefs.bufcache.Buffer;
import com.igeekinc.indelible.indeliblefs.bufcache.BufferCache;
import com.igeekinc.util.BitTwiddle;

public class ExtendibleHashTableBucket
{
    protected int recordSize, keySize;
    protected int maxRecords;
    protected BufferHashtable bucket;
    protected ExtendibleHashKey minKey, maxKey;
    protected int bucketSize;
    protected BufferCache bufferCache;

    public ExtendibleHashTableBucket(Buffer inBuffer, BufferCache inBufferCache, int inRecordSize, int inKeySize, int inBucketSize)
    throws IOException
    {
        recordSize = inRecordSize;
        bucketSize = inBucketSize;
        keySize = inKeySize;
        bufferCache = inBufferCache;
        bucket = new BufferHashtable(recordSize, keySize, inBuffer, 0);
        maxRecords = bucket.getMaxEntries();
        minKey = null;
        maxKey = null;
        bufferCache = inBufferCache;
    }
    
    public ExtendibleHashTableBucket(BufferCache inBufferCache, int inRecordSize, int inKeySize, int inBucketSize)
    throws IOException
    {
        this(inBufferCache.getBuffer(inRecordSize), inBufferCache, inRecordSize, inKeySize, inBucketSize);
    }
    
    public void insertRecord(int key, byte [] recordBytes)
    throws BucketFullException, RecordExistsException
    {
        byte [] keyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(key, keyBytes, 0);
        insertRecord(new ExtendibleHashKey(keyBytes), recordBytes);       
    }
    
    public void insertRecord(ExtendibleHashKey key, byte [] recordBytes)
    throws BucketFullException, RecordExistsException
    {
        if (recordBytes.length != recordSize)
            throw new IllegalArgumentException("recordSize set to "+recordSize+", attempting to insert record of size "+recordBytes.length);
        if (bucket.size() == maxRecords)
            throw new BucketFullException();
        if (bucket.containsKey(key))
        	throw new RecordExistsException();
        try
        {
            bucket.put(key, recordBytes);
        } catch (HashTableFullException e)
        {
            throw new BucketFullException();
        }
        if (minKey == null || minKey.compareTo(key) > 0)
            minKey = key;
        if (maxKey == null || maxKey.compareTo(key) < 0)
            maxKey = key;
    }
    
    public byte [] retrieveRecord(int key)
    throws RecordNotFoundException
    {
        byte [] keyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(key, keyBytes, 0);
        return(retrieveRecord(new ExtendibleHashKey(keyBytes)));
    }
    
    public byte [] retrieveRecord(ExtendibleHashKey key)
    throws RecordNotFoundException
    {
        byte [] returnRecord = (byte [])bucket.get(key);
        if (returnRecord == null)
        {
            throw new RecordNotFoundException(key);
        }
        return(returnRecord);
    }
    
    public void updateRecord(int key, byte [] recordBytes)
    throws RecordNotFoundException
    {
        byte [] keyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(key, keyBytes, 0);
        updateRecord(new ExtendibleHashKey(keyBytes), recordBytes);
    }
    
    public void updateRecord(ExtendibleHashKey key, byte [] recordBytes)
    throws RecordNotFoundException
    {
        if (recordBytes.length != recordSize)
            throw new IllegalArgumentException("recordSize set to "+recordSize+", attempting to insert record of size "+recordBytes.length);

        if (!bucket.containsKey(key))
        {
            throw new RecordNotFoundException(key);
        }
        try
        {
            bucket.put(key, recordBytes);
        } catch (HashTableFullException e)
        {
            throw new InternalError("Got hashtable full exception on update");
        }
    }
    
    public void removeRecord(int key)
    throws RecordNotFoundException
    {
        byte [] keyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(key, keyBytes, 0);
        removeRecord(new ExtendibleHashKey(keyBytes));
    }
    
    public void removeRecord(ExtendibleHashKey key)
    throws RecordNotFoundException
    {
        if (bucket.remove(key) == null)
        {
            throw new RecordNotFoundException(key);
        }
    }
    
    /**
     * 
     */
    private void recalcMinMax()
    {
        {
            minKey = null;
            maxKey = null;
            Iterator<ExtendibleHashKey> keysIterator = bucket.getKeyIterator();
            while(keysIterator.hasNext())
            {
                ExtendibleHashKey curKey = keysIterator.next();
                if (minKey == null || curKey.compareTo(minKey) < 0)
                    minKey = curKey;
                if (maxKey == null || curKey.compareTo(maxKey) > 0)
                    maxKey = curKey;
            }
        }
    }

    /**
     * Splits the contents of the current bucket around the given splitKey.  
     * Returns a new bucket with all keys less than splitKey while this bucket
     * retains all keys >= splitKey
     * @param splitKey
     * @return
     */
    public ExtendibleHashTableBucket split(int splitKey)
    throws IOException
    {
        byte [] splitKeyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(splitKey, splitKeyBytes, 0);
        return split(new ExtendibleHashKey(splitKeyBytes));
    }
    
    public ExtendibleHashTableBucket split(ExtendibleHashKey splitKey)
    throws IOException
    {
        ExtendibleHashTableBucket returnBucket = new ExtendibleHashTableBucket(bufferCache, recordSize, keySize, bucketSize);
        
        Iterator<ExtendibleHashKey> keyIterator = bucket.getKeyIterator();
        while (keyIterator.hasNext())
        {
            ExtendibleHashKey curKey = keyIterator.next();
            
            if (curKey.compareTo(splitKey) < 0)
            {
                byte [] recordBytes = (byte [])bucket.get(curKey);
                if (recordBytes == null)
                    throw new InternalError("No record for key "+curKey);
                keyIterator.remove();
                try
                {
                    returnBucket.insertRecord(curKey, recordBytes);
                } catch (BucketFullException e)
                {
                    throw new InternalError("Destination bucket overflowed during split");
                } catch (RecordExistsException e)
                {
                    throw new InternalError("Destination bucket already contains key");
                }
            }
        }
        recalcMinMax();
        return(returnBucket);
    }
    
    public int keysLessThan(ExtendibleHashKey checkKey)
    {
        int keysLessThan = 0;
        Iterator<ExtendibleHashKey> keyIterator = bucket.getKeyIterator();
        while (keyIterator.hasNext())
        {
            ExtendibleHashKey curKey = keyIterator.next();
            
            if (curKey.compareTo(checkKey) < 0)
                keysLessThan++;
        }
        return keysLessThan;
    }
    
    public ExtendibleHashKey getMaxKey()
    {
        return maxKey;
    }
    public int getMaxRecords()
    {
        return maxRecords;
    }
    public ExtendibleHashKey getMinKey()
    {
        return minKey;
    }
    public int getRecordSize()
    {
        return recordSize;
    }
    
    public int getNumRecords()
    {
        return bucket.size();
    }
}

