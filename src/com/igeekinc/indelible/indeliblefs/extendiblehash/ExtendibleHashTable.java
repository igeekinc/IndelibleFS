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
import java.math.BigInteger;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.bufcache.BufferCache;
import com.igeekinc.util.BitTwiddle;


/**
 * ExtendibleHashTable - implements an extendible hash table on top of a BufferCache.
 * The BufferCache can be backed by a file, device, etc.
 * 
 * A master block contains the information needed to bootstrap the hashtable
 */
public class ExtendibleHashTable
{
    protected int bucketSize, recordSize;
    protected int hashBits;
    ExtendibleHashTableIndexEntry [] index;
    private Logger logger;
    private boolean paranoid = false;
    private int keySize;
    private BufferCache bufferCache;
    
    public ExtendibleHashTable(BufferCache inBufferCache, long headerOffset, int inBucketSize, int inRecordSize, int inKeySize, boolean init)
    throws IOException
    {
        logger = Logger.getLogger(this.getClass());
        bucketSize = inBucketSize;
        recordSize = inRecordSize;
        keySize = inKeySize;
        hashBits = 0;
        bufferCache = inBufferCache;
        index = new ExtendibleHashTableIndexEntry[1];
        index[0] = new ExtendibleHashTableIndexEntry(0, 0, new ExtendibleHashTableBucket(bufferCache, recordSize, keySize, bucketSize));
    }
    
    public synchronized void insert(int key, byte [] recordBytes)
    throws RecordExistsException, IOException
    {
        byte [] keyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(key, keyBytes, 0);
        insert(new ExtendibleHashKey(keyBytes), recordBytes);        
    }
    
    public synchronized void insert(ExtendibleHashKey key, byte [] recordBytes)
    throws RecordExistsException, IOException
    {
        if (recordBytes.length != recordSize)
            throw new IllegalArgumentException("recordSize set to "+recordSize+", attempting to insert record of size "+recordBytes.length);
        logger.debug("Inserting key "+key);
        int indexKey = calculateIndexKey(key, hashBits);
        ExtendibleHashTableIndexEntry insertIndexEntry;

        insertIndexEntry = index[indexKey];
        boolean successful = false;
        while (!successful)
        {
            try
            {
                insertIndexEntry.insertRecord(key, recordBytes);
                successful = true;
            } catch (BucketFullException e)
            {
                splitBucket(key);
                indexKey = calculateIndexKey(key, hashBits);
                
                insertIndexEntry = index[indexKey];
            }
        }
        if (paranoid)
        {
            ExtendibleHashKey indexMinKey = calcMinHashKeyForIndex(indexKey, hashBits, keySize);
            ExtendibleHashKey indexMaxKey = calcMaxHashKeyForIndex(indexKey, hashBits, keySize);
            if (key.compareTo(indexMinKey) < 0)
                throw new InternalError("insert key ("+key+")less than min key ("+indexMinKey+")for index");
            if (key.compareTo(indexMaxKey) > 0)
                throw new InternalError("insert key ("+key+")greater than max key ("+indexMaxKey+")for index");
            checkIndex();
        }
    }
    
    public synchronized byte [] retrieve(int key)
    throws RecordNotFoundException
    {
        byte [] keyBytes = new byte[4];
        BitTwiddle.intToJavaByteArray(key, keyBytes, 0);
        return retrieve(new ExtendibleHashKey(keyBytes));
    }
    
    public synchronized byte [] retrieve(ExtendibleHashKey key)
    throws RecordNotFoundException
    {
        int indexKey = calculateIndexKey(key, hashBits);
        ExtendibleHashTableIndexEntry retrieveIndexEntry;
        logger.debug("Retrieving key "+key);
        retrieveIndexEntry = index[indexKey];
        byte [] returnBytes = null;
        
        returnBytes = retrieveIndexEntry.retrieveRecord(key);
 
        return(returnBytes);
    }

    int calculateIndexKey(ExtendibleHashKey key, int hashBitsForCalc)
    {
        int intKey;
        byte [] keyBytes;
        if (keySize >= 4)
            keyBytes = key.getBytes();
        else
        {
            keyBytes = new byte[4];
            System.arraycopy(key.getBytes(), 0, keyBytes, 4-key.getBytes().length, keySize);
        }
        intKey = BitTwiddle.javaByteArrayToInt(keyBytes, 0);
        long indexKey;
        long indexKeyMask1 = (0xffffffff << (32-hashBitsForCalc)) & 0xffffffff;
        long indexKeyMask2 = 0;
        for (int curBitNum = 0; curBitNum < hashBitsForCalc; curBitNum++)
        {
            indexKeyMask2 |= 1 << curBitNum;
        }
        indexKey = intKey & indexKeyMask1;
        indexKey = indexKey >> (32 - hashBitsForCalc);
        indexKey &= indexKeyMask2;
        return (int)indexKey;
    }
    
    protected void splitBucket(ExtendibleHashKey splitCauseKey)
    throws IOException
    {
        ExtendibleHashTableBucket oldBucket, newBucket;
        int indexKey = calculateIndexKey(splitCauseKey, hashBits);
        oldBucket = index[indexKey].getBucket();
        
        int minIndex = calcMinIndex(indexKey);
        int maxIndex =calcMaxIndex(indexKey);
        if (minIndex == maxIndex)	// Need to split the whole index
        {
            logger.debug("Splitting index - index size = "+index.length+" -> "+index.length*2);
            splitIndex();
            if (paranoid)
                checkIndex();
            indexKey = calculateIndexKey(splitCauseKey, hashBits);
            oldBucket = index[indexKey].getBucket();
            minIndex = calcMinIndex(indexKey);
            maxIndex =calcMaxIndex(indexKey);
        }
        
        int splitIndexKey;
        
        ExtendibleHashKey splitHashKey= null;
                
        if (minIndex == maxIndex)
        {
            throw new InternalError("Bucket does not span indices - can't split it");
        }
        logger.debug("bucket spans indices "+minIndex+" to "+maxIndex);
        
        if (maxIndex - minIndex < 2)
            splitIndexKey = maxIndex;
        else
        {
            logger.debug("---Big bucket spread---");
            for (splitIndexKey = minIndex; splitIndexKey < maxIndex-1; splitIndexKey++)
            {
                splitHashKey = calcMinHashKeyForIndex(splitIndexKey, hashBits, keySize);
                int numLess = oldBucket.keysLessThan(splitHashKey);
                if (numLess >= oldBucket.getNumRecords()/2)
                    break;
            }
        }
        /*
         
        if (maxIndex - minIndex < 2)
            splitIndexKey = maxIndex;
        else
            splitIndexKey = minIndex + (maxIndex - minIndex)/2;
           */ 
        splitHashKey = calcMinHashKeyForIndex(splitIndexKey, hashBits, keySize);           
        
        
        logger.debug("Splitting bucket on key "+splitHashKey+", splitIndexKey = "+splitIndexKey);
        
        newBucket = oldBucket.split(splitHashKey);
        logger.debug("After split, old bucket has "+oldBucket.getNumRecords()+", new bucket has "+newBucket.getNumRecords());
        logger.debug(minIndex+" to "+(splitIndexKey-1)+" set to new bucket "+splitIndexKey+" to "+maxIndex+" remain old bucket");
        //index[(int)minIndex].setBucket(newBucket);
        for (long changeIndex = minIndex; changeIndex < splitIndexKey; changeIndex++)
        {
            //logger.debug("Setting index "+changeIndex+" to new bucket");
            index[(int)changeIndex].setBucket(newBucket);
        }
        if (paranoid)
        {
            ExtendibleHashKey newMinKey = calcMinHashKeyForIndex(minIndex, hashBits, keySize);
            ExtendibleHashKey newMaxKey = calcMaxHashKeyForIndex(splitIndexKey-1, hashBits, keySize);
            if (newBucket.getMinKey() != null && newBucket.getMinKey().compareTo(newMinKey) < 0)
                throw new InternalError("new bucket's min key ("+newBucket.getMinKey()+") is less than calculated minKey ("+
                        newMinKey+")");
            if (newBucket.getMaxKey() != null && newBucket.getMaxKey().compareTo(newMaxKey) > 0)
                throw new InternalError("new bucket's max key ("+newBucket.getMinKey()+") is greater than calculated maxKey ("+
                        newMinKey+")");
            ExtendibleHashKey oldMinKey = calcMinHashKeyForIndex(splitIndexKey, hashBits, keySize) ;
            ExtendibleHashKey oldMaxKey = calcMaxHashKeyForIndex(maxIndex, hashBits, keySize) ;
            checkIndex();
            if (oldBucket.getMinKey() != null && oldBucket.getMinKey().compareTo(oldMinKey) < 0)
                throw new InternalError("old bucket's min key ("+oldBucket.getMinKey()+") is less than calculated minKey ("+
                        oldMinKey+")");
            if (oldBucket.getMaxKey() != null && oldBucket.getMaxKey().compareTo(oldMaxKey) > 0)
                throw new InternalError("old bucket's max key ("+oldBucket.getMaxKey()+") is greater than calculated maxKey ("+
                        oldMaxKey+")");
        }
        
    }

    /**
     * @param oldBucket
     * @param splitIndexKey
     * @return
     */
    private int calcMinIndex(int splitIndexKey)
    {
        int minIndex;
        ExtendibleHashTableBucket oldBucket = index[splitIndexKey].getBucket();
        minIndex = splitIndexKey;
        while (minIndex > 0 && index[minIndex-1].getBucket() == oldBucket)
            minIndex--;
        return minIndex;
    }

    /**
     * @param oldBucket
     * @param splitIndexKey
     * @return
     */
    private int calcMaxIndex(int splitIndexKey)
    {
        int maxIndex;
        ExtendibleHashTableBucket oldBucket = index[splitIndexKey].getBucket();
        maxIndex = splitIndexKey;
        while (maxIndex < index.length - 1 && index[maxIndex + 1].getBucket() == oldBucket)
            maxIndex++;
        return maxIndex;
    }
    /**
     * @param index
     * @param calcHashBits
     * @param keyLength
     * @return
     */
    private ExtendibleHashKey calcMinHashKeyForIndex(int index, int calcHashBits, int keyLength)
    {
        long splitKey = (index << (32 - calcHashBits)) & 0xffffffffL;
        byte [] splitKeyBytes = new byte[keyLength];
        BitTwiddle.intToJavaByteArray((int)splitKey, splitKeyBytes, 0);
        for (int curByteNum = 4; curByteNum < splitKeyBytes.length; curByteNum++)
            splitKeyBytes[curByteNum] = 0;
        ExtendibleHashKey splitHashKey = new ExtendibleHashKey(splitKeyBytes);
        return splitHashKey;
    }
    private ExtendibleHashKey calcMaxHashKeyForIndex(int index, int calcHashBits, int keyLength)
    {
        // Check to see if all relevant bits are 1
        int checkInt = (int)(0xffffffffL >> (32 - calcHashBits));
        byte [] returnKeyBytes = new byte[keyLength];
        BigInteger calcInt;
        if ((checkInt ^ index) == 0)
        {
            // We're at the max key so just set the return key to be all 1's
            for (int curByteNum = 0; curByteNum < keyLength; curByteNum++)
                returnKeyBytes[curByteNum] = (byte)0xff;
        }
        else
        {
            byte [] splitKeyBytes = new byte[keyLength+1];	// Ensure that number is not "negative" by ensuring that high byte is 0

            index++;
            long splitKey = (index << (32 - calcHashBits)) & 0xffffffffL;
            splitKeyBytes[0] = 0;
            BitTwiddle.intToJavaByteArray((int)splitKey, splitKeyBytes, 1);
            for (int curByteNum = 5; curByteNum < splitKeyBytes.length; curByteNum++)
                splitKeyBytes[curByteNum] = 0;
            calcInt = new BigInteger(splitKeyBytes);
            
            calcInt = calcInt.subtract(BigInteger.ONE);
            byte [] calcBytes = calcInt.toByteArray();
            
            if (calcBytes.length <= keyLength)
                System.arraycopy(calcBytes, 0, returnKeyBytes, keyLength-calcBytes.length, calcBytes.length);
            else
                System.arraycopy(calcBytes, 1, returnKeyBytes, 0, keyLength);
            
        }
        ExtendibleHashKey returnHashKey = new ExtendibleHashKey(returnKeyBytes);
        return returnHashKey;
    }
    protected void splitIndex()
    {
        ExtendibleHashTableBucket oldBucket, newBucket;
        
        ExtendibleHashTableIndexEntry [] newIndex = new ExtendibleHashTableIndexEntry[index.length * 2];
        for (int oldIndexPos = 0; oldIndexPos < index.length; oldIndexPos++)
        {
            int newIndexPos = oldIndexPos * 2;
            newIndex[newIndexPos] = index[oldIndexPos];
            newIndexPos++;
            try
            {
                newIndex[newIndexPos] = (ExtendibleHashTableIndexEntry)index[oldIndexPos].clone();
            } catch (CloneNotSupportedException e)
            {
                throw new InternalError("Clone not supported on ExtendibleHashTableIndexEntry");
            }
        }
        index = newIndex;
        hashBits++;
    }
    
    private void checkIndex()
    {
        int curIndex = 0;
        int startIndex, endIndex;
        while (curIndex < index.length)
        {
            startIndex = curIndex;
            ExtendibleHashTableBucket curBucket = index[startIndex].getBucket();
            ExtendibleHashKey minKey = calcMinHashKeyForIndex(startIndex, hashBits, 16);
            while (curIndex < index.length && index[curIndex].getBucket().equals(curBucket))
                curIndex++;
            endIndex = curIndex - 1;
            ExtendibleHashKey maxKey = calcMaxHashKeyForIndex(endIndex, hashBits, 16);
            
            if (curBucket.getMinKey() != null && curBucket.getMinKey().compareTo(minKey) < 0)
                throw new InternalError("Min key ("+curBucket.getMinKey()+") for index "+startIndex+" less than calc min key "+minKey);
            if (curBucket.getMaxKey() != null && curBucket.getMaxKey().compareTo(maxKey) > 0)
                throw new InternalError("Max key ("+curBucket.getMaxKey()+") for index "+endIndex+" greater than calc max key "+maxKey);

        }
    }
    public boolean isParanoid()
    {
        return paranoid;
    }
    public void setParanoid(boolean paranoid)
    {
        this.paranoid = paranoid;
    }
}
