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

public class ExtendibleHashTableIndexEntry implements Cloneable
{

    protected long bucketOffset;
    protected int bucketLength;
    protected ExtendibleHashTableBucket bucket;
    
    public ExtendibleHashTableIndexEntry(long inBucketOffset, int inBucketLength, ExtendibleHashTableBucket inBucket)
    {
        bucketOffset = inBucketOffset;
        bucketLength = inBucketLength;
        bucket = inBucket;
    }
    
    public void insertRecord(int key, byte [] recordBytes) 
    throws BucketFullException, RecordExistsException
    {
        bucket.insertRecord(key, recordBytes);
    }
    
    public void insertRecord(ExtendibleHashKey key, byte [] recordBytes) 
    throws BucketFullException, RecordExistsException
    {
        bucket.insertRecord(key, recordBytes);
    }
    
    public byte [] retrieveRecord(int key)
    throws RecordNotFoundException
    {
        return(bucket.retrieveRecord(key));
    }
    
    public byte [] retrieveRecord(ExtendibleHashKey key)
    throws RecordNotFoundException
    {
        return(bucket.retrieveRecord(key));
    }
    public ExtendibleHashTableBucket getBucket()
    {
        return bucket;
    }
    
    public void setBucket(ExtendibleHashTableBucket newBucket)
    {
        bucket = newBucket;
    }
    
    protected Object clone() throws CloneNotSupportedException
    {
        ExtendibleHashTableIndexEntry cloneEntry = new ExtendibleHashTableIndexEntry(bucketOffset, bucketLength, bucket);
        return cloneEntry;
    }
}
