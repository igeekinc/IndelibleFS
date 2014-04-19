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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.mapdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;

import org.mapdb.DB;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.perf4j.log4j.Log4JStopWatch;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.junitext.iGeekTestCase;
import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.SHA1HashID;

interface InsertRunnable
{
	public void insert(CASIdentifier key, long value) throws IOException;
}

class InsertNull implements InsertRunnable
{

	@Override
	public void insert(CASIdentifier key, long value) throws IOException
	{
		// Do nothing
	}
	
}

class InsertMapDB implements InsertRunnable
{
	HTreeMap<CASIdentifier, Long>casIDToDataIDMapper;
	
	public InsertMapDB(HTreeMap<CASIdentifier, Long>casIDToDataIDMapper)
	{
		this.casIDToDataIDMapper = casIDToDataIDMapper;
	}
	
	@Override
	public void insert(CASIdentifier key, long value) throws IOException
	{
		casIDToDataIDMapper.put(key, value);
	}
}
public class IndexTest extends iGeekTestCase
{
	private static final int	kNumGenerateKeys	= 1000000;
	public void testKeyGenSpeed() throws IOException
	{
		Log4JStopWatch generateWatch = new Log4JStopWatch("keygen");
		generateKeys(kNumGenerateKeys, 0, new InsertNull(), null);
		generateWatch.stop();
		System.out.println("Generated "+kNumGenerateKeys+" in "+generateWatch.getElapsedTime()+" ms");
	}
	
	public static final int kInsertSmallNum = 1000000;
	public static final int kInsertMediumNum = kInsertSmallNum * 10;
	public static final int kInsertLargeNum = kInsertMediumNum * 10;
	/*
	public void testMapDBSmall() throws IOException
	{
		File createIndexDir = new File("/tmp/testMapDBSmall");
		File createIndexFile = getIndexFile(createIndexDir);
		System.out.println("Test insert async writes, soft ref cache");
        long numKeysToInsert = kInsertSmallNum;
		// Create with caching, etc. disable to force write of object serialization data
        DB casDB = DBMaker.newFileDB(createIndexFile).closeOnJvmShutdown().cacheSoftRefEnable().make();
        HTreeMap<CASIdentifier, Long> casIDToDataIDMapper = createMap(casDB);
        

        insertRecords(casDB, casIDToDataIDMapper, numKeysToInsert);
		TestFilesTool.deleteTree(createIndexDir);
	}
*/
	public void testMapDBNoCacheNoWALSmall() throws IOException
	{
		File createIndexDir = new File("/db1/testMapDBSmall");
		File createIndexFile = getIndexFile(createIndexDir);
		System.out.println("Test insert no async writes, no cache");

        DBMaker newFileDB = DBMaker.newFileDB(createIndexFile);
        newFileDB.closeOnJvmShutdown();
        newFileDB.asyncWriteDisable();
        newFileDB.transactionDisable();
		DB casDB = newFileDB.make();
        HTreeMap<CASIdentifier, Long> casIDToDataIDMapper = createMap(casDB);
        
        long numKeysToInsert = kInsertSmallNum;
        insertRecords(casDB, casIDToDataIDMapper, numKeysToInsert);
		TestFilesTool.deleteTree(createIndexDir);
	}
	
	
	public void testMapDBNoCacheNoWALMedium() throws IOException
	{
		File createIndexDir = new File("/db1/testMapDBMedium");
		File createIndexFile = getIndexFile(createIndexDir);
		System.out.println("Test insert no async writes, no cache");

        DBMaker newFileDB = DBMaker.newFileDB(createIndexFile);
        newFileDB.closeOnJvmShutdown();
        newFileDB.asyncWriteDisable();
        newFileDB.transactionDisable();
		DB casDB = newFileDB.make();
        HTreeMap<CASIdentifier, Long> casIDToDataIDMapper = createMap(casDB);
        
        long numKeysToInsert = kInsertMediumNum;
        insertRecords(casDB, casIDToDataIDMapper, numKeysToInsert);
		TestFilesTool.deleteTree(createIndexDir);
	}
	
	public void testMapDBCacheWALMedium() throws IOException
	{
		File createIndexDir = new File("/db1/testMapDBMedium");
		File createIndexFile = getIndexFile(createIndexDir);
		System.out.println("Test insert async writes, soft ref cache");

        DBMaker newFileDB = DBMaker.newFileDB(createIndexFile);
        newFileDB.closeOnJvmShutdown();
        newFileDB.cacheSoftRefEnable();
		DB casDB = newFileDB.make();
        HTreeMap<CASIdentifier, Long> casIDToDataIDMapper = createMap(casDB);
        
        long numKeysToInsert = kInsertMediumNum;
        insertRecords(casDB, casIDToDataIDMapper, numKeysToInsert);
		TestFilesTool.deleteTree(createIndexDir);
	}
	
	public void testMapDBCacheWALLarge() throws IOException
	{
		File createIndexDir = new File("/db1/testMapDBLarge");
		File createIndexFile = getIndexFile(createIndexDir);
		System.out.println("Test insert async writes, soft ref cache");

        DBMaker newFileDB = DBMaker.newFileDB(createIndexFile);
        newFileDB.closeOnJvmShutdown();
        newFileDB.cacheSoftRefEnable();
		DB casDB = newFileDB.make();
        HTreeMap<CASIdentifier, Long> casIDToDataIDMapper = createMap(casDB);
        
        long numKeysToInsert = kInsertLargeNum;
        insertRecords(casDB, casIDToDataIDMapper, numKeysToInsert);
		TestFilesTool.deleteTree(createIndexDir);
	}
	
	public void testMapDBNoCacheNoWALLarge() throws IOException
	{
		File createIndexDir = new File("/db1/testMapDBLarge");
		File createIndexFile = getIndexFile(createIndexDir);
		System.out.println("Test insert no async writes, no cache");

        DBMaker newFileDB = DBMaker.newFileDB(createIndexFile);
        newFileDB.closeOnJvmShutdown();
        newFileDB.asyncWriteDisable();
        newFileDB.transactionDisable();
		DB casDB = newFileDB.make();
        HTreeMap<CASIdentifier, Long> casIDToDataIDMapper = createMap(casDB);
        
        long numKeysToInsert = kInsertLargeNum;
        insertRecords(casDB, casIDToDataIDMapper, numKeysToInsert);
		TestFilesTool.deleteTree(createIndexDir);
	}
	protected void insertRecords(DB casDB,
			HTreeMap<CASIdentifier, Long> casIDToDataIDMapper,
			long numKeysToInsert) throws IOException
	{
		System.out.println("Test insert "+numKeysToInsert+" records");
		Log4JStopWatch totalWatch = new Log4JStopWatch("total");
		Log4JStopWatch insertWatch = new Log4JStopWatch("insertMapDBSmall");
        InsertMapDB inserter = new InsertMapDB(casIDToDataIDMapper);
        generateKeys(numKeysToInsert, 0, inserter, casDB);
        insertWatch.stop();
		
		Log4JStopWatch commitWatch = new Log4JStopWatch("commitMapDBSmall");
        casDB.commit();
        commitWatch.stop();
        
        casDB.close();
        totalWatch.stop();
        System.out.println("Inserted "+numKeysToInsert+" in "+insertWatch.getElapsedTime()+" ms commit took "+commitWatch.getElapsedTime()+" ms total time = "+totalWatch.getElapsedTime());
	}

	protected HTreeMap<CASIdentifier, Long> createMap(DB casDB)
	{
		HTreeMapMaker mapMaker = casDB.createHashMap("testmapsmall");
        mapMaker.keySerializer(new CASIdentifierSerializer());
        mapMaker.valueSerializer(new LongSerializer());
        HTreeMap<CASIdentifier, Long>casIDToDataIDMapper = mapMaker.make();
        casDB.commit();
		return casIDToDataIDMapper;
	}

	protected File getIndexFile(File createIndexDir) throws IOException
	{
		if (createIndexDir.exists())
			TestFilesTool.deleteTree(createIndexDir);
		assertTrue(createIndexDir.mkdir());
		File createIndexFile = new File(createIndexDir, "Index");
		return createIndexFile;
	}
	
	public void generateKeys(long numKeysToGenerate, long startKeyNum, InsertRunnable insertRunnable, DB casDB) throws IOException
	{
		ByteBuffer generateBuffer = ByteBuffer.allocateDirect(512);
		Log4JStopWatch insertWatch = new Log4JStopWatch("insertWatch");
		for (long ourKeyNum = 0; ourKeyNum < numKeysToGenerate; ourKeyNum++)
		{
			long curKeyNum = ourKeyNum + startKeyNum;
			generateBuffer.position(0);
			for (int numWrites = 0; numWrites < generateBuffer.limit()/8; numWrites++)
				generateBuffer.putLong(curKeyNum);
			generateBuffer.position(0);
			SHA1HashID hashID = new SHA1HashID(generateBuffer);
			CASIdentifier identifier = new CASIdentifier(hashID, generateBuffer.limit());
			insertRunnable.insert(identifier, curKeyNum);
			if (ourKeyNum % 1000000 == 0 && ourKeyNum > 0)
			{
				insertWatch.stop();
				Log4JStopWatch commitWatch = new Log4JStopWatch("insertCommit");
				if (casDB != null)
					casDB.commit();
				commitWatch.stop();
				long insertPlusCommit = insertWatch.getElapsedTime() + commitWatch.getElapsedTime();
				System.out.println("Inserted from "+((ourKeyNum - 1000000) + startKeyNum)+" to "+(ourKeyNum+startKeyNum)+" records in "+insertWatch.getElapsedTime()+" ms commit took "+commitWatch.getElapsedTime()+" ms total = "+
				insertPlusCommit+" records/sec = "+MessageFormat.format("{0, number, #.##}", (1000000.0/((double)insertPlusCommit/1000.0))));
				insertWatch.start();

			}
		}
	}
}
