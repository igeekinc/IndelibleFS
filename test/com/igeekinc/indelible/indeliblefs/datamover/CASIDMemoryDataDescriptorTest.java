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
 
package com.igeekinc.indelible.indeliblefs.datamover;

import junit.framework.TestCase;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;

public class CASIDMemoryDataDescriptorTest extends TestCase
{
/*
    public BasicDataDescriptorTest()
	{
		int cryptixPos =
			Security.insertProviderAt(
				new cryptix.jce.provider.CryptixCrypto(),
				2);
		try
		{
		  Provider[] providers = Security.getProviders();
		  for( int i=0; i<providers.length; i++ )
		  {
		   System.out.println( "Provider: " + providers[ i ].getName() + ", " + providers[ i ].getInfo() );
		   for( Iterator itr = providers[ i ].keySet().iterator(); 
		itr.hasNext(); )
		   {
			 String key = ( String )itr.next();
			 String value = ( String )providers[ i ].get( key );
			 System.out.println( "\t" + key + " = " + value );
		   }
		  }
		}
		catch( Exception e )
		{
		  e.printStackTrace();
		}
	}
	*/
    /*
     * Class under test for void BasicDataDescriptor(byte[])
     */
    public void testCASIDMemoryDataDescriptorbyteArray()
    {
        byte [] test10 = new byte[10];
        byte [] test100 = new byte[100];
        byte [] test1024 = new byte[1024];
        byte [] test1M = new byte[1024*1024];
        fillInArray(test10);
        fillInArray(test100);
        fillInArray(test1024);
        fillInArray(test1M);
        
        CASIDMemoryDataDescriptor bdd10 = new CASIDMemoryDataDescriptor(test10);
        System.out.println("id len = "+bdd10.getCASIdentifier().toString().length());
        CASIDMemoryDataDescriptor bdd100 = new CASIDMemoryDataDescriptor(test100);
        CASIDMemoryDataDescriptor bdd1024 = new CASIDMemoryDataDescriptor(test1024);
        CASIDMemoryDataDescriptor bdd1M = new CASIDMemoryDataDescriptor(test1M);

    }

    void fillInArray(byte [] arrayToFill)
    {
        for (int curByteNum = 0; curByteNum < arrayToFill.length; curByteNum++)
            arrayToFill[curByteNum] = (byte)curByteNum;
    }
    /*
     * Class under test for void BasicDataDescriptor(byte[], int, int)
     */
    public void testBasicDataDescriptorbyteArrayintint()
    {
    }

}
