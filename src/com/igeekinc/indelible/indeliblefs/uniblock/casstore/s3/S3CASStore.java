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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.AbstractCASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStore;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreStatus;
import com.igeekinc.indelible.oid.CASStoreID;
import com.igeekinc.indelible.oid.ObjectIDFactory;

public class S3CASStore extends AbstractCASStore implements CASStore
{
	private String accessKey, secretKey;
	private AWSCredentials credentials;
	private AmazonS3Client s3Client;
	private CASStoreID storeID;
	private File initPropertiesFiles;
	private Region region;
	public static final String kAmazonAccessKeyName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3.S3CASStore.accessKey";
	public static final String kAmazonSecretName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3.S3CASStore.secret";
	public static final String kStoreIDName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3.S3CASStore.storeID";
	public static final String kAmazonRegionName = "com.igeekinc.indelible.indeliblefs.uniblock.casstore.s3.S3CASStore.regionName";
	public S3CASStore(String accessKey, String secretKey, CASStoreID storeID, String regionString, File initPropertiesFile) throws IOException
	{
		super(null);
		this.initPropertiesFiles = initPropertiesFile;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		if (regionString != null)
			region = Region.fromValue(regionString);
		else
			region = null;
		Properties writeProperties = new Properties();
		writeProperties.put(kAmazonAccessKeyName, accessKey);
		writeProperties.put(kAmazonSecretName, secretKey);
		writeProperties.put(kStoreIDName, storeID.toString());
		if (region != null)
			writeProperties.put(kAmazonRegionName, region.toString());
		FileOutputStream initPropertiesOutStream = new FileOutputStream(initPropertiesFile);
		writeProperties.store(initPropertiesOutStream, "S3CASStore Properties saved at"+(new Date()).toString());
		initPropertiesOutStream.close();
		
		this.storeID = storeID;
		initInternal();
		setStatus(CASStoreStatus.kReady);
	}

	public S3CASStore(File initPropertiesFile) throws IOException
	{
		super(null);
		FileInputStream initPropertiesInputStream = new FileInputStream(initPropertiesFile);
		Properties initProperties = new Properties();
		initProperties.load(initPropertiesInputStream);
		accessKey = initProperties.getProperty(kAmazonAccessKeyName);
		secretKey = initProperties.getProperty(kAmazonSecretName);
		String storeIDStr = initProperties.getProperty(kStoreIDName);
		String regionString = initProperties.getProperty(kAmazonRegionName);
		if (regionString != null)
			region = Region.fromValue(regionString);
		else
			region = null;
		storeID = (CASStoreID)ObjectIDFactory.reconstituteFromString(storeIDStr);
		initInternal();
		setStatus(CASStoreStatus.kReady);
	}
	
	protected void initInternal()
	{
		credentials = new BasicAWSCredentials(accessKey, secretKey);
		s3Client = new AmazonS3Client(credentials);
		List<Bucket>bucketList = s3Client.listBuckets();
		
		for (Bucket checkBucket:bucketList)
		{
			if (checkBucket.getName().equals(storeID.toString()))
			{
				return;	// It already exists
			}
		}
		if (region != null)
			s3Client.createBucket(storeID.toString(), region);	// storeID's should be globally unique so we don't worry about bucket name collisions
		else
			s3Client.createBucket(storeID.toString());
	}

	public File getInitPropertiesFile()
	{
		return initPropertiesFiles;
	}
	
	@Override
	public CASStoreID getCASStoreID()
	{
		return storeID;
	}

	@Override
	public CASIDDataDescriptor retrieveSegment(CASIdentifier segmentID)
			throws IOException
	{
		S3DataDescriptor returnDescriptor = null;
		GetObjectRequest retrieveRequest = new GetObjectRequest(storeID.toString(), segmentID.toString());
		S3Object retrieveObject = s3Client.getObject(retrieveRequest);
		if (retrieveObject != null)
			returnDescriptor = new S3DataDescriptor(segmentID, retrieveObject);
		return returnDescriptor;
	}

	@Override
	public void storeSegment(CASIDDataDescriptor segmentDescriptor)
			throws IOException
	{
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(segmentDescriptor.getLength());
		PutObjectResult result = s3Client.putObject(storeID.toString(), segmentDescriptor.getCASIdentifier().toString(), segmentDescriptor.getInputStream(), metadata);
		// If we get this far we're golden
	}

	@Override
	public boolean deleteSegment(CASIdentifier segmentID) throws IOException
	{
		s3Client.deleteObject(storeID.toString(), segmentID.toString());
		return true;
	}

	@Override
	public long getNumSegments()
	{
		return 0;
	}

	@Override
	public long getTotalSpace()
	{
		return 1024L*1024L*1024L*1024L*100L;	// It's S3 - we should let the admin set a limit for this store to keep bills from going completely whacky
	}

	@Override
	public long getUsedSpace()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getFreeSpace()
	{
		return 1024L*1024L*1024L*1024L*100L;	// It's S3 - we should let the admin set a limit for this store to keep bills from going completely whacky
	}

	@Override
	public void sync() throws IOException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void shutdown() throws IOException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rebuild() throws IOException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void reloadNewObjects() throws IOException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean verifySegment(CASIdentifier identifierToVerify)
	{
		// Just assume S3 did not eat it
		return true;
	}	
}
