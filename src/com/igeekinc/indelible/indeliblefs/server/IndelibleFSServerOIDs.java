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
 
package com.igeekinc.indelible.indeliblefs.server;

import com.igeekinc.indelible.indeliblefs.IndelibleEntity;
import com.igeekinc.indelible.indeliblefs.core.IndelibleFSObject;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSession;
import com.igeekinc.indelible.indeliblefs.datamover.NetworkDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.casstore.CASStoreManager;
import com.igeekinc.indelible.oid.IndelibleFSClientOIDs;

public class IndelibleFSServerOIDs
{
	public static void initMapping()
	{
		IndelibleFSClientOIDs.initMappings();
		
		// No mapping for CASSegmentID
		// CASStoreID handled in IndelibleFSClientOIDs.initMappings
		CASStoreManager.initMappings();
		DataMoverSession.initMapping();
		IndelibleEntity.initMapping();
		IndelibleFSObject.initMapping();
		NetworkDataDescriptor.initMapping();
	}
}
