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
 
package com.igeekinc.indelible.indeliblefs.uniblock.casstore;

public enum CASStoreStatus
{
	kStarting,					// The store is initializing
	kReady, 					// The store is ready for read/write operations
	kClosed, 					// The store is closed (cannot be read or written)
	kRebuilding, 				// The store is performing a rebuild operation (cannot be read or written)
	kNeedsRebuild, 				// The store needs to be rebuilt (cannot be read or written)
	kNeedsReloadOfNewObjects,	// The store has new objects not in the index yet
	kReadOnly					// The store has been set to read-only 
}
