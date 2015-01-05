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
 
package com.igeekinc.indelible.indeliblefs.uniblock.dbcas;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.CASSegmentIDIterator;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.logging.ErrorLogMessage;

public class DBCASSegmentIDIterator implements CASSegmentIDIterator
{
	private ResultSet setToIterate;
	private boolean hasNext;
	
	public DBCASSegmentIDIterator(ResultSet setToIterate)
	{
		this.setToIterate = setToIterate;
		try
		{
			hasNext = setToIterate.next();
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new IllegalArgumentException("setToIterate threw a SQLException");
		}
	}
	
	@Override
	public boolean hasNext()
	{
		return hasNext;
	}

	@Override
	public ObjectID next()
	{
		ObjectID returnSegmentID = null;
		try
		{
			byte [] idBlob = setToIterate.getBytes(1);
			if (idBlob.length == ObjectID.kTotalBytes)
			{
				returnSegmentID = ObjectIDFactory.reconstituteFromBytes(idBlob);
			}
			hasNext = setToIterate.next();
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InternalError("caught a SQLException");
		}
		return returnSegmentID;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void close()
	{
		try
		{
			setToIterate.close();
		} catch (SQLException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
	}

}
