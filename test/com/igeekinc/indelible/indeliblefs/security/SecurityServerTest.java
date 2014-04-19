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
 
package com.igeekinc.indelible.indeliblefs.security;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.CertificateException;

import junit.framework.TestCase;

import com.igeekinc.indelible.oid.EntityID;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;

public class SecurityServerTest extends TestCase
{
    public void testInitRootSecurity() throws KeyStoreException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException, IllegalStateException, CertificateException, FileNotFoundException, IOException
    {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        File testRootSecurityDir = File.createTempFile("InitRootSecurityTest", ".dir");
        testRootSecurityDir.delete();
        testRootSecurityDir.mkdir();
        GeneratorIDFactory genIDFactory;
        genIDFactory = new GeneratorIDFactory();
        GeneratorID genID = genIDFactory.createGeneratorID();
        ObjectIDFactory testFactory = new ObjectIDFactory(genID);
        EntityID rootSecurityServerID = (EntityID) testFactory.getNewOID(EntityAuthenticationServerCore.class);
        EntityAuthenticationServerCore.initRootSecurity(testRootSecurityDir, rootSecurityServerID);
    }
}
