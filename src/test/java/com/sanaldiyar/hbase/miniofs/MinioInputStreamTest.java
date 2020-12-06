/*
Copyright 2020 Kazım SARIKAYA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package com.sanaldiyar.hbase.miniofs;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.security.SecureRandom;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioInputStreamTest {

    private final static Logger logger = LoggerFactory.getLogger(MinioInputStreamTest.class.getName());

    @BeforeAll
    public static void setUpClass() {
        MinioFSSuiteTest.init();
    }

    @BeforeEach
    public void cleanup() throws Exception {
        MinioFSSuiteTest.cleanUp();
    }

    @Test
    public void testCreateFile() {

        try {
            Path rw = new Path(MinioFSSuiteTest.getRootPath(), "mistest/file1");

            HashFunction sha256 = Hashing.sha256();
            SecureRandom random = new SecureRandom();
            int partsize = MinioFSSuiteTest.getConf().getInt(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, 0);
            assert partsize != 0;
            byte[] tmpData = new byte[3 << 20];
            MinioOutputStream mos = new MinioOutputStream(rw, MinioFSSuiteTest.getConf(), partsize);
            Hasher hasher = sha256.newHasher();
            for (int i = 0; i < 3; i++) {
                random.nextBytes(tmpData);
                hasher.putBytes(tmpData);
                mos.write(tmpData);
            }
            mos.close();
            String hashSended = hasher.hash().toString();

            MinioInputStream mis = new MinioInputStream(rw, MinioFSSuiteTest.getConf(), tmpData.length >> 2);
            hasher = sha256.newHasher();
            long pos = 0;
            while (true) {
                int r = mis.read(tmpData);
                if (r <= 0) {
                    logger.debug("readed {} pos {}", r, pos);
                    break;
                }
                logger.debug("readed {} pos {}", r, pos);
                pos += r;
                hasher.putBytes(tmpData, 0, r);
            }
            mis.close();
            String hashReaded = hasher.hash().toString();

            logger.debug("sended hash: {}", hashSended);
            logger.debug("recved hash {}", hashReaded);

            assert hashSended.equals(hashReaded);
        } catch (IOException ex) {
            logger.error("error occured", ex);
            assert false;
        }

    }
}
