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

import java.io.IOException;
import java.security.SecureRandom;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioOutputStreamTest {

    private final static Logger logger = LoggerFactory.getLogger(MinioOutputStreamTest.class.getName());

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
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "mostest/file1");
            SecureRandom random = new SecureRandom();
            int partsize = MinioFSSuiteTest.getConf().getInt(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, 0);
            assert partsize != 0;
            byte[] tmpData = new byte[3 << 20];
            try (MinioOutputStream mos = new MinioOutputStream(p, MinioFSSuiteTest.getConf(), partsize)) {
                for (int i = 0; i < 3; i++) {
                    random.nextBytes(tmpData);
                    mos.write(tmpData);
                }
            }

            FileStatus fileStatus = MinioFSSuiteTest.getMinioUtil().getFileStatus(p);
            assert fileStatus.isFile();
            assert fileStatus.getLen() == 9 << 20;

        } catch (IOException ex) {
            logger.error("error occured", ex);
            assert false;
        }

    }
}
