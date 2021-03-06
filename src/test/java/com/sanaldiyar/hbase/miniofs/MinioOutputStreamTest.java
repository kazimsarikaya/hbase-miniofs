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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioOutputStreamTest extends BaseTestClass {

    private final static Logger logger = LoggerFactory.getLogger(MinioOutputStreamTest.class.getName());

    @Test
    public void testCreateFile() {

        try {
            FileSystem.Statistics statistics = new FileSystem.Statistics("minio");
            Path p = new Path(getRootPath(), "mostest/file1");
            SecureRandom random = new SecureRandom();
            int partsize = getConf().getInt(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, 0);
            assert partsize != 0;
            byte[] tmpData = new byte[3 << 20];
            try (MinioOutputStream mos = new MinioOutputStream(p, getConf(), partsize, statistics)) {
                for (int i = 0; i < 3; i++) {
                    random.nextBytes(tmpData);
                    mos.write(tmpData);
                }
            }

            FileStatus fileStatus = getMinioUtil().getFileStatus(p);
            assert fileStatus.isFile();
            assert fileStatus.getLen() == 9 << 20;

        } catch (IOException ex) {
            logger.error("error occured", ex);
            assert false;
        }

    }
}
