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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnitPlatform.class)
@SelectPackages("com.sanaldiyar.hbase.miniofs")
public class MinioFSSuiteTest {

    private final static Logger logger = LoggerFactory.getLogger(MinioFSSuiteTest.class.getName());

    private final static MinioUtil minioUtil = MinioUtil.getInstance();
    private static Configuration conf;

    private static FileSystem fileSystem;
    private static Path rootPath;

    public static void init() {

        try {
            conf = new Configuration();
            conf.set(MinioFileSystem.MINIO_ENDPOINT, "http://localhost:9000");
            conf.set(MinioFileSystem.MINIO_ROOT, "minio://test/");
            conf.set(MinioFileSystem.MINIO_ACCESS_KEY, "minioadmin");
            conf.set(MinioFileSystem.MINIO_SECRET_KEY, "minioadmin");
            conf.set(MinioFileSystem.MINIO_STREAM_BUFFER_SIZE, String.valueOf(128 << 10));
            conf.set(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, String.valueOf(8 << 20));

            minioUtil.setConf(conf);

            rootPath = new Path(conf.get(MinioFileSystem.MINIO_ROOT));
            fileSystem = rootPath.getFileSystem(conf);
            fileSystem.setWorkingDirectory(rootPath);
        } catch (IOException ex) {
            logger.error("error occured", ex);
            assert false;
        }
    }

    public static MinioUtil getMinioUtil() {
        return minioUtil;
    }

    public static FileSystem getFileSystem() {
        return fileSystem;
    }

    public static Path getRootPath() {
        return rootPath;
    }

    public static Configuration getConf() {
        return conf;
    }

}
