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
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioFileSystemTest {

    private final static MinioUtil minioUtil = MinioUtil.getInstance();
    private static Configuration conf;
    private final static Logger logger = LoggerFactory.getLogger(MinioFileSystemTest.class.getName());

    private FileSystem fs;
    private Path root;

    public MinioFileSystemTest() {
    }

    @BeforeAll
    public static void setUpClass() {

        conf = new Configuration();
        conf.set(MinioFileSystem.MINIO_ENDPOINT, "http://localhost:9000");
        conf.set(MinioFileSystem.MINIO_ROOT, "minio://test");
        conf.set(MinioFileSystem.MINIO_ACCESS_KEY, "minioadmin");
        conf.set(MinioFileSystem.MINIO_SECRET_KEY, "minioadmin");
        conf.set(MinioFileSystem.MINIO_STREAM_BUFFER_SIZE, String.valueOf(128 << 10));
        conf.set(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, String.valueOf(8 << 20));

        minioUtil.setConf(conf);
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
        try {
            root = new Path(conf.get(MinioFileSystem.MINIO_ROOT));
            fs = root.getFileSystem(conf);
            fs.setWorkingDirectory(root);
        } catch (IOException ex) {
            logger.error("cannot get fs", ex);
            assert false;
        }
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testGetUri() {
        try {
            assert fs.getUri().compareTo(new URI(conf.get(MinioFileSystem.MINIO_ROOT))) == 0;
        } catch (URISyntaxException ex) {
            logger.error("error occured while checking uri {}", ex);
            assert false;
        }
    }

    @Test
    public void testRootFileStatus() {
        try {
            FileStatus f_s = fs.getFileStatus(root);
            assert f_s.isDirectory();
            assert f_s.getPath().isRoot();
        } catch (IOException ex) {
            logger.error("cannot get status", ex);
            assert false;
        }
    }

    @Test
    public void testCreateDir() {
        try {
            Path newDir = new Path(root, "/dir1/subdir1/subdir2");
            boolean result = fs.mkdirs(newDir, null);
            assert result;
            FileStatus[] fileStatuses = fs.listStatus(root, (Path path) -> path.toUri().getPath().startsWith("/dir1"));
            assert fileStatuses.length == 3;

        } catch (IOException ex) {
            logger.error("cannot create/check dir", ex);
            assert false;
        }
    }

    @Test
    public void testAppendFile() {
        try {
            fs.append(new Path("/dummy"));
            assert false;
        } catch (IOException ex) {
            assert false;
        } catch (UnsupportedOperationException ex) {
            assert true;
        }
    }

    @Test
    public void testDeleteNonExistedPath() {
        try {
            fs.delete(new Path("/nonexists-delete"), true);
            assert true;
        } catch (IOException ex) {
            assert false;
        }
    }

}
