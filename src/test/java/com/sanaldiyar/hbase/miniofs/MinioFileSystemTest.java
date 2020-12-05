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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioFileSystemTest {

    private final static Logger logger = LoggerFactory.getLogger(MinioFileSystemTest.class.getName());

    @BeforeAll
    public static void setUpClass() {
        MinioFSSuiteTest.init();
    }

    @Test
    public void testGetUri() {
        try {
            assert MinioFSSuiteTest.getFileSystem().getUri().compareTo(new URI(MinioFSSuiteTest.getConf().get(MinioFileSystem.MINIO_ROOT))) == 0;
        } catch (URISyntaxException ex) {
            logger.error("error occured while checking uri {}", ex);
            assert false;
        }
    }

    @Test
    public void testRootFileStatus() {
        try {
            FileStatus f_s = MinioFSSuiteTest.getFileSystem().getFileStatus(MinioFSSuiteTest.getRootPath());
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
            Path newDir = new Path(MinioFSSuiteTest.getRootPath(), "/dir1/subdir1/subdir2");
            boolean result = MinioFSSuiteTest.getFileSystem().mkdirs(newDir, null);
            assert result;
            FileStatus[] fileStatuses = MinioFSSuiteTest.getFileSystem().listStatus(MinioFSSuiteTest.getRootPath(), (Path path) -> path.toUri().getPath().startsWith("/dir1"));
            assert fileStatuses.length == 1;

        } catch (IOException ex) {
            logger.error("cannot create/check dir", ex);
            assert false;
        }
    }

    @Test
    public void testAppendFile() {
        try {
            MinioFSSuiteTest.getFileSystem().append(new Path("/dummy"));
            assert false;
        } catch (IOException ex) {
            logger.error("test failed", ex);
            assert false;
        } catch (UnsupportedOperationException ex) {
            assert true;
        }
    }

    @Test
    public void testDeleteNonExistedPath() {
        try {
            MinioFSSuiteTest.getFileSystem().delete(new Path("/nonexists-delete"), true);
            assert true;
        } catch (IOException ex) {
            logger.error("test failed", ex);
            assert false;
        }
    }

    @Test
    public void testCreateFile() {
        try {
            FSDataOutputStream os = MinioFSSuiteTest.getFileSystem().create(new Path(MinioFSSuiteTest.getRootPath(), "/cftest/file1"));
            os.close();
            assert true;
        } catch (IOException ex) {
            logger.error("test failed", ex);
            assert false;
        }
    }

    @Test
    public void testCreateFileFailed() {
        try {
            FSDataOutputStream os = MinioFSSuiteTest.getFileSystem().create(new Path(MinioFSSuiteTest.getRootPath(), "/cftest-failed/file1"));
            os.close();
            assert true;
            FSDataOutputStream os2 = MinioFSSuiteTest.getFileSystem().create(new Path(MinioFSSuiteTest.getRootPath(), "/cftest-failed/file1/failed"));
            os2.close();
            assert false;
        } catch (IOException ex) {
            assert true;
        }
    }

    @Test
    public void testCreateFileFailedNO() {
        try {
            Path path = new Path(MinioFSSuiteTest.getRootPath(), "/cftest-failed/file2");
            FSDataOutputStream os = MinioFSSuiteTest.getFileSystem().create(path, false, 128 << 10);
            os.close();
            assert true;
            FSDataOutputStream os2 = MinioFSSuiteTest.getFileSystem().create(path, false, 128 << 10);
            os2.close();
            assert false;
        } catch (IOException ex) {
            assert true;
        }
    }

    @Test
    public void testCreateNRFile() {
        try {
            Path path = new Path(MinioFSSuiteTest.getRootPath(), "/cftestnr/file1");
            MinioFSSuiteTest.getFileSystem().mkdirs(new Path(MinioFSSuiteTest.getRootPath(), "/cftestnr"));
            FSDataOutputStream os = MinioFSSuiteTest.getFileSystem().createNonRecursive(path, true, 0, (short) 0, 0, null);
            os.close();
            assert true;
        } catch (IOException ex) {
            logger.error("test failed", ex);
            assert false;
        }
    }

    @Test
    public void testCreateNRFileFailed() {
        try {
            FSDataOutputStream os = MinioFSSuiteTest.getFileSystem().createNonRecursive(new Path(MinioFSSuiteTest.getRootPath(), "/cftestnr-failed/file1"), true, 0, (short) 0, 0, null);
            os.close();
            assert false;
        } catch (IOException ex) {
            assert true;
        }
    }

}
