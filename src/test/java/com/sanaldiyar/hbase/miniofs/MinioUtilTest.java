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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioUtilTest {

    private final static Logger logger = LoggerFactory.getLogger(MinioUtilTest.class.getName());

    @BeforeAll
    public static void setUpClass() {
        MinioFSSuiteTest.init();
    }

    @BeforeEach
    public void cleanup() throws Exception {
        MinioFSSuiteTest.cleanUp();
    }

    @Test
    public void testMakeSingleDir() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(p) == true;
            FileStatus fileStatus = MinioFSSuiteTest.getMinioUtil().getFileStatus(p);
            assert fileStatus.isDirectory();
            assert fileStatus.getPath().toUri().equals(p.toUri());
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testMakeRecursiveDir() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test/subdir/subsubdir");
            Path pp = new Path(MinioFSSuiteTest.getRootPath(), "test/subdir");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(p) == true;
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isDirectory();
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(pp).isDirectory();
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testCreateFile() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test/file1");
            String data = "HELLO WORLD";
            byte[] bdata = data.getBytes();
            MinioFSSuiteTest.getMinioUtil().putStream(p, new ByteArrayInputStream(bdata), bdata.length);
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isFile();
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testCreateFileNotExistsDir() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test/no-exists-dir/file1");
            String data = "HELLO WORLD";
            byte[] bdata = data.getBytes();
            MinioFSSuiteTest.getMinioUtil().putStream(p, new ByteArrayInputStream(bdata), bdata.length);
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isFile();
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testDeleteFile() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test/file2");
            String data = "HELLO WORLD";
            byte[] bdata = data.getBytes();
            MinioFSSuiteTest.getMinioUtil().putStream(p, new ByteArrayInputStream(bdata), bdata.length);
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isFile();
            assert MinioFSSuiteTest.getMinioUtil().delete(p, false);
            try {
                assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p) == null;
            } catch (FileNotFoundException ne) {
                assert true;
            }
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testDeleteFolder() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test3/subdir/subsubdir");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(p) == true;
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isDirectory();
            assert MinioFSSuiteTest.getMinioUtil().delete(p, false);
            try {
                assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p) == null;
            } catch (FileNotFoundException ne) {
                assert true;
            }
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testDeleteFolderRecursive() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test3/subdir/subsubdir");
            Path parent = new Path(MinioFSSuiteTest.getRootPath(), "test3");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(p) == true;
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isDirectory();
            assert MinioFSSuiteTest.getMinioUtil().delete(parent, true);
            try {
                assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p) == null;
            } catch (FileNotFoundException ne) {
                assert true;
            }
            try {
                assert MinioFSSuiteTest.getMinioUtil().getFileStatus(new Path(MinioFSSuiteTest.getRootPath(), "test3/subdir")) == null;
            } catch (FileNotFoundException ne) {
                assert true;
            }
            try {
                assert MinioFSSuiteTest.getMinioUtil().getFileStatus(new Path(MinioFSSuiteTest.getRootPath(), "test3")) == null;
            } catch (FileNotFoundException ne) {
                assert true;
            }
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testDeleteFolderRecursiveFailNonEmpty() {
        try {
            Path p = new Path(MinioFSSuiteTest.getRootPath(), "test3/subdir/subsubdir");
            Path parent = new Path(MinioFSSuiteTest.getRootPath(), "test3");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(p) == true;
            assert MinioFSSuiteTest.getMinioUtil().getFileStatus(p).isDirectory();
            assert MinioFSSuiteTest.getMinioUtil().delete(parent, false);
            assert false;
        } catch (IOException e) {
            assert true;
        }
    }

    @Test
    public void testListStatus() {
        try {
            Path parent = new Path(MinioFSSuiteTest.getRootPath(), "listnr");
            Path path1 = new Path(MinioFSSuiteTest.getRootPath(), "listnr/path1");
            Path path2 = new Path(MinioFSSuiteTest.getRootPath(), "listnr/path2");
            Path path3 = new Path(MinioFSSuiteTest.getRootPath(), "listnr/path2/path3");

            Path[] childs = new Path[]{path1, path2};
            Path[] paths = new Path[]{parent, path1, path2, path3};
            for (var p : paths) {
                assert MinioFSSuiteTest.getMinioUtil().mkdirs(p);
            }

            FileStatus[] fileStatuses = MinioFSSuiteTest.getMinioUtil().listStatus(parent);
            assert fileStatuses.length == childs.length;

            for (var fileStatus : fileStatuses) {
                assert !fileStatus.getPath().equals(path3);
            }

        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testListStatusRecursive() {
        try {
            Path parent = new Path(MinioFSSuiteTest.getRootPath(), "listr");
            Path path1 = new Path(MinioFSSuiteTest.getRootPath(), "listr/path1");
            Path path2 = new Path(MinioFSSuiteTest.getRootPath(), "listr/path2");
            Path path3 = new Path(MinioFSSuiteTest.getRootPath(), "listr/path2/path3");

            Path[] childs = new Path[]{path1, path2, path3};
            Path[] paths = new Path[]{parent, path1, path2, path3};
            for (var p : paths) {
                assert MinioFSSuiteTest.getMinioUtil().mkdirs(p);
            }

            FileStatus[] fileStatuses = MinioFSSuiteTest.getMinioUtil().listStatus(parent, true);
            assert fileStatuses.length == childs.length;

            for (var fileStatus : fileStatuses) {
                logger.debug("returned file status {}", fileStatus);
                boolean found = false;
                for (var c : childs) {
                    if (fileStatus.getPath().toUri().getPath().equals(c.toUri().getPath())) {
                        found = true;
                    }
                }
                if (!found) {
                    logger.debug("path is not in child {}", fileStatus.getPath());
                }
                assert found;
            }

        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testRenameFile() {
        try {
            Path pathSrc = new Path(MinioFSSuiteTest.getRootPath(), "renamenr/path2/src");
            Path pathDst = new Path(MinioFSSuiteTest.getRootPath(), "renamenr/path2/dst");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(pathSrc);
            assert MinioFSSuiteTest.getMinioUtil().rename(pathSrc, pathDst);
            FileStatus fileStatus = MinioFSSuiteTest.getMinioUtil().getFileStatus(pathDst);
            assert fileStatus != null;
            assert fileStatus.getPath().equals(pathDst);
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }

    @Test
    public void testRenameFileRecursive() {
        try {
            Path pathWithChilds = new Path(MinioFSSuiteTest.getRootPath(), "renamer/child1/child2");

            Path pathSrc = new Path(MinioFSSuiteTest.getRootPath(), "renamer");
            Path pathDst = new Path(MinioFSSuiteTest.getRootPath(), "renamer-new");
            assert MinioFSSuiteTest.getMinioUtil().mkdirs(pathWithChilds);

            assert MinioFSSuiteTest.getMinioUtil().rename(pathSrc, pathDst);

            Path pathNewChild1 = new Path(MinioFSSuiteTest.getRootPath(), "renamer-new/child1");
            Path pathNewChild2 = new Path(MinioFSSuiteTest.getRootPath(), "renamer-new/child1/child2");

            FileStatus fileStatus;

            fileStatus = MinioFSSuiteTest.getMinioUtil().getFileStatus(pathDst);
            assert fileStatus != null;
            assert fileStatus.getPath().equals(pathDst);

            fileStatus = MinioFSSuiteTest.getMinioUtil().getFileStatus(pathNewChild1);
            assert fileStatus != null;
            assert fileStatus.getPath().equals(pathNewChild1);

            fileStatus = MinioFSSuiteTest.getMinioUtil().getFileStatus(pathNewChild2);
            assert fileStatus != null;
            assert fileStatus.getPath().equals(pathNewChild2);
        } catch (IOException e) {
            logger.error("test failed", e);
            assert false;
        }
    }
}
