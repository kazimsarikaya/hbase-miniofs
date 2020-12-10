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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisplayName("Minio File System Tests")
public class MinioFileSystemTest extends BaseTestClass {

    private final static Logger logger = LoggerFactory.getLogger(MinioFileSystemTest.class.getName());

    @Test
    @DisplayName("Test file system uri setted as configured")
    public void testGetUri() {
        try {
            URI atConf = new URI(getConf().get(MinioFileSystem.MINIO_ROOT));
            URI atFS = getFileSystem().getUri();
            assert atConf.getScheme().equals(atFS.getScheme());
            assert atConf.getHost().equals(atFS.getHost());
            assert atConf.getPort() == atFS.getPort();
            assert atConf.getPath().equals(atFS.getPath());
        } catch (URISyntaxException ex) {
            logger.error("error occured while checking uri {}", ex);
            assert false;
        }
    }

    @Test
    @DisplayName("Test root path setted as configured")
    public void testRootFileStatus() {
        try {
            FileStatus f_s = getFileSystem().getFileStatus(getRootPath());
            assert f_s.isDirectory();
            assert f_s.getPath().equals(getRootPath());
        } catch (IOException ex) {
            logger.error("cannot get status", ex);
            assert false;
        }
    }

    @Test
    @DisplayName("Test create directory")
    public void testCreateDir() {
        try {
            Path newDir = new Path(getRootPath(), "dir1/subdir1/subdir2");
            boolean result = getFileSystem().mkdirs(newDir, null);
            assert result;
            FileStatus[] fileStatuses = getFileSystem().listStatus(getRootPath(), (Path path) -> path.toUri().getPath().startsWith("/test/dir1"));
            assert fileStatuses.length == 1;
            assert getFileSystem().getFileStatus(newDir) != null;

        } catch (IOException ex) {
            logger.error("cannot create/check dir", ex);
            assert false;
        }
    }

    @Test
    public void testAppendFile() {
        try {
            getFileSystem().append(new Path("dummy"));
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
            getFileSystem().delete(new Path("nonexists-delete"), true);
            assert true;
        } catch (IOException ex) {
            logger.error("test failed", ex);
            assert false;
        }
    }

    @Test
    public void testCreateFile() {
        try {
            FSDataOutputStream os = getFileSystem().create(new Path(getRootPath(), "cftest/file1"));
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
            FSDataOutputStream os = getFileSystem().create(new Path(getRootPath(), "cftest-failed/file1"));
            os.close();
            assert true;
            FSDataOutputStream os2 = getFileSystem().create(new Path(getRootPath(), "cftest-failed/file1/failed"));
            os2.close();
            assert false;
        } catch (IOException ex) {
            assert true;
        }
    }

    @Test
    public void testCreateFileFailedNO() {
        try {
            Path path = new Path(getRootPath(), "cftest-failed/file2");
            FSDataOutputStream os = getFileSystem().create(path, false, 128 << 10);
            os.close();
            assert true;
            FSDataOutputStream os2 = getFileSystem().create(path, false, 128 << 10);
            os2.close();
            assert false;
        } catch (IOException ex) {
            assert true;
        }
    }

    @Test
    public void testCreateNRFile() {
        try {
            Path path = new Path(getRootPath(), "cftestnr/file1");
            getFileSystem().mkdirs(new Path(getRootPath(), "cftestnr"));
            FSDataOutputStream os = getFileSystem().createNonRecursive(path, true, 0, (short) 0, 0, null);
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
            FSDataOutputStream os = getFileSystem().createNonRecursive(new Path(getRootPath(), "cftestnr-failed/file1"), true, 0, (short) 0, 0, null);
            os.close();
            assert false;
        } catch (IOException ex) {
            assert true;
        }
    }

    @Test
    public void testQualifiedRootPath() {
        Path path = new Path(getRootPath(), "qualified/path");
        Path qpath = path.makeQualified(getFileSystem().getUri(), getFileSystem().getWorkingDirectory());
        assert path.equals(qpath);
    }

    @Test
    public void testRemoveRootFromPath() {
        Path path = new Path(getRootPath(), "qualified/toremove/path");
        Path rootPath = new Path(getRootPath(), "qualified/toremove/");
        Path qpath = rootPath.makeQualified(getFileSystem().getUri(), getFileSystem().getWorkingDirectory());

        String strPath = path.toString();
        String remStrPath = strPath.substring(qpath.toString().length() + 1);
        assert remStrPath.equals("path");
    }

    @Test
    public void testFSRemoveFromPath() {
        try {
            Path path = new Path(getRootPath(), "qualified/toremove/path");
            Path rootPath = new Path(getRootPath(), "qualified/toremove/");
            Path qpath = rootPath.makeQualified(getFileSystem().getUri(), getFileSystem().getWorkingDirectory());
            assert getFileSystem().mkdirs(path, null);

            FileStatus fsPath = getFileSystem().getFileStatus(path);
            FileStatus fsRootPath = getFileSystem().getFileStatus(rootPath);

            String strPath = fsPath.getPath().toString();
            String remStrPath = strPath.substring(qpath.toString().length() + 1);
            assert remStrPath.equals("path");

            remStrPath = strPath.substring(fsRootPath.getPath().toString().length() + 1);
            assert remStrPath.equals("path");

            fsRootPath = getFileSystem().getFileStatus(new Path("qualified/toremove/"));
            remStrPath = strPath.substring(fsRootPath.getPath().toString().length() + 1);
            logger.debug("rempath {}", remStrPath);
            assert remStrPath.equals("path");

        } catch (IOException e) {
        }
    }

}
