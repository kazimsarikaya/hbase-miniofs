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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioFileSystem extends FileSystem {

    public final static String MINIO_STREAM_BUFFER_SIZE = "fs.minio.stream-buffer.size";
    public final static String MINIO_UPLOAD_PART_SIZE = "fs.minio.upload-part.size";
    public final static String MINIO_ROOT = "hbase.rootdir";
    public final static int MINIO_DEFAULT_PART_SIZE = 5 << 20;
    public final static int MINIO_DEFAULT_BUFFER_SIZE = 128 << 10;

    private final static Logger logger = LoggerFactory.getLogger(MinioFileSystem.class.getName());

    private URI uri;
    private Path workingDir;
    private final MinioUtil minioUtil = MinioUtil.getInstance();

    public MinioFileSystem() {
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {

        minioUtil.setConf(conf);
        this.uri = minioUtil.getUri();
        this.workingDir = new Path(uri);
        logger.debug("workingdir {}", workingDir);

        setConf(conf);
        super.initialize(name, conf);
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return getConf().getLong(MINIO_STREAM_BUFFER_SIZE, MINIO_DEFAULT_BUFFER_SIZE);
    }

    public long getDefaultPartsize(Path f) {
        return getConf().getLong(MINIO_UPLOAD_PART_SIZE, MINIO_DEFAULT_PART_SIZE);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        path = makeAbsolute(path);
        logger.debug("try to open path {} with buffer {}", path.toUri().getPath(), bufferSize);
        try {
            FileStatus fs = getFileStatus(path);
            if (!fs.isFile()) {
                throw new IOException(String.format("requested path is not file %s", path.toString()));
            }
        } catch (FileNotFoundException ex) {
            throw ex;
        }
        MinioInputStream mis = new MinioInputStream(path, getConf(), getDefaultBlockSize(path));
        return new FSDataInputStream(mis);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean override, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return create_internal(path, permission, override, bufferSize, replication, blockSize, progress, true);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        boolean override = flags.contains(CreateFlag.OVERWRITE);
        return create_internal(path, permission, override, bufferSize, replication, blockSize, progress, false);
    }

    private FSDataOutputStream create_internal(Path path, FsPermission perm, boolean override, int bufferSize, short replication, long blockSize, Progressable p, boolean recursive) throws IOException {
        path = makeAbsolute(path);
        logger.debug("new file will be created for {}", path);
        Path parent = path.getParent();
        if (recursive) {
            mkdirs(parent, perm);
        } else {
            FileStatus p_fs = getFileStatus(parent);
            if (!p_fs.isDirectory()) {
                throw new IOException(String.format("cannot create file %s parent %s is not directory", path, parent));
            }
        }
        if (override) {
            delete(path, false);
        } else {
            boolean exists = false;
            try {
                getFileStatus(path);
                exists = true;
            } catch (FileNotFoundException ex) {

            }
            if (exists) {
                throw new FileAlreadyExistsException(String.format("path already exists %s", path.toString()));
            }
        }
        MinioOutputStream mos = new MinioOutputStream(path, getConf());
        return new FSDataOutputStream(mos, null);
    }

    @Override
    public boolean rename(Path source, Path destination) throws IOException {
        logger.debug("renaming old path {} to new path {}", source, destination);
        return minioUtil.rename(makeAbsolute(source), makeAbsolute(destination));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return minioUtil.delete(makeAbsolute(path), recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        return minioUtil.listStatus(makeAbsolute(path));
    }

    @Override
    public void setWorkingDirectory(Path path) {
        logger.debug("new working directory will be {} old working directroy is {}", path, workingDir);
        workingDir = makeAbsolute(path);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission perm) throws IOException {
        return minioUtil.mkdirs(makeAbsolute(path));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return minioUtil.getFileStatus(makeAbsolute(path));
    }

    @Override
    public String getScheme() {
        return "minio";
    }

    protected Path makeAbsolute(Path path) {
        assert !path.toString().contains("%2C");
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable p) throws IOException {
        throw new UnsupportedOperationException("Minio File System does not support append");
    }

}
