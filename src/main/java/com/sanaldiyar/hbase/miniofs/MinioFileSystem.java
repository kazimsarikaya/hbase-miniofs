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
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioFileSystem extends FileSystem {

    public final static String MINIO_STREAM_BUFFER_SIZE = "fs.minio.stream-buffer.size";
    public final static String MINIO_UPLOAD_PART_SIZE = "fs.minio.upload-part.size";
    public final static String MINIO_ROOT = "hbase.rootdir";
    public final static int MINIO_DEFAULT_PART_SIZE = 5 << 20;
    public final static int MINIO_DEFAULT_BUFFER_SIZE = 128 << 10;
    public final static String MINIO_DEFAULT_SHUTDOWN = "fs.minio.shutdown.timeout";
    public final static long MINIO_DEFAULT_SHUTDOWN_TIMEOUT = 5;
    public final static TimeUnit MINIO_DEFAULT_SHUTDOWN_TIMEOUT_UNIT = TimeUnit.MINUTES;

    private final static Logger logger = LoggerFactory.getLogger(MinioFileSystem.class.getName());

    private final static List<String> locks = Collections.synchronizedList(new LinkedList<>());

    private URI uri;
    private Path workingDir;
    private final MinioUtil minioUtil = MinioUtil.getInstance();
    private final List<MinioOutputStream> outputStreams = Collections.synchronizedList(new LinkedList<>());
    private boolean closed = false;

    public MinioFileSystem() {
    }

    public static List<String> getLocks() {
        return locks;
    }

    List<MinioOutputStream> getOutputStreams() {
        return outputStreams;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {

        minioUtil.setConf(conf);
        this.uri = minioUtil.getUri();
        this.workingDir = new Path(uri);
        logger.debug("workingdir {}", workingDir);
        long timeout = conf.getTimeDuration(MINIO_DEFAULT_SHUTDOWN, MINIO_DEFAULT_SHUTDOWN_TIMEOUT, MINIO_DEFAULT_SHUTDOWN_TIMEOUT_UNIT, MINIO_DEFAULT_SHUTDOWN_TIMEOUT_UNIT);
        ShutdownHookManager.get().addShutdownHook(new Thread(new ShutdownHook(this)), SHUTDOWN_HOOK_PRIORITY, timeout, TimeUnit.MINUTES);
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
        try {
            FileStatus fs = getFileStatus(path);
            if (!fs.isFile()) {
                throw new IOException(String.format("requested path is not file %s", path.toString()));
            }
        } catch (FileNotFoundException ex) {
            throw ex;
        }
        MinioInputStream mis = new MinioInputStream(path, getConf(), getDefaultBlockSize(path));
        logger.debug("file {} opened with buffer {}", path, bufferSize);
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
        if (!checkLock(path)) {
            throw new IOException(String.format("path is locked %s", path));
        }
        Path parent = path.getParent();
        if (recursive) {
            mkdirs(parent, perm);
        } else {
            FileStatus p_fs = getFileStatus(parent);
            if (!p_fs.isDirectory()) {
                throw new ParentNotDirectoryException(String.format("cannot create file %s parent %s is not directory", path, parent));
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
        FSDataOutputStream outputStream = new FSDataOutputStream(mos, null);
        outputStreams.add(mos);
        logger.info("new file will be created for {}", path);
        return outputStream;
    }

    private boolean checkLock(Path path) throws IOException {
        String searchKey = minioUtil.getPrefix(path);
        boolean islocked = true;
        while (islocked) {
            logger.trace("CHECKLOCK checking lock for path {}", path);
            islocked = false;
            synchronized (locks) {
                for (var lock : locks) {
                    if (lock.startsWith(searchKey)) {
                        logger.trace("CHECKLOCK lock for path {} by path {}", path, lock);
                        islocked = true;
                        break;
                    }
                }
            }
            if (islocked) {
                try {
                    logger.trace("CHECKLOCK lock found for path {} sleeping 100ms", path);
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    logger.error("lock holding interrupted", ex);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean rename(Path source, Path destination) throws IOException {
        source = makeAbsolute(source);
        destination = makeAbsolute(destination);
        logger.debug("renaming old path {} to new path {}", source, destination);
        if (!checkLock(source)) {
            return false;
        }
        return minioUtil.rename(source, destination);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        path = makeAbsolute(path);
        logger.debug("deleting path {} with {}", path, recursive);
        if (!checkLock(path)) {
            return false;
        }
        boolean result = minioUtil.delete(path, recursive);
        logger.info("path {} {} deleted with option recursive={}", path, result ? "can" : "cannot", recursive);
        return result;
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

    public synchronized void internalClose() throws IOException {

    }

//    @Override
//    public void close() throws IOException {
//        internalClose();
//        super.close();
//    }
    public boolean isClosed() {
        return closed;
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable p) throws IOException {
        throw new UnsupportedOperationException("Minio File System does not support append");
    }

    static class ShutdownHook implements Runnable {

        private final MinioFileSystem fileSystem;
        private final static Executor executor = Executors.newFixedThreadPool(10);

        public ShutdownHook(MinioFileSystem fileSystem) {
            this.fileSystem = fileSystem;
        }

        @Override
        public void run() {
            logger.debug("closing file system {}", this.toString());
            for (var os : fileSystem.getOutputStreams()) {
                executor.execute(() -> {
                    try {
                        if (fileSystem.isClosed()) {
                            return;
                        }
                        if (!os.isClosed()) {
                            try {
                                logger.debug("closing output steam {}", os.toString());
                                os.close();
                                logger.debug("output steam {} closed", os.toString());

                            } catch (IOException e) {
                                logger.error("error at closing output stream {} , error: {}", os.toString(), e);
                                throw e;
                            }
                        }

                    } catch (IOException ex) {
                        logger.error("error at shutdown", ex);
                    }
                });
            }
            fileSystem.closed = true;
            logger.debug("file system {} closed", this.toString());

        }

    }

}
