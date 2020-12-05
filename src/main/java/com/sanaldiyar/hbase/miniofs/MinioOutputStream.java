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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UploadHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioOutputStream extends OutputStream {

    private final static Logger logger = LoggerFactory.getLogger(MinioOutputStream.class.getName());

    private final Configuration conf;
    private final MultipartUploader uploader;
    private final Path path;
    private final UploadHandle uploadHandle;
    private final Map<Integer, PartHandle> parts;

    private int partNo = 1;
    private int backendOffset = 0;
    private int partSize;
    private File backendFile;
    private OutputStream backendStream;
    private boolean closed;
    private long totalWriten = 0;

    public MinioOutputStream(Path path, Configuration conf) throws IOException {
        this.path = path;
        this.conf = conf;
        this.uploader = MinioMultipartUploader.Factory.get(path.getFileSystem(conf), conf);
        this.uploadHandle = this.uploader.initialize(path);
        this.parts = new HashMap<>();
        this.backendFile = createBackendFile();
        this.backendStream = new BufferedOutputStream(new FileOutputStream(backendFile));
        this.closed = false;
        this.partSize = conf.getInt(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, MinioFileSystem.MINIO_DEFAULT_PART_SIZE);
    }

    public MinioOutputStream(Path path, Configuration conf, int partSize) throws IOException {
        this(path, conf);
        this.partSize = partSize;
    }

    private File createBackendFile() throws IOException {
        File dir = new File(conf.get("hadoop.tmp.dir"));
        if (!dir.mkdirs() && !dir.exists()) {
            throw new IOException("Cannot create tmp buffer directory: " + dir);
        }
        File result = File.createTempFile("output-", ".tmp", dir);
        logger.debug("a new backend {} created for the path {}", result.toPath(), path);
        result.deleteOnExit();
        return result;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public synchronized void write(byte[] buffer, int offset, int len) throws IOException {
        if (offset < 0 || len < 0 || (offset + len) > buffer.length) {
            throw new IndexOutOfBoundsException("Invalid offset/length for write");
        }
        if (closed) {
            throw new IOException("stream is closed");
        }
        while (backendOffset + len > partSize) {
            int bytesCW = partSize - backendOffset;
            backendStream.write(buffer, offset, bytesCW);
            backendOffset += bytesCW;
            totalWriten += bytesCW;
            logger.debug("{} bytes of data writen to the backend file {} from offset {} new position {} for path {}", bytesCW, backendFile.toPath(), offset, totalWriten, path);
            uploadPart(false);
            offset += bytesCW;
            len -= bytesCW;
        }
        backendStream.write(buffer, offset, len);
        backendOffset += len;
        totalWriten += len;
        logger.debug("{} bytes of data writen to the backend file {} from offset {} new position {} for path {}", len, backendFile.toPath(), offset, totalWriten, path);
    }

    @Override
    public void flush() throws IOException {
        backendStream.flush();
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }
        closed = true;
        uploadPart(true);
        backendFile = null;
        backendStream = null;
        logger.debug("{} bytes of data writen to the destination {}", totalWriten, path);
    }

    private synchronized void uploadPart(boolean lastPart) throws IOException {
        backendStream.flush();
        backendStream.close();
        logger.debug("sending file {}", backendFile.toPath());

        InputStream is = new BufferedInputStream(new FileInputStream(backendFile));

        PartHandle ph = uploader.putPart(path, is, partNo, uploadHandle, backendOffset);
        parts.put(partNo, ph);
        partNo++;

        if (lastPart) {
            uploader.complete(path, parts, uploadHandle);
        } else {
            backendOffset = 0;
            backendFile = createBackendFile();
            backendStream = new BufferedOutputStream(new FileOutputStream(backendFile));
        }
        logger.debug("sending file {} completed", backendFile.toPath());
    }

}
