/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

/**
 *
 * @author kazim
 */
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
            logger.debug("{} bytes of data writen to the backend file {} from offset {}", bytesCW, backendFile.toPath(), offset);
            uploadPart(false, backendFile);
            offset += bytesCW;
            len -= bytesCW;
        }
        backendStream.write(buffer, offset, len);
        backendOffset += len;
        logger.debug("{} bytes of data writen to the backend file {} from offset {}", len, backendFile.toPath(), offset);
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
        uploadPart(true, backendFile);
        backendFile = null;
        backendStream = null;

    }

    private synchronized void uploadPart(boolean lastPart, File part) throws IOException {

        backendStream.close();
        logger.debug("sending file {}", part.toPath());

        InputStream is = new BufferedInputStream(new FileInputStream(part));

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
        logger.debug("sending file {} completed", part.toPath());
    }

}
