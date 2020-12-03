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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderFactory;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kazim
 */
public class MinioMultipartUploader extends MultipartUploader {

    private final static Logger logger = LoggerFactory.getLogger(MultipartUploader.class.getName());

    private final Configuration conf;
    private final FileSystem fs;
    private final MinioUtil minioUtil = MinioUtil.getInstance();

    public MinioMultipartUploader(FileSystem fs, Configuration conf) {
        this.fs = fs;
        this.conf = conf;
    }

    @Override
    public UploadHandle initialize(Path filePath) throws IOException {
        return new MinioMultipartUploadHandle(fs, filePath);
    }

    @Override
    public PartHandle putPart(Path filePath, InputStream inputStream, int partNumber, UploadHandle uploadId, long len) throws IOException {
        MinioMultipartUploadHandle uh = (MinioMultipartUploadHandle) uploadId;
        Path dstDir = uh.getTmpPath();
        Path dst = new Path(dstDir, "part-" + String.valueOf(partNumber));
        minioUtil.putStream(dst, inputStream, len);
        return new MinioMultipartPartHandle(dst, partNumber);
    }

    @Override
    public PathHandle complete(Path filePath, Map<Integer, PartHandle> handles, UploadHandle uploadId) throws IOException {
        MinioMultipartUploadHandle uh = (MinioMultipartUploadHandle) uploadId;
        Path tmpDir = uh.getTmpPath();

        int partCnt = handles.size();
        Path[] items = new Path[partCnt];
        for (int i = 0; i < partCnt; i++) {
            items[i] = ((MinioMultipartPartHandle) handles.get(i + 1)).getPartPath();
        }

        minioUtil.mergeAndClean(filePath, items, tmpDir);

        return new MinioMultiPartPathHandle(filePath);
    }

    @Override
    public void abort(Path filePath, UploadHandle multipartUploadId) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static class Factory extends MultipartUploaderFactory {

        @Override
        protected MultipartUploader createMultipartUploader(FileSystem fs, Configuration conf) throws IOException {
            if (fs.getScheme().equals("minio")) {
                return new MinioMultipartUploader(fs, conf);
            }
            logger.error(String.format("invalid fs: %s", fs.getScheme()));
            return null;
        }

    }

}

class MinioMultipartUploadHandle implements UploadHandle {

    private final Path path;
    private final Path tmpPath;

    public MinioMultipartUploadHandle(FileSystem fs, Path path) {
        this.path = path;
        UUID uuid = UUID.randomUUID();
        this.tmpPath = new Path(fs.getWorkingDirectory(), "/.tmp-parts/" + uuid.toString().replace("-", ""));
    }

    public Path getPath() {
        return path;
    }

    public Path getTmpPath() {
        return tmpPath;
    }

    @Override
    public ByteBuffer bytes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}

class MinioMultipartPartHandle implements PartHandle {

    private final Path partPath;
    private final int partNumber;

    public MinioMultipartPartHandle(Path partPath, int partNumber) {
        this.partPath = partPath;
        this.partNumber = partNumber;
    }

    public int getPartNumber() {
        return partNumber;
    }

    public Path getPartPath() {
        return partPath;
    }

    @Override
    public ByteBuffer bytes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}

class MinioMultiPartPathHandle implements PathHandle {

    private final Path path;

    public MinioMultiPartPathHandle(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public ByteBuffer bytes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
