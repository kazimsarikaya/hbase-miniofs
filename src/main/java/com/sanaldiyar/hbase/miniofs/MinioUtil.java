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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.minio.ComposeObjectArgs;
import io.minio.ComposeSource;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioUtil implements Configurable {

    public final static String MINIO_METADATA_BLOCK_SIZE = "hbase.fs.file.blocksize";

    private String endpoint;
    private String accessKey;
    private String secretKey;
    private URI uri;
    private MinioClient client;
    private String bucket;
    private Configuration conf;

    private final static Logger logger = LoggerFactory.getLogger(MinioUtil.class.getName());

    private final static MinioUtil instance = new MinioUtil();

    @Override
    public void setConf(Configuration conf) {
        try {
            this.conf = conf;
            this.endpoint = conf.get(MinioFileSystem.MINIO_ENDPOINT);
            this.accessKey = conf.get(MinioFileSystem.MINIO_ACCESS_KEY);
            this.secretKey = conf.get(MinioFileSystem.MINIO_SECRET_KEY);
            this.uri = new URI(conf.get(MinioFileSystem.MINIO_ROOT));
            this.bucket = this.uri.getHost();
            this.client = MinioClient.builder().
                    endpoint(this.getEndpoint())
                    .credentials(this.accessKey, this.secretKey)
                    .build();
        } catch (URISyntaxException ex) {
            logger.error("hbase.rootdir malformed", ex);
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static MinioUtil getInstance() {
        return MinioUtil.instance;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public URI getUri() {
        return uri;
    }

    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        logger.debug("listing status of {} {}", path.toUri().getPath(), path.toString());
        return listStatus(path, false);
    }

    public FileStatus[] listStatus(Path path, boolean recursive) throws FileNotFoundException, IOException {
        FileStatus base_fs;
        try {
            base_fs = getFileStatus(path);
        } catch (FileNotFoundException ex) {
            logger.debug("path {} not exists", path);
            throw ex;
        } catch (IOException ex) {
            throw ex;
        }

        if (base_fs.isFile()) {
            return new FileStatus[]{base_fs};
        }

        List<FileStatus> statuses = new LinkedList<>();

        try {
            String prefix = convertDirKey(path);

            Iterable<Result<Item>> results;

            if (prefix.equals("/")) {
                results = client.listObjects(ListObjectsArgs.builder()
                        .bucket(bucket)
                        .useUrlEncodingType(true)
                        .recursive(recursive)
                        .build());
            } else {
                results = client.listObjects(ListObjectsArgs.builder()
                        .bucket(bucket)
                        .useUrlEncodingType(true)
                        .recursive(recursive)
                        .prefix(prefix)
                        .build());
            }

            for (Result<Item> result : results) {
                Item item = result.get();
                String strItemPath = this.uri + "/" + item.objectName();
                Path itemPath = new Path(strItemPath);
                if (path.toUri().equals(itemPath.toUri())) {
                    continue;
                }
                boolean isDir = false;
                if (strItemPath.endsWith("/")) {
                    isDir = true;
                }
                FileStatus fs = new MinioFileStatus(itemPath, isDir, item.size());
                logger.debug("path found {} isDir {} size {}", fs.getPath(), fs.isDirectory(), fs.getLen());
                statuses.add(fs);
            }

        } catch (IllegalArgumentException | ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | JsonMappingException | JsonParseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.error("cannot list statuses", ex);
            throw new IOException("cannot list statuses", ex);
        }

        FileStatus[] fses = new FileStatus[statuses.size()];
        statuses.toArray(fses);
        logger.debug("listing returned {} paths", fses.length);
        return fses;
    }

    public FileStatus getFileStatus(Path path) throws IOException {
        String basePath = path.toUri().getPath();
        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
        }
        logger.debug("get status of path {}", basePath);

        if (basePath.isEmpty()) { //root
            return new MinioFileStatus(path, true, 0);
        }

        StatObjectResponse stat = null;
        boolean isDir = false;
        try {
            stat = client.statObject(StatObjectArgs.builder()
                    .bucket(bucket)
                    .object(basePath)
                    .build());
        } catch (InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.debug(null, ex);
            throw new IOException(String.format("cannot stat path: {}", path.toString()));
        } catch (ErrorResponseException ex) {
            if (ex.response().code() == 404) {
                try {
                    logger.debug("probably not file, try as folder: {}", basePath);
                    stat = client.statObject(StatObjectArgs.builder()
                            .bucket(bucket)
                            .object(basePath + "/")
                            .build());
                    isDir = true;
                } catch (InvalidKeyException | InsufficientDataException | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex1) {
                    logger.debug(null, ex1);
                } catch (ErrorResponseException ex1) {
                    if (ex1.response().code() == 404) {
                        throw new FileNotFoundException(basePath);
                    } else {
                        throw new IOException(String.format("cannot stat path: {}", path.toString()));
                    }
                }
            } else {
                throw new IOException(String.format("cannot stat path: {}", path.toString()), ex);
            }
        }
        if (stat == null) {
            throw new IOException(String.format("cannot stat path: {}", path.toString()));
        } else {
            MinioFileStatus fs = new MinioFileStatus(path, isDir, stat.size());
            return fs;
        }
    }

    public synchronized boolean mkdirs(Path path) throws IOException {
        Path orig_path = path;
        logger.debug("the dir will be created {}", orig_path.toUri().getPath());

        Stack<Path> dirs = new Stack<>();

        while (true) {
            dirs.push(path);
            path = path.getParent();
            if (path == null || path.isRoot()) {
                break;
            }
        }

        while (!dirs.empty()) {
            Path tmpPath = dirs.pop();
            String key = convertDirKey(tmpPath);
            if (key.equals("/")) {
                continue;
            }
            try {
                FileStatus fs = getFileStatus(tmpPath);
                if (!fs.isDirectory()) {
                    throw new IOException(String.format("parent is not a folder: {}", orig_path.toUri().getPath()));
                }
            } catch (FileNotFoundException nfexp) {
                try {
                    client.putObject(PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(key)
                            .stream(new ByteArrayInputStream(new byte[]{}), 0, -1)
                            .build());
                } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidResponseException | ServerException | XmlParserException | IOException | IllegalArgumentException | InvalidKeyException | NoSuchAlgorithmException ex) {
                    logger.debug(null, ex);
                    throw new IOException("cannot create directory", ex);
                }
            } catch (IOException ex) {
                throw ex;
            }

        }
        logger.debug("the dir created {}", orig_path.toUri().getPath());

        return true;
    }

    public boolean danglingExists(Path path) throws IOException {
        FileStatus fileStatus = null;
        try {
            fileStatus = getFileStatus(path);
        } catch (FileNotFoundException e) {
        }
        if (fileStatus == null) {
            String prefix = path.toUri().getPath();
            if (prefix.startsWith("/")) {
                prefix = prefix.substring(1);
            }
            prefix += "/";
            Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder()
                    .bucket(bucket)
                    .useUrlEncodingType(true)
                    .recursive(true)
                    .prefix(prefix)
                    .build());
            int itemCnt = 0;
            for (Result<Item> result : results) {
                itemCnt++;
            }
            return itemCnt != 0;
        }
        return false;
    }

    private boolean cleanupDanglings(Path path) throws IOException {
        logger.debug("start cleanup danglings of {} completed", path);
        String prefix = path.toUri().getPath();
        if (prefix.startsWith("/")) {
            prefix = prefix.substring(1);
        }
        prefix += "/";
        Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder()
                .bucket(bucket)
                .useUrlEncodingType(true)
                .recursive(true)
                .prefix(prefix)
                .build());

        for (Result<Item> result : results) {
            try {
                Item item = result.get();
                client.removeObject(RemoveObjectArgs.builder()
                        .bucket(bucket)
                        .object(item.objectName())
                        .build());
            } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidResponseException | ServerException | XmlParserException | IOException | IllegalArgumentException | InvalidKeyException | NoSuchAlgorithmException exp) {
                logger.error("unknown error occured: {}", exp);
                throw new IOException(String.format("unknown error occured %s", exp));
            }
        }
        logger.debug("cleanup danglings of {} completed", path);
        return true;
    }

    public synchronized boolean delete(Path path, boolean recursive) throws IOException {
        logger.debug("try to delete path {}", path.toString());
        FileStatus fs;
        try {
            fs = getFileStatus(path);
        } catch (FileNotFoundException ex) {
            return true;
        }

        if (fs.isDirectory()) {
            FileStatus[] child_fses = new FileStatus[]{};
            try {
                child_fses = listStatus(path, false);
            } catch (FileNotFoundException ex) {

            }

            if (!recursive && child_fses.length != 0) {
                throw new IOException(String.format("folder is not empty %s", path));
            }
            if (child_fses.length == 0 && danglingExists(path)) {
                logger.debug("danglings found at {}", path);
                return cleanupDanglings(path);
            }
            for (FileStatus child_fs : child_fses) {
                if (child_fs.compareTo(fs) == 0) {
                    continue;
                }
                delete(child_fs.getPath(), true);
            }
        }
        deleteItem(path, fs.isDirectory());

        logger.debug("delete path suceeded {}", path.toUri().getPath());
        return true;
    }

    private void deleteItem(Path path, boolean isDir) throws IOException {
        try {
            String itemPath = path.toUri().getPath();
            if (itemPath.startsWith("/")) {
                itemPath = itemPath.substring(1);
            }
            if (isDir) {
                itemPath += "/";
            }
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucket)
                    .object(itemPath)
                    .build()
            );
            logger.debug("path {} deleted", path.toUri().getPath());
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.error("cannot delete item", ex);
        }
    }

    public void putStream(Path path, InputStream is, long len) throws IOException {
        try {
            logger.debug("try to upload object to path {} with len {}", path.toString(), len);
            mkdirs(path.getParent());
            String key = path.toUri().getPath();
            if (key.startsWith("/")) {
                key = key.substring(1);
            }
            logger.debug("dst key will be: {}", key);
            ObjectWriteResponse resp = client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(is, len, getDefaultPartSize())
                    .build());

            logger.debug("upload object to path {} with len {} suceeded. etag: {}", path.toString(), len, resp.etag());
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.error("cannot put inputstream as object", ex);
            throw new IOException("cannot put inputstream as object", ex);
        }
    }

    private String convertDirKey(Path path) {
        String result = path.toUri().getPath();
        if (result.startsWith("/")) {
            result = result.substring(1);
        }
        if (!result.endsWith("/")) {
            result += "/";
        }
        return result;
    }

    public int getDefaultBlockSize() {
        return getConf().getInt(MinioFileSystem.MINIO_STREAM_BUFFER_SIZE, MinioFileSystem.MINIO_DEFAULT_BUFFER_SIZE);
    }

    public int getDefaultPartSize() {
        return getConf().getInt(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, MinioFileSystem.MINIO_DEFAULT_PART_SIZE);
    }

    public void mergeAndClean(Path dst, Path[] items, Path tmpDir) throws IOException {
        List<ComposeSource> sources = new LinkedList<>();

        for (Path item : items) {
            String strPath = item.toUri().getPath();
            if (strPath.startsWith("/")) {
                strPath = strPath.substring(1);
            }
            sources.add(ComposeSource.builder().bucket(bucket).object(strPath).build());
            logger.debug("source added: {}", strPath);
        }

        try {
            String strDst = dst.toUri().getPath();
            if (strDst.startsWith("/")) {
                strDst = strDst.substring(1);
            }
            if (strDst.contains("%2C")) {
                logger.debug("inccorrect url character {}", dst);
            }
            client.composeObject(
                    ComposeObjectArgs.builder()
                            .bucket(bucket)
                            .object(strDst)
                            .sources(sources)
                            .build());
            logger.debug("object merged to the path {}", strDst);
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.error("cannot merge items", ex);
            throw new IOException("cannot merge items", ex);
        }

        delete(tmpDir, true);
    }

    public boolean rename(Path src, Path dst) throws IOException {

        FileStatus src_fs = getFileStatus(src);

        copyItem(src, dst);
        if (src_fs.isDirectory()) {
            String strSrc = src.toUri().getPath();
            if (strSrc.startsWith("/")) {
                strSrc = strSrc.substring(1);
            }
            String strDst = dst.toUri().getPath();
            if (strDst.startsWith("/")) {
                strDst = strDst.substring(1);
            }

            FileStatus[] srcFSes = listStatus(src, true);
            for (FileStatus fs : srcFSes) {
                String tmp = fs.getPath().toUri().getPath();
                if (tmp.startsWith("/")) {
                    tmp = tmp.substring(1);
                }
                tmp = tmp.replaceFirst("^" + strSrc, strDst);
                copyItem(fs.getPath(), new Path(tmp));
            }
        }

        delete(src, true);
        return true;
    }

    private boolean copyItem(Path src, Path dst) throws IOException {
        if (!dst.isRoot()) {
            mkdirs(dst.getParent());
        }

        String strSrc = src.toUri().getPath();
        if (strSrc.startsWith("/")) {
            strSrc = strSrc.substring(1);
        }
        FileStatus src_fs = getFileStatus(src);
        String strDst = dst.toUri().getPath();
        if (strDst.startsWith("/")) {
            strDst = strDst.substring(1);
        }
        if (src_fs.isDirectory()) {
            strSrc += "/";
            strDst += "/";
        }

        try {
            client.copyObject(
                    CopyObjectArgs.builder()
                            .bucket(bucket)
                            .object(strDst)
                            .source(CopySource.builder().bucket(bucket).object(strSrc).build())
                            .build());
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.error("cannot copy object", ex);
            throw new IOException("cannot copy object", ex);
        }

        return true;
    }

    int fillData(Path path, long start, byte[] buffer) throws IOException {
        try {
            String key = path.toUri().getPath();
            if (key.startsWith("/")) {
                key = key.substring(1);
            }
            InputStream is = client.getObject(GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .offset(start)
                    .length((long) buffer.length)
                    .build());

            int readed = 0;
            int offset = 0;
            while (true) {
                int r = is.read(buffer, offset, buffer.length - offset);
                if (r < 0) {
                    break;
                }
                offset += r;
                readed += r;
            }
            is.close();
            return readed;
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException ex) {
            logger.error("cannot fill buffer data from s3", ex);
            throw new IOException("cannot fill buffer data from s3", ex);
        }
    }

}
