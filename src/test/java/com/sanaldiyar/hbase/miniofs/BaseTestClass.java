/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sanaldiyar.hbase.miniofs;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kazim
 */
@RunWith(JUnitPlatform.class)
public class BaseTestClass {

    private final static Logger logger = LoggerFactory.getLogger(BaseTestClass.class.getName());

    private final static MinioUtil minioUtil = MinioUtil.getInstance();
    private static Configuration conf;

    private static FileSystem fileSystem;
    private static Path rootPath;

    @BeforeAll
    public static void setUpClass() {
        try {
            conf = new Configuration(true);
            conf.set(MinioFileSystem.MINIO_ROOT, "minio://minioadmin:minioadmin@localhost:9000/test");
            conf.set(MinioFileSystem.MINIO_STREAM_BUFFER_SIZE, String.valueOf(128 << 10));
            conf.set(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, String.valueOf(8 << 20));

            minioUtil.setConf(conf);

            rootPath = new Path(conf.get(MinioFileSystem.MINIO_ROOT));
            fileSystem = rootPath.getFileSystem(conf);
            fileSystem.setWorkingDirectory(rootPath);
        } catch (IOException ex) {
            logger.error("error occured", ex);
            assert false;
        }
    }

    @BeforeEach
    public void cleanup() throws Exception {
        MinioClient client = MinioClient.builder().
                endpoint("http://localhost:9000")
                .credentials("minioadmin", "minioadmin")
                .build();

        Iterable<Result<Item>> objects = client.listObjects(ListObjectsArgs.builder()
                .recursive(true)
                .bucket("test")
                .build());
        for (var object : objects) {
            Item item = object.get();
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket("test")
                    .object(item.objectName())
                    .build());
        }
    }

    public static MinioUtil getMinioUtil() {
        return minioUtil;
    }

    public static FileSystem getFileSystem() {
        return fileSystem;
    }

    public static Path getRootPath() {
        return rootPath;
    }

    public static Configuration getConf() {
        return conf;
    }

}
