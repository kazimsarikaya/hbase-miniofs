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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kazim
 */
public class MinioUtilTest {

    private final static MinioUtil minioUtil = MinioUtil.getInstance();
    private static Configuration conf;
    private final static Logger logger = LoggerFactory.getLogger(MinioUtilTest.class.getName());

    public MinioUtilTest() {
    }

    @BeforeAll
    public static void setUpClass() {

        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "{} [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.DEBUG);
        console.activateOptions();
        //add appender to any Logger (here is root)
        org.apache.log4j.Logger.getRootLogger().addAppender(console);

        conf = new Configuration();
        conf.set(MinioFileSystem.MINIO_ENDPOINT, "http://localhost:9000");
        conf.set(MinioFileSystem.MINIO_ROOT, "minio://test");
        conf.set(MinioFileSystem.MINIO_ACCESS_KEY, "minioadmin");
        conf.set(MinioFileSystem.MINIO_SECRET_KEY, "minioadmin");
        conf.set(MinioFileSystem.MINIO_STREAM_BUFFER_SIZE, String.valueOf(128 << 10));
        conf.set(MinioFileSystem.MINIO_UPLOAD_PART_SIZE, String.valueOf(8 << 20));

        minioUtil.setConf(conf);
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testComplete() {
        try {
            assert minioUtil.mkdirs(new Path("test")) == true;
            assert minioUtil.mkdirs(new Path("test/subdir")) == true;
            assert minioUtil.mkdirs(new Path("test/subdir/subsubdir")) == true;
            assert minioUtil.mkdirs(new Path("test2/subdir/subsub")) == true;
            FileStatus[] listStatus = minioUtil.listStatus(new Path("/test2"));
            for (FileStatus fs : listStatus) {
                logger.info(fs.toString());
            }
            assert minioUtil.delete(new Path("/test2"), true) == true;
            byte[] data = new byte[]{65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75};
            minioUtil.putStream(new Path("/test/file1"), new ByteArrayInputStream(data), data.length);
            minioUtil.putStream(new Path("/test/subdir/file2"), new ByteArrayInputStream(data), data.length);
            minioUtil.putStream(new Path("/test/subdir/subsubdir/file3"), new ByteArrayInputStream(data), data.length);
            minioUtil.putStream(new Path("/test/subdir/subsubdir/file4"), new ByteArrayInputStream(data), data.length);
            assert minioUtil.rename(new Path("/test"), new Path("/test3")) == true;

            minioUtil.listStatus(new Path("/"));

            Path rw = new Path("minio:///test3/subdir/subsubdir/file5");

            HashFunction sha256 = Hashing.sha256();
            SecureRandom random = new SecureRandom();
            int partsize = 8 << 20;
            byte[] tmpData = new byte[4 << 20];
            MinioOutputStream mos = new MinioOutputStream(rw, conf, partsize);
            Hasher hasher = sha256.newHasher();
            for (int i = 0; i < 16; i++) {
                random.nextBytes(tmpData);
                hasher.putBytes(tmpData);
                mos.write(tmpData);
            }
            mos.close();
            String hashSended = hasher.hash().toString();

            MinioInputStream mis = new MinioInputStream(rw, conf, tmpData.length >> 2);
            hasher = sha256.newHasher();
            long pos = 0;
            while (true) {
                int r = mis.read(tmpData);
                if (r <= 0) {
                    logger.debug("readed {} pos {}", r, pos);
                    break;
                }
                logger.debug("readed {} pos {}", r, pos);
                pos += r;
                hasher.putBytes(tmpData, 0, r);
            }
            mis.close();
            String hashReaded = hasher.hash().toString();

            logger.info("sended hash: {}", hashSended);
            logger.info("recved hash {}", hashReaded);

            assert hashSended.equals(hashReaded);

            mis = new MinioInputStream(new Path("/test3/subdir/subsubdir/file4"), conf, tmpData.length >> 2);
            int r = mis.read(tmpData);
            assert r == data.length;
            assert new String(tmpData, 0, r).equals("ABCDEFGHIJK");

            logger.debug("getName: {} len {}", rw.getName(), rw.getName().length());

            FileSystem fs = rw.getFileSystem(conf);
            FileStatus f_s = fs.getFileStatus(rw);
            assert f_s.getPath().getName().equals("file5");

            FileStatus[] f_ses = fs.listStatus(rw.getParent(), new PathFilter() {

                private final ArrayList<String> filters = new ArrayList<>();

                @Override
                public boolean accept(Path path) {
                    filters.add("file5");
                    return filters.contains(path.getName());
                }
            });

            assert f_ses.length == 1;

            FSUtils.UserTableDirFilter filter = new FSUtils.UserTableDirFilter(fs);
            assert !filter.accept(new MinioFileStatus(new Path("minio://test/.tmp"), true, 0));

        } catch (IOException ex) {
            logger.error(null, ex);
            assert false;
        }
    }

}
