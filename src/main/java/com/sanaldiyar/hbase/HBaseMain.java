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
package com.sanaldiyar.hbase;

import com.sanaldiyar.hbase.miniofs.MinioFileSystem;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kazim
 */
public class HBaseMain {

    private final static Logger logger = LoggerFactory.getLogger(HBaseMain.class.getName());

    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        conf.set(MinioFileSystem.MINIO_ENDPOINT, "http://127.0.0.1:9000");
        conf.set(MinioFileSystem.MINIO_ROOT, "minio://hbase/");

        conf.set(MinioFileSystem.MINIO_ACCESS_KEY, "minioadmin");
        conf.set(MinioFileSystem.MINIO_SECRET_KEY, "minioadmin");

        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.unsafe.stream.capability.enforce", "false");

        if (args.length != 1) {
            logger.error("master/region type param should be given");
        }

        String type = args[0];

        try {
            HRegionServer regionServer = null;
            if (type.equals("master")) {
                regionServer = new HMaster(conf);
            } else if (type.equals("region")) {
                regionServer = new HRegionServer(conf);
            } else {
                logger.error("unknown type");
                System.exit(-1);
            }
            regionServer.start();
        } catch (IOException e) {
        }

    }

}
