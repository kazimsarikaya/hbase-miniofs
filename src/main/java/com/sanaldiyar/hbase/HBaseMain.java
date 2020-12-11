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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMain {

    public static void main(String[] args) {

        final LoggerContextFactory factory = LogManager.getFactory();

        if (factory instanceof Log4jContextFactory) {
            Log4jContextFactory contextFactory = (Log4jContextFactory) factory;

            ((DefaultShutdownCallbackRegistry) contextFactory.getShutdownCallbackRegistry()).stop();
        }
        Logger logger = LoggerFactory.getLogger(HBaseMain.class.getName());

        Configuration conf = HBaseConfiguration.create();
        conf.set(MinioFileSystem.MINIO_ROOT, "minio://minioadmin:minioadmin@127.0.0.1:9000/hbase"); // minio://<host:port>/<bucket>

        conf.setBoolean("hbase.cluster.distributed", true);
        conf.setBoolean("hbase.unsafe.stream.capability.enforce", false);
        conf.setInt("hbase.master.namespace.init.timeout", 1000 * 60 * 60);
        conf.setInt("io.file.buffer.size", MinioFileSystem.MINIO_DEFAULT_BUFFER_SIZE);

        System.setProperty(MinioFileSystem.MINIO_BUFFER_SIZE, String.valueOf(MinioFileSystem.MINIO_DEFAULT_BUFFER_SIZE));

        ShutdownHookManager.affixShutdownHook(new Thread() {
            @Override
            public void run() {
                Runtime.getRuntime().halt(0); //the fucking reaper of the fucking dangling threads.
            }

        }, -1); // -1 for the last one.

        if (args.length < 1) {
            logger.error("master/region type param should be given");
        }

        String type = args[0];

        int portIncrease = 0;
        if (args.length == 2) {
            portIncrease = Integer.valueOf(args[1]);
        }

        try {
            HRegionServer regionServer = null;
            switch (type) {
                case "master":
                    int masterServerPort = 16000;
                    int masterServerInfoPort = 16010;
                    masterServerPort += portIncrease;
                    masterServerInfoPort += portIncrease;
                    conf.setInt(HConstants.MASTER_PORT, masterServerPort);
                    conf.setInt(HConstants.MASTER_INFO_PORT, masterServerInfoPort);
                    regionServer = new HMaster(conf);
                    break;
                case "region":
                    int regionServerPort = 16020;
                    int regionServerInfoPort = 16030;
                    regionServerPort += portIncrease;
                    regionServerInfoPort += portIncrease;
                    conf.setInt(HConstants.REGIONSERVER_PORT, regionServerPort);
                    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, regionServerInfoPort);
                    regionServer = new HRegionServer(conf);
                    break;
                default:
                    logger.error("unknown type");
                    System.exit(-1);
            }

            regionServer.run();
            try {
                regionServer.join();
            } catch (InterruptedException ex) {
                logger.error("error occured at region server {} exception {}", regionServer.getId(), ex);
            }
            logger.debug("region server stopped");

        } catch (IOException ex) {
            logger.error("error occured", ex);
        }

    }

}
