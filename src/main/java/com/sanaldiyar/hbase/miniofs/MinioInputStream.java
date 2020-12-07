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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioInputStream extends FSInputStream {

    private final static Logger logger = LoggerFactory.getLogger(MinioInputStream.class.getName());

    private final MinioUtil minioUtil = MinioUtil.getInstance();

    private long position = 0;
    private long bufferStart = 0;
    private long filesize = 0;
    private int bufferPosition = 0;
    private int bufferLength = 0;
    private final byte[] buffer;
    private final Path path;
    private final Configuration conf;
    private final String key;

    public MinioInputStream(Path path, Configuration conf, long bufferSize) throws IOException {
        this.key = minioUtil.getPrefix(path);
        List<String> locks = MinioFileSystem.getLocks();
        synchronized (locks) {
            if (!locks.contains(key)) {
                locks.add(key);
                logger.debug("LOCK lock added for path {}", path);
            }
        }
        this.path = path;
        this.conf = conf;
        this.buffer = new byte[(int) bufferSize];
        FileStatus fs = minioUtil.getFileStatus(path);
        filesize = fs.getLen();
        fillBuffer(0);
    }

    @Override
    public void close() throws IOException {
        super.close();
        List<String> locks = MinioFileSystem.getLocks();
        synchronized (locks) {
            locks.remove(key);
            logger.debug("LOCK lock removed for path {}", path);
        }
    }

    private void fillBuffer(long start) throws IOException {
        if (start == filesize) {
            logger.debug("buffer will not be filled for path {} from {} len {} EOF", path.toUri().getPath(), start, buffer.length);
            bufferLength = 0;
        } else {
            logger.debug("buffer will be filled for path {} from {} len {}", path.toUri().getPath(), start, buffer.length);
            bufferLength = minioUtil.fillData(path, start, buffer);
            logger.debug("buffer  filled for path {} from {} len {}", path.toUri().getPath(), start, bufferLength);
        }
        bufferStart = start;
        position = start;
        bufferPosition = 0;
        logger.debug("after fill buffer new position {}", position);
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        if (bufferStart <= pos && pos <= bufferStart + buffer.length) {
            position = pos;
            long tmp_off = pos - bufferStart;
            bufferPosition = (int) tmp_off;
        } else {
            fillBuffer(pos);
        }
        logger.debug("input stream position changed to {}", position);
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public synchronized int read() throws IOException {
        if (position == filesize) {
            return -1;
        }
        if (bufferPosition == buffer.length) {
            fillBuffer(position);
        }
        if (bufferLength == 0) {
            return -1;
        }
        int data = buffer[bufferPosition];
        position++;
        bufferPosition++;
        return data;
    }

    @Override
    public synchronized int read(byte[] data, int off, int len) throws IOException {
        if (data == null) {
            throw new NullPointerException("destination array is empty");
        }
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException("parameters are incorrect");
        }
        int readed = 0;
        if (len == 0) {
            return 0;
        }

        int off_backup = off;
        int len_backup = len;
        long pos_backup = position;

        while (len > 0) {
            int avail = bufferLength - bufferPosition;
            if (avail == 0) {
                logger.debug("need buffer filling");
                fillBuffer(position);
                avail = bufferLength - bufferPosition;
                logger.debug("buffer filled");
            }

            int maxRead = len;
            if (maxRead > avail) {
                maxRead = avail;
            }

            if (maxRead > bufferLength) {
                maxRead = bufferLength;
            }

            if (maxRead == 0) {
                break;
            }

            System.arraycopy(buffer, bufferPosition, data, off, maxRead);
            logger.debug("buffer copied from {} to {} with len {}", bufferPosition, off, maxRead);
            len -= maxRead;
            off += maxRead;
            bufferPosition += maxRead;
            readed += maxRead;
            position += maxRead;
        }
        logger.debug("data readed from file {} to offset {} with len {} requested len {} new position {} old_position {}", path.toUri().getPath(), off_backup, readed, len_backup, position, pos_backup);
        if (readed == 0) {
            return -1;
        }
        return readed;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
