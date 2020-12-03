/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sanaldiyar.hbase.miniofs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kazim
 */
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

    public MinioInputStream(Path path, Configuration conf, long bufferSize) throws IOException {
        this.path = path;
        this.conf = conf;
        this.buffer = new byte[(int) bufferSize];
        FileStatus fs = minioUtil.getFileStatus(path);
        filesize = fs.getLen();
        fillBuffer(0);
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
    }

    @Override
    public void seek(long pos) throws IOException {
        if (bufferStart <= pos && pos <= bufferStart + buffer.length) {
            position = pos;
        } else {
            fillBuffer(pos);
        }
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
        logger.debug("data readed from file {} postition {} to offset {} with len {} requested len {}", path.toUri().getPath(), position, off_backup, readed, len_backup);
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
