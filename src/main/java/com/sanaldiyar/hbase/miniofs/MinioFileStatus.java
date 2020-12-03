/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sanaldiyar.hbase.miniofs;

import java.util.Date;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author kazim
 */
public class MinioFileStatus extends FileStatus {

    private final boolean isDir;

    public MinioFileStatus(Path path, boolean isDirectory, long length) {
        super(length, isDirectory, 0, 0, new Date().getTime(), new Date().getTime(), null, "miniofs", "miniofs", path);
        isDir = isDirectory;
    }

    @Override
    public boolean isDirectory() {
        return isDir;
    }

    @Override
    public boolean isFile() {
        return !isDirectory();
    }

}
