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

import java.util.Date;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 *
 * @author kazim
 */
public class MinioFileStatus extends FileStatus {

    private static final long serialVersionUID = 689346074963917036L;
    private final boolean isDir;
    private final long ma_time;
    private final Path path;
    private final long length;

    public MinioFileStatus(Path path, boolean isDirectory, long length) {
        this.path = path;
        this.length = length;
        ma_time = new Date().getTime();
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

    @Override
    public long getModificationTime() {
        return ma_time;
    }

    @Override
    public long getAccessTime() {
        return ma_time;
    }

    @Override
    public String getOwner() {
        return "miniofs";
    }

    @Override
    public String getGroup() {
        return "miniofs";
    }

    @Override
    public FsPermission getPermission() {
        if (isDir) {
            return new FsPermission("0755");
        }
        return new FsPermission("0644");
    }

    @Override
    public long getBlockSize() {
        return MinioUtil.getInstance().getDefaultBlockSize();
    }

    @Override
    public long getLen() {
        return length;
    }

    @Override
    public Path getPath() {
        return path;
    }

}
