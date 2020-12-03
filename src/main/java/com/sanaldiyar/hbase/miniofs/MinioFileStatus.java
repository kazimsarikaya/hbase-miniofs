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
