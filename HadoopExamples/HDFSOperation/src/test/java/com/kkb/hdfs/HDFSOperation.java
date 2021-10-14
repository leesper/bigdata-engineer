package com.kkb.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSOperation {
    // examples of creating directory
    @Test
    public void mkDirOnHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        boolean isOK = fileSystem.mkdirs(new Path("/kaikeba/dir1"));
        if (isOK) {
            System.out.println("目录创建成功");
        }
        fileSystem.close();
    }

    @Test
    public void mkDirOnHDFS2() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "test");
        boolean isOK = fileSystem.mkdirs(new Path("/kaikeba/dir2"));
        if (isOK) {
            System.out.println("目录创建成功");
        }
        fileSystem.close();
    }

    @Test
    public void mkDirOnHDFS3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        boolean isOK = fileSystem.mkdirs(new Path("/kaikeba/dir3"),
                new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ));
        if (isOK) {
            System.out.println("目录创建成功");
        }
        fileSystem.close();
    }

    // examples of file uploading/downloading
    @Test
    public void uploadFile2HDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path("/Users/likejun/bigdata-engineer/data/hello.txt"),
                new Path("/kaikeba/dir1"));
        fileSystem.close();
    }

    @Test
    public void downloadFileFromHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(new Path("/kaikeba/dir1/hello.txt"),
                new Path("/Users/likejun/bigdata-engineer/data/helloDownload.txt"));
        fileSystem.close();
    }

    // examples of file deletion and renaming
    @Test
    public void renamingFileOnHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        boolean isOK = fileSystem.rename(new Path("/kaikeba/dir1/hello.txt"),
                new Path("/kaikeba/dir1/rename.txt"));
        if (isOK) {
            System.out.println("文件重命名成功");
        }

        fileSystem.close();
    }

    @Test
    public void deleteFileOnHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        boolean isOK = fileSystem.delete(new Path("/kaikeba/dir1/rename.txt"), true);
        if (isOK) {
            System.out.println("文件删除成功");
        }
        fileSystem.close();
    }

    // example of checking file on HDFS
    @Test
    public void viewFileInfoOnHDFS() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);

        RemoteIterator<LocatedFileStatus> fileStatuses = fileSystem.listFiles(new Path("/kaikeba"), true);

        while (fileStatuses.hasNext()) {
            LocatedFileStatus status = fileStatuses.next();

            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());
            System.out.println(status.getOwner());
            if (status.isFile()) {
                System.out.println("file");
            } else if (status.isDirectory()) {
                System.out.println("directory");
            }

            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation location : blockLocations) {
                String[] hosts = location.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        fileSystem.close();
    }

    // examples of file operations by IO stream
    @Test
    public void streamFileToHDFS() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        FileInputStream fis = new FileInputStream(new File("/Users/likejun/bigdata-engineer/data/hello.txt"));
        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://node01:8020/kaikeba/dir3/hello.txt"));
        IOUtils.copy(fis, fos);
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
        fileSystem.close();
    }

    @Test
    public void streamFileFromHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fis = fileSystem.open(new Path("hdfs://node01:8020/kaikeba/dir3/hello.txt"));
        FileOutputStream fos = new FileOutputStream(new File("/Users/likejun/bigdata-engineer/data/stream.txt"));
        IOUtils.copy(fis, fos);
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
        fileSystem.close();
    }

    // examples of merge small files into a big one
    @Test
    public void mergeSmallFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "hadoop");

        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://node01:8020/kaikeba/bigfile.txt"));

        LocalFileSystem localFileSystem = fileSystem.getLocal(configuration);
        RemoteIterator<LocatedFileStatus> iterator = localFileSystem.listFiles(
                new Path("/Users/likejun/bigdata-engineer/data/smallfile"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            FSDataInputStream fis = localFileSystem.open(status.getPath());
            IOUtils.copy(fis, fos);
            IOUtils.closeQuietly(fis);
        }
        IOUtils.closeQuietly(fos);
        localFileSystem.close();
        fileSystem.close();
    }
}
