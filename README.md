# Hadoop-Perfect-File : A Fast access container for small files

Hadoop Perfect File (HPF) like others hadoop index based archive files also consists
of combining small files into large files before storing
on HDFS. HPF organizes its index system efficiently and
provide a very fast access performance. Unlike HAR file, MapFile ... 
where it is not possible to access the metadata
of a particular file directly from the index file, HPF
offers direct access to file’s metadata. By using a monotone minimal perfect hash function in its index
system, HPF can calculate from which offset to which
limit of the index file to read the metadata of a file.
After the offset and limit calculation, HPF seek in the
index file at the offset position and read to limit. Seek in
a random way to some positions in a file is an operation
that can take time when the file is very large. In order
to avoid the degradation of seek operations, we avoid
getting too big index file by distributing the metadata
of our files into several index files using an extendible
hash function. Our approach allows appending more
files after the creation of HPF file without having to
decompress and recompress the entire archive file.



![hpf_creation](https://user-images.githubusercontent.com/18069576/51819858-eaf5c380-230e-11e9-85a3-74696db11d43.png)



Creating and adding files to the HPF file is done using the PerfectFile.Writer class and reading the HPF file is done using the PerfectFile.Reader class.

## Example of writing to HPF file

#### From Local
In this example, we will add all the files located in the folder D:/files/ of the client in the HPF file whose path on HDFS is /data/file.hpf. 
If the file /data/file.hpf does not exist on HDFS, it will be automatically created.


    FileSystem lfs = LocalFileSystem.getLocal(conf);
		
    try (PerfectFile.Writer writer = new PerfectFile.Writer(conf, new Path("/data/file.hpf"), 200000)) {
        for (FileStatus status : lfs.listStatus(new Path("D:/files/"))) {
            String key = status.getPath().getName();
            Path path = status.getPath();
            writer.putFromLocal(key,path);
        }
    }

The last parameter(200000) of the class PerfectFile.Writer's constructor represents the maximun number of files metadata that each index file can hold.

#### From HDFS
In this example, we will add all the files located in the folder /data/files/ of HDFS in the HPF file whose path on HDFS is /data/file.hpf. 

    FileSystem fs = FileSystem.getLocal(conf);
		
    try (PerfectFile.Writer writer = new PerfectFile.Writer(conf, new Path("/data/file.hpf"), 200000)) {
        for (FileStatus status : fs.listStatus(new Path("/data/files/"))) {
            String key = status.getPath().getName();
            Path path = status.getPath();
            writer.put(key,path);
        }
    }

## Example of reading from HPF file
To read a file from the HPF file, we have two functions: the get() function that returns an input steam and the getBytes() function that return the binary contents of the file.

    try (PerfectFile.Reader reader = new PerfectFile.Reader(conf, new Path("/data/file.hpf"))) {
        for (String key : keys) {
            InputStream inputStream = reader.get(key);
            byte[] bs = reader.getBytes(key);
        }
     }


