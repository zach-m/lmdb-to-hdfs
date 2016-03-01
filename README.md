# lmdb-to-hdfs
Small utility to load LMDB datastore into HDFS as a sequence file.

Pre-configured to run on Linux 64-bit. To run on other architectures (Windows, OSX, Android), change the Maven dependency in [this line within pom.xml](https://github.com/zach-m/lmdb-to-hdfs/blob/master/pom.xml#L20). Possible values are:
* `lmdbjni-linux64`
* `lmdbjni-win64`
* `lmdbjni-osx64`
* `lmdbjni-android`

See the [lmdbjni](https://libraries.io/github/deephacks/lmdbjni) project home page for more information.

The Maven page for this library is [here](http://mvnrepository.com/artifact/org.deephacks.lmdbjni).

### Usage
```bash
java -jar lmdbToHdfs.jar <lmdb-path> <hdfs-path> [<resource-uri>..]
```
* `lmdb-path` is a directory containing an LMDB database, _e.g. /var/imagenet/lmdb_
* `hdfs-path` is a path for (new) sequence file on HDFS, _e.g. hdfs://master:9000/imagenet/lmdb_
* `resource-uri` (0 or more values) are URIs of Hadoop configuration file, _e.g. file:///usr/local/hadoop/etc/hadoop/core-site.xml_

> NOTE: When not specifying any Hadoop configuration files, Hadoop's default [Configuration](https://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/conf/Configuration.html) will be used
