package com.tectonica;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.fusesource.lmdbjni.*;

public class LmdbToHdfs
{
	public static void main(String args[]) throws IOException
	{
		if (args.length < 2)
		{
			System.out.println("Lmdb-To-HDFS Loader");
			System.out.println("Usage:");
			System.out.println("  java -jar lmdbToHdfs.jar <lmdb-path> <hdfs-path> [<resource-uri>..]");
			System.out.println("Where:");
			System.out.println("  <lmdb-path> is a directory containing an LMDB database, e.g. /var/imagenet/lmdb");
			System.out.println(
					"  <hdfs-path> is a path for (new) sequence file on HDFS, e.g. hdfs://master:9000/imagenet/lmdb");
			System.out.println(
					"  <resource-uri> (0 or more values) are URIs of Hadoop configuration file, e.g. file:///usr/local/hadoop/etc/hadoop/core-site.xml");
			System.exit(1);
		}

		lmdbToHdfs(args[0], args[1], Arrays.copyOfRange(args, 2, args.length));
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////

	private static SequenceFile.Writer seqWriter = null;
	private static BytesWritable seqKey = new BytesWritable();
	private static BytesWritable seqValue = new BytesWritable();

	private static void write(byte[] key, byte[] value) throws IOException
	{
		seqKey.set(key, 0, key.length);
		seqValue.set(value, 0, value.length);
		seqWriter.append(seqKey, seqValue);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////

	private static void lmdbToHdfs(String lmdbPath, String hdfsSeqFile, String[] resourceUris) throws IOException
	{
		Configuration conf = new Configuration();

		for (String resourceUri : resourceUris)
			conf.addResource(new Path(resourceUri));

		try
		{
			seqWriter = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(hdfsSeqFile)),
					SequenceFile.Writer.keyClass(seqKey.getClass()),
					SequenceFile.Writer.valueClass(seqValue.getClass()));

			copyFromLmdb(lmdbPath);
		}
		finally
		{
			IOUtils.closeStream(seqWriter);
		}
	}

	private static void copyFromLmdb(String lmdbPath) throws IOException
	{
		try (Env env = new Env(lmdbPath))
		{
			try (Database db = env.openDatabase())
			{
				Transaction tx = env.createReadTransaction();
				try (EntryIterator it = db.iterate(tx))
				{
					int count = 0;
					for (Entry entry : it.iterable())
					{
						write(entry.getKey(), entry.getValue());
						if (++count % 250 == 0)
						{
							System.out.println("Saved " + count + " records");
						}
					}
					System.out.println("Completed. Saved " + count + " records in total");
				}
			}
		}
	}
}
