package com.tectonica;

import java.io.IOException;

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
		// args[0] should be the LMDB folder, e.g: "/var/imagenet/lmdb/lmdb_train"
		// args[1] should be the HDFS file, e.g "hdfs://master:9000/imagenet/lmdb_train"
		lmdbToHdfs(args[0], args[1]);
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

	private static void lmdbToHdfs(String lmdbPath, String hdfsSeqFile) throws IOException
	{
		Configuration conf = new Configuration();
		// conf.addResource(new Path("file:///usr/local/hadoop/etc/hadoop/core-site.xml"));

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
