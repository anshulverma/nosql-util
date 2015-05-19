package com.nuaavee.nosql.task;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Exporter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(Locator.class);
  private static final String FAMILY_NAME = Parameters.getName("family_name");

  public static class EntityExportMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
      String family = context.getConfiguration().get(FAMILY_NAME);
      NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Bytes.toBytes(family));
      StringBuilder details = new StringBuilder();
      for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
        if (details.length() > 0) {
          details.append(',');
        }
        details
          .append(tokenize(entry.getKey()))
          .append(':')
          .append(tokenize(entry.getValue()));
      }
      context.write(new Text(""),
        new Text(Bytes.toBytes('{' + details.toString() + '}')));
    }

    private String tokenize(byte[] bytes) {
      return '"' + Bytes.toString(bytes).replaceAll("\"", "\\\\\"") + '"';
    }
  }

  public Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    Job job = Job.getInstance(conf, "lowes entity detail export");
    job.setJarByClass(Exporter.class);

    LOG.warn("arguments: {}", args);
    String tableName = args[1];
    String family = args[2];
    String prefix = args[3];

    TableMapReduceUtil.initTableMapperJob(tableName, prepareScan(family, prefix),
      EntityExportMapper.class, Text.class, Text.class, job);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    Path outputDir = new Path(new Path("/tmp/exported/" + tableName), prefix.replaceAll(":", "_"));
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outputDir, true);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.getConfiguration().set(FAMILY_NAME, family);

    return job;
  }

  private Scan prepareScan(String family, String prefix) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(family));
    scan.setCacheBlocks(false);
    scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
    scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
    return scan;
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = createSubmittableJob(getConf(), args);
    if (job == null) {
      return -1;
    }
    return (job.waitForCompletion(true) ? 0 : 1);
  }
}
