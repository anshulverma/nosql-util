package com.nuaavee.nosql.task;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowCounter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(Locator.class);
  private static final String FAMILY_NAME = Parameters.getName("family_name");

  public static class RowCountMapper extends TableMapper<ImmutableBytesWritable, Result> {

    private static enum Counters { ROWS }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

  public Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    LOG.warn("arguments: {}", args);
    String tableName = args[1];
    String family = args[2];
    String startRow = args[3];
    String stopRow = args[4];

    Job job = Job.getInstance(conf, "count rows from " + startRow + " to " + stopRow);
    job.setJarByClass(RowCounter.class);

    TableMapReduceUtil.initTableMapperJob(tableName, prepareScan(family, startRow, stopRow),
      RowCountMapper.class, NullWritable.class, NullWritable.class, job);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.getConfiguration().set(FAMILY_NAME, family);

    return job;
  }

  private Scan prepareScan(String family, String startRow, String stopRow) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(family));
    scan.setCacheBlocks(false);
    scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
    scan.setStartRow(Bytes.toBytes(startRow));
    scan.setStopRow(Bytes.toBytes(stopRow));
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
