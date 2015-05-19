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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nuaavee.nosql.filter.MissingColumnFilter;

public class InvalidEntityLocator extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(InvalidEntityLocator.class);
  private static final String FAMILY_NAME = Parameters.getName("family_name");
  private static final String SEARCH_COLUMN_NAME = Parameters.getName("search_column");
  private static final String CONSTANT_COLUMN_NAME = Parameters.getName("constant_column");

  public static class InvalidEntityMapper extends TableMapper<ImmutableBytesWritable, Result> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
      String family = context.getConfiguration().get(FAMILY_NAME);
      byte[] familyBytes = Bytes.toBytes(family);
      String searchColumnName = context.getConfiguration().get(SEARCH_COLUMN_NAME);
      String constantColumnName = context.getConfiguration().get(CONSTANT_COLUMN_NAME);
      if (!value.containsColumn(familyBytes, Bytes.toBytes(searchColumnName))) {
        LOG.error("found ===> key: {}, constantColumn: {}",
          Bytes.toString(key.get()),
          Bytes.toString(value.getValue(familyBytes, Bytes.toBytes(constantColumnName))));
      }
    }
  }

  public Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    LOG.warn("arguments: {}", args);
    String tableName = args[1];
    String family = args[2];
    String prefix = args[3];
    String searchColumn = args[4];
    String constantColumn = args[5];

    Job job = Job.getInstance(conf, "locate invalid entities for " + prefix);
    job.setJarByClass(EntityDetailExporter.class);

    TableMapReduceUtil.initTableMapperJob(tableName, prepareScan(family, searchColumn),
      InvalidEntityMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(NullOutputFormat.class);

    job.getConfiguration().set(FAMILY_NAME, family);
    job.getConfiguration().set(SEARCH_COLUMN_NAME, searchColumn);
    job.getConfiguration().set(CONSTANT_COLUMN_NAME, constantColumn);

    return job;
  }

  private Scan prepareScan(String family, String searchColumnName) throws IOException {
    byte[] familyBytes = Bytes.toBytes(family);
    Scan scan = new Scan();
    scan.addFamily(familyBytes);
    scan.setCacheBlocks(false);
    scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
    scan.setFilter(new MissingColumnFilter(familyBytes, Bytes.toBytes(searchColumnName)));
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
