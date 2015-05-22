package com.nuaavee.nosql.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
import com.google.common.base.Joiner;

public class Exporter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(Locator.class);
  private static final String FAMILY_NAME = Parameters.getName("family_name");
  private static final String EXPORT_TYPE = Parameters.getName("export_type");

  public static class RowExportMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
      String family = context.getConfiguration().get(FAMILY_NAME);
      String exportType = context.getConfiguration().get(EXPORT_TYPE);
      NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Bytes.toBytes(family));
      DetailsExport detailsExport = getDetailsExport(exportType);
      for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
        detailsExport.write(entry.getKey(), entry.getValue());
      }
      if (detailsExport.canExport()) {
        context.write(new Text(""), new Text(detailsExport.export()));
      }
    }

    private DetailsExport getDetailsExport(String exportType) {
      switch (exportType) {
        case "json":
          return new JSONDetailsExport();
        case "csv":
          return new CSVDetailsExport();
        default:
          throw new IllegalArgumentException("unknown export type: " + exportType);
      }
    }

    private abstract class DetailsExport {
      private static final String ATTRIBUTE_SEPARATOR = ",";

      private String fieldSeparator;

      private final Map<String, List<String>> details = new HashMap<>();

      DetailsExport(String fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
      }

      final void write(byte[] key, byte[] value) {
        String keyToken = Bytes.toString(key).replaceAll("\"", "\\\\\"").split(":")[0];
        List<String> valueList = details.get(keyToken);
        if (valueList == null) {
          valueList = new ArrayList<>();
          details.put(keyToken, valueList);
        }
        valueList.add(Bytes.toString(value).replaceAll("\"", "\\\\\""));
      }

      final byte[] export() {
        StringBuilder exported = new StringBuilder();
        List<String> idValue = details.remove("id");
        append(exported, "id", idValue);
        for (Map.Entry<String, List<String>> detailEntry : details.entrySet()) {
          if (exported.length() > 0) {
            exported.append(ATTRIBUTE_SEPARATOR);
          }
          append(exported, detailEntry.getKey(), detailEntry.getValue());
        }
        return Bytes.toBytes(wrap(exported.toString()));
      }

      private void append(StringBuilder exported, String key, List<String> value) {
        exported
          .append(tokenize(key))
          .append(fieldSeparator)
          .append(tokenize(Joiner.on(fieldSeparator).join(value)));
      }

      public boolean canExport() {
        return details.containsKey("id");
      }

      protected String tokenize(String str) {
        return str;
      }

      protected String wrap(String exported) {
        return exported;
      }
    }

    private class JSONDetailsExport extends DetailsExport {

      private JSONDetailsExport() {
        super(",");
      }

      @Override
      public String tokenize(String str) {
        return '"' + str + '"';
      }

      @Override
      protected String wrap(String exported) {
        return "{" + exported + "}";
      }
    }

    private class CSVDetailsExport extends DetailsExport {
      private CSVDetailsExport() {
        super("#");
      }
    }
  }

  public Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    LOG.warn("arguments: {}", args);
    String tableName = args[1];
    String family = args[2];
    String startRow = args[3];
    String stopRow = args[4];
    String exportType = args[5];

    Job job = Job.getInstance(conf, "export rows from " + startRow + " to " + stopRow);
    job.setJarByClass(Exporter.class);

    TableMapReduceUtil.initTableMapperJob(tableName, prepareScan(family, startRow, stopRow),
      RowExportMapper.class, Text.class, Text.class, job);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    Path outputDir = new Path(new Path("/tmp/exported/" + tableName), startRow.replaceAll(":", "_"));
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outputDir, true);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.getConfiguration().set(FAMILY_NAME, family);
    job.getConfiguration().set(EXPORT_TYPE, exportType);

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
