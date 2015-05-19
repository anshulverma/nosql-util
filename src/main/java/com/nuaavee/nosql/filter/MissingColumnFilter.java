package com.nuaavee.nosql.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MissingColumnFilter extends FilterBase {

  private static final Logger LOG = LoggerFactory.getLogger(MissingColumnFilter.class);

  private byte[] columnFamily;
  private byte[] columnQualifier;

  @SuppressWarnings("unused") // used by scanner at runtime
  public MissingColumnFilter() {
    super();
  }

  public MissingColumnFilter(byte[] columnFamily, byte[] columnQualifier) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue keyValue) {
//    LOG.error("buffer: {}, key: {}, value: {}",
//      new Object[] {
//        Bytes.toString(ignored.getBuffer()),
//        Bytes.toString(ignored.getKey()),
//        Bytes.toString(ignored.getValue())
//      });
    if (keyValue.matchingColumn(columnFamily, Bytes.toBytes("name"))) {
      return ReturnCode.INCLUDE;
    }
    if (keyValue.matchingColumn(columnFamily, columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.SKIP;
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, columnFamily);
    Bytes.writeByteArray(out, columnQualifier);
  }

  public void readFields(DataInput in) throws IOException {
    columnFamily = Bytes.readByteArray(in);
    columnQualifier = Bytes.readByteArray(in);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " "
      + Bytes.toStringBinary(columnFamily) + ":" + Bytes.toString(columnQualifier);
  }

//  @Override
//  public void write(DataOutput out) throws IOException {
//    out.writeInt(columnName.length());
//    out.write(Bytes.toBytes(columnName));
//  }
//
//  @Override
//  public void readFields(DataInput in) {
//    try {
//      int len = in.readInt();
//      if (len > 0) {
//        byte[] buffer = new byte[len];
//        in.readFully(buffer, 0, len);
//        columnName = Bytes.toString(buffer);
//      }
//    } catch (Throwable t) {
//      LOG.error("unable to read column name", t);
//      columnName = "id";
//    }
//  }
}
