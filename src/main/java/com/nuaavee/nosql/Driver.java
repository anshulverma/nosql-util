package com.nuaavee.nosql;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.nuaavee.nosql.task.EntityDetailExporter;
import com.nuaavee.nosql.task.InvalidEntityLocator;

public class Driver {
  public static void main(String[] args) throws Exception {
    Tool tool = getTool(args[0]);
    int errCode = ToolRunner.run(HBaseConfiguration.create(), tool, args);
    System.exit(errCode);
  }

  private static Tool getTool(String toolName) {
    switch (toolName) {
      case "export":
        return new EntityDetailExporter();
      default:
        throw new IllegalArgumentException("no task by name '" + toolName + "'");
    }
  }
}
