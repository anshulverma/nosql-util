package com.nuaavee.nosql.task;

public class Parameters {

  private static final String CONF_PREFIX = "com.nuaavee.nosql.util:";

  public static String getName(String suffix) {
    return CONF_PREFIX + suffix;
  }
}
