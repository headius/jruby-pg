package org.jruby.pg.internal;

import java.util.Properties;

public class PostgresqlConnectionUtils {
  public static String user(Properties props) {
    return props.getProperty("user", System.getProperty("user.name", ""));
  }

  public static String dbname(Properties prop) {
    return prop.getProperty("dbname");
  }

  public static String password(Properties prop) {
    return prop.getProperty("password", "");
  }

  public static String ssl(Properties prop) {
    return prop.getProperty("ssl", "disable");
  }


  public static int port(Properties prop) {
    return Integer.parseInt(prop.getProperty("port", "54321"));
  }

  public static String host(Properties prop) {
    String host = prop.getProperty("host");
    if (host == null) {
      return prop.getProperty("hostaddr", "localhost");
    }
    return host;
  }
}
