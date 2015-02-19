package org.jruby.pg.internal;

import java.util.Properties;

public class Utils {
  public static String user(Properties prop) {
    return getPropertyOrEnv(prop, "user", "PGUSER", System.getProperty("user.name", ""));
  }

  public static String dbname(Properties prop) {
    return getPropertyOrEnv(prop, "dbname", "PGDATABASE", user(prop));
  }

  public static String password(Properties prop) {
    return getPropertyOrEnv(prop, "password", "PGPASSWORD", null);
  }

  public static String options(Properties prop) {
    return prop.getProperty("options", "");
  }

  public static String ssl(Properties prop) {
    return getPropertyOrEnv(prop, "sslmode", "PGSSLMODE", "disable");
  }


  public static int port(Properties prop) {
    String port = getPropertyOrEnv(prop, "port", "PGPORT", "5432");
    return Integer.parseInt(port);
  }

  public static String host(Properties prop) {
    String host = prop.getProperty("host");
    if(host == null) {
      return prop.getProperty("hostaddr", "localhost");
    }
    return host;
  }

  /**
   * Get the given key from prop, falling back to the given
   * environment variable and finally to defaultValue
   */
  private static String getPropertyOrEnv(Properties prop, String key,
                                         String envVar, String defaultVal) {
    String envValue = System.getenv(envVar);
    if(envValue == null || envValue.isEmpty()) {
      envValue = defaultVal;
    }
    return prop.getProperty(key, envValue);
  }
}
