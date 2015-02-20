package org.jruby.pg.internal;

import java.nio.charset.Charset;

public class PostgresqlString {
  public static final PostgresqlString NULL_STRING = new PostgresqlString(new byte[0]) {
    @Override
    public byte[] getBytes() {
      return new byte[0];
    }
  };

  private final byte[] bytes;
  private final Charset charset;

  public PostgresqlString(byte[] bytes) {
    this.bytes = bytes;
    charset = null;
  }

  public PostgresqlString(String value) {
    this.bytes = value.getBytes();
    charset = null;
  }

  public PostgresqlString(String value, Charset charset) {
    this.bytes = value.getBytes(charset);
    this.charset = charset;
  }

  public byte[] getBytes() {
    byte[] bytes = new byte[this.bytes.length];
    System.arraycopy(this.bytes, 0, bytes, 0, bytes.length);
    return bytes;
  }

  public String toString() {
    if(charset == null) {
      return new String(bytes);
    }
    return new String(bytes, charset);
  }
}
