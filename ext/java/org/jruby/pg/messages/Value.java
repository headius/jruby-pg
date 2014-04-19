package org.jruby.pg.messages;


public class Value {
  private final Format format;
  private final byte[] bytes;

  public Value(byte[] bytes, Format format) {
    this.bytes = bytes;
    this.format = format;
  }

  public Format getFormat() {
    return format;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
