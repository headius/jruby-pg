package org.jruby.pg.messages;


public class DataRow extends BackendMessage {
  private final byte[][] values;

  public DataRow(byte[][] values, int length) {
    this.values = values;
  }

  @Override
  public MessageType getType() {
    return MessageType.DataRow;
  }

  public byte[][] getValues() {
    return values;
  }
}
