package org.jruby.pg.messages;


public class CopyData extends FrontendMessage {
  private final byte[] bytes;
  public ErrorResponse errorResponse;

  public CopyData(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public MessageType getType() {
    return MessageType.CopyData;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeNChar(bytes);
  }

  public byte[] getValue() {
    return bytes;
  }
}
