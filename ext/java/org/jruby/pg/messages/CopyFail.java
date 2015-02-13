package org.jruby.pg.messages;


public class CopyFail extends FrontendMessage {
  private final String error;

  public CopyFail(String error) {
    this.error = error;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeString(error.getBytes());
  }

  @Override
  public MessageType getType() {
    return MessageType.CopyFail;
  }
}
