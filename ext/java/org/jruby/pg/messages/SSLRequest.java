package org.jruby.pg.messages;


public class SSLRequest extends FrontendMessage {
  public SSLRequest() {
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeInt(80877103);
  }

  @Override
  public MessageType getType() {
    return MessageType.SSLRequest;
  }
}
