package org.jruby.pg.messages;


public class Terminate extends FrontendMessage {
  @Override
  public MessageType getType() {
    return MessageType.Terminate;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    // nothing, this is an empty message
  }
}
