package org.jruby.pg.messages;


public class Flush extends FrontendMessage {
  @Override
  public MessageType getType() {
    return MessageType.Flush;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    // nothing, this is an empty message
  }
}
