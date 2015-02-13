package org.jruby.pg.messages;



public class Sync extends FrontendMessage {
  @Override
  public MessageType getType() {
    return MessageType.Sync;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    // nothing, this is an empty message
  }
}
