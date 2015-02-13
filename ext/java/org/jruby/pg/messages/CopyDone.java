package org.jruby.pg.messages;


public class CopyDone extends FrontendMessage {
  @Override
  public MessageType getType() {
    return MessageType.CopyDone;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    // nothing, this is an empty message
  }
}
