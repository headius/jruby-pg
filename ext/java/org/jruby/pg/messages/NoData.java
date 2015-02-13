package org.jruby.pg.messages;

public class NoData extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.NoData;
  }
}
