package org.jruby.pg.messages;

public class ParseComplete extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.ParseComplete;
  }
}
