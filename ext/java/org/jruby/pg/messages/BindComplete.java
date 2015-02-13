package org.jruby.pg.messages;

public class BindComplete extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.BindComplete;
  }
}
