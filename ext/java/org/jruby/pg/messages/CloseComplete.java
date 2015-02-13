package org.jruby.pg.messages;

public class CloseComplete extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.CloseComplete;
  }
}
