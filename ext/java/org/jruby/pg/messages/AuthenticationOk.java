package org.jruby.pg.messages;

public class AuthenticationOk extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.AuthenticationOk;
  }
}
