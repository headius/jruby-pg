package org.jruby.pg.messages;

public class AuthenticationKerbosV5 extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.AuthenticationKerbosV5;
  }
}
