package org.jruby.pg.messages;

public class AuthenticationKerbosV5 extends BackendMessage {
  @Override
  public int getLength() {
    return 8;
  }

  @Override
  public MessageType getType() {
    return MessageType.AuthenticationKerbosV5;
  }
}
