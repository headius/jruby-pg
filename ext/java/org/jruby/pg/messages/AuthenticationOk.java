package org.jruby.pg.messages;

public class AuthenticationOk extends BackendMessage {
  @Override
  public int getLength() {
    return 8;
  }

  @Override
  public MessageType getType() {
    return MessageType.AuthenticationOk;
  }
}
