package org.jruby.pg.messages;

public class AuthenticationCleartextPassword extends BackendMessage {
  @Override
  public int getLength() {
    return 8;
  }

  @Override
  public MessageType getType() {
    return MessageType.AuthenticationCleartextPassword;
  }
}
