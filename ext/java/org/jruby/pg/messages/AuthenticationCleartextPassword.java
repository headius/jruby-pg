package org.jruby.pg.messages;

public class AuthenticationCleartextPassword extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.AuthenticationCleartextPassword;
  }
}
