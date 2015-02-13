package org.jruby.pg.messages;


public class AuthenticationSCMCredential extends BackendMessage {
  @Override
  public MessageType getType() {
    return MessageType.AuthenticationSCMCredential;
  }
}
