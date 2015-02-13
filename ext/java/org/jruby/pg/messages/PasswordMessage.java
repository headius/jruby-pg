package org.jruby.pg.messages;


public class PasswordMessage extends FrontendMessage {
  private final byte[] password;

  public PasswordMessage(byte[] password) {
    this.password = password;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeString(password);
  }

  @Override
  public MessageType getType() {
    return MessageType.PasswordMessage;
  }
}
