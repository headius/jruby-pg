package org.jruby.pg.messages;


public class CancelRequest extends FrontendMessage {
  private final int pid;
  private final int secret;

  public CancelRequest(int pid, int secret) {
    this.pid = pid;
    this.secret = secret;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeInt(80877102);
    writer.writeInt(pid);
    writer.writeInt(secret);
  };

  @Override
  public MessageType getType() {
    return MessageType.CancelRequest;
  }

  public int getPid() {
    return pid;
  }

  public int getSecret() {
    return secret;
  }
}
