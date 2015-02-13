package org.jruby.pg.messages;

import org.jruby.pg.internal.PostgresqlString;

public class Execute extends FrontendMessage {
  private PostgresqlString name;

  public Execute(PostgresqlString name) {
    this.name = name;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeString(name.getBytes());
    writer.writeInt(0);
  }

  @Override
  public MessageType getType() {
    return MessageType.Execute;
  }
}
