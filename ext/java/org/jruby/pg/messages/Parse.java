package org.jruby.pg.messages;

import org.jruby.pg.internal.PostgresqlString;

public class Parse extends FrontendMessage {
  private PostgresqlString name;
  private PostgresqlString query;
  private int[] oids;

  public Parse(PostgresqlString name, PostgresqlString query, int [] oids) {
    this.name = name;
    this.query = query;
    this.oids = oids;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeString(name.getBytes());
    writer.writeString(query.getBytes());
    writer.writeShort(oids.length);
    for(int oid : oids) {
      writer.writeInt(oid);
    }
  }

  @Override
  public MessageType getType() {
    return MessageType.Parse;
  }
}
