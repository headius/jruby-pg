package org.jruby.pg.messages;

import org.jruby.pg.internal.PostgresqlString;

public class Query extends FrontendMessage {
  private final PostgresqlString query;

  public Query(PostgresqlString query) {
    this.query = query;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeString(query.getBytes());
  }

  @Override
  public MessageType getType() {
    return MessageType.Query;
  }

  public PostgresqlString getQuery() {
    return query;
  }
}
