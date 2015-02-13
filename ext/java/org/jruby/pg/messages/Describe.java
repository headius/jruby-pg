package org.jruby.pg.messages;

import org.jruby.pg.internal.PostgresqlString;
import org.jruby.pg.messages.Close.StatementType;

public class Describe extends FrontendMessage {
  private final PostgresqlString name;
  private final StatementType statementType;

  public Describe(PostgresqlString name, StatementType statementType) {
    this.name = name;
    this.statementType = statementType;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeByte(statementType == StatementType.Portal ? 'P' : 'S');
    writer.writeString(name.getBytes());
  }

  @Override
  public MessageType getType() {
    return MessageType.Describe;
  }

  public PostgresqlString getName() {
    return name;
  }

  public StatementType getStatementType() {
    return statementType;
  }
}
