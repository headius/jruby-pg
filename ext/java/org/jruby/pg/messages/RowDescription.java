package org.jruby.pg.messages;

public class RowDescription extends BackendMessage {
  private final Column[] columns;

  public RowDescription(Column[] columns, int length) {
    this.columns = columns;
  }

  @Override
  public MessageType getType() {
    return MessageType.RowDescription;
  }

  public Column[] getColumns() {
    return columns;
  }
}
