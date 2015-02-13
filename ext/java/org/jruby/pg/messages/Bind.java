package org.jruby.pg.messages;

import org.jruby.pg.internal.PostgresqlString;

public class Bind extends FrontendMessage {
  private final byte[] destinationPortal, sourceStatement;
  private final Value[] params;
  private final Format format;

  public Bind(PostgresqlString destinationPortal, PostgresqlString sourceStatement, Value[] params, Format format) {
    this.destinationPortal = destinationPortal.getBytes();
    this.sourceStatement = sourceStatement.getBytes();
    this.params = params;
    this.format = format;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeString(destinationPortal);
    writer.writeString(sourceStatement);
    writer.writeShort(params.length);
    for(Value parameter : params) {
      writer.writeShort(parameter.getFormat().getValue());
    }
    writer.writeShort(params.length);
    for(Value parameter : params) {
      if(parameter.getBytes() == null) {
        writer.writeInt(-1);
      } else {
        writer.writeInt(parameter.getBytes().length);
        writer.writeNChar(parameter.getBytes());
      }
    }
    writer.writeShort(1);
    writer.writeShort(format.getValue());
  }

  @Override
  public MessageType getType() {
    return MessageType.Bind;
  }
}
