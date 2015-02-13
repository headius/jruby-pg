package org.jruby.pg.messages;


public class Close extends FrontendMessage {
  private final String name;
  private final StatementType type;

  public static enum StatementType {
    Portal,
    Prepared;
  }

  public Close(String name, StatementType type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeByte(type == StatementType.Portal ? 'P' : 'S');
    writer.writeString(name.getBytes());
  }

  @Override
  public MessageType getType() {
    return MessageType.Close;
  }

  public String getName() {
    return name;
  }

  public StatementType getStatmentType() {
    return type;
  }
}
