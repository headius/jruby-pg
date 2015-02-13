package org.jruby.pg.messages;

public class Startup extends FrontendMessage {
  private String user;
  private String database;
  private String options;

  public Startup(String user, String database, String options) {
    this.user = user;
    this.database = database;
    this.options = options;
  }

  @Override
  public void writeInternal(ProtocolWriter writer) {
    writer.writeShort(3); // major version is 3
    writer.writeShort(0); // minor version is 0
    writer.writeString("user");
    writer.writeString(user);
    writer.writeString("database");
    writer.writeString(database);
    writer.writeString("options");
    writer.writeString(options);
    writer.writeByte((char) 0);
  }

  @Override
  public MessageType getType() {
    return MessageType.StartupMessage;
  }

}
