package org.jruby.pg.messages;

public abstract class FrontendMessage extends ProtocolMessage {
  /**
   * Encode the message and write to the given writer
   */
  public void write(ProtocolWriter writer) {
    writer.writeMsgStart(getFirstByte());
    writeInternal(writer);
    writer.writeMsgEnd();
  }

  /**
   * Implemented by subclasses who don't want to worry about
   * starting and ending the message. Implementors can ignore
   * the first byte and the length of the message and write
   * the rest of the fields using the different write() methods
   * of the {@link ProtocolWriter}
   *
   * @param writer
   */
  public abstract void writeInternal(ProtocolWriter writer);
}
