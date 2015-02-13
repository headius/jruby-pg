package org.jruby.pg.messages;

import org.jruby.pg.internal.PostgresqlString;

/**
 * A ProtocolWriter is used by the messages to write their content out
 * on the wire. This is implemented by PostgresqlConnection
 */
public interface ProtocolWriter {
  /**
   * Write message start
   */
  public void writeMsgStart(byte type);

  /**
   * Write a byte
   */
  public void writeByte(char b);

  /**
   * Write a 4 byte integer
   */
  public void writeInt(int n);

  /**
   * Write a 2 byte integer. The argument has to fit in a short,
   * otherwise it will be truncated. The argument isn't of type short
   * for convenience, otherwise there will be short casting all over
   * the place
   */
  public void writeShort(int s);

  /**
   * Write a null terminated string, add the null byte if the string
   * doesn't end with a null byte already
   */
  public void writeString(byte[] b);

  public void writeString(String s);

  public void writeString(PostgresqlString s);

  /**
   * Write n bytes
   */
  public void writeNChar(byte[] b);

  /**
   * Determine the length of the message and prepare it to be sent
   * over the wire
   */
  public void writeMsgEnd();
}
