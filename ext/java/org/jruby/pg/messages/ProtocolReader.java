package org.jruby.pg.messages;

/**
 * A protocol reader is used by the ProtocolMessageParser to parse
 * messages. This is implemented by the PostgresqlConnection to read
 * messages from the input buffer
 */
public interface ProtocolReader {
  /**
   * Returns true if a complete message is ready to be read
   */
  public boolean hasCompleteMessage();

  /**
   * Return the next byte
   */
  public byte getByte();

  /**
   * Returns the next int
   */
  public int getInt();

  /**
   * Returns the next short
   */
  public short getShort();

  /**
   * Returns the next null terminated string
   */
  public String getString();

  /**
  * Returns the next n bytes
  */
  public byte[] getNChar(int len);
}
