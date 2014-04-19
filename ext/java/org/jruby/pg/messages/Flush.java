package org.jruby.pg.messages;

import java.nio.ByteBuffer;

public class Flush extends ProtocolMessage {
  private final byte [] bytes = {'H', 0, 0, 0, 4};

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public MessageType getType() {
    return MessageType.Flush;
  }

  @Override
  public ByteBuffer toBytes() {
    return ByteBuffer.wrap(bytes);
  }
}
