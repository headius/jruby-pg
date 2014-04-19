package org.jruby.pg.messages;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class CopyData extends ProtocolMessage {
  private final byte[] bytes;
  public ErrorResponse errorResponse;

  public CopyData(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public MessageType getType() {
    return MessageType.CopyData;
  }

  @Override
  public ByteBuffer toBytes() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      out.write('d');
      ByteUtils.writeInt4(out, bytes.length + 4);
      out.write(bytes, 0, bytes.length);
    } catch(Exception e) {
      // we cannot be here
    }
    return ByteBuffer.wrap(out.toByteArray());
  }

  public byte[] getValue() {
    return bytes;
  }
}
