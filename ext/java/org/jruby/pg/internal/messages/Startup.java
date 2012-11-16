package org.jruby.pg.internal.messages;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Properties;

public class Startup extends ProtocolMessage {
  private final byte[] bytes;

  public Startup(String user, String database, Properties props) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      ByteUtils.writeInt4(out, 0);
      ByteUtils.writeInt2(out, 3);
      ByteUtils.writeInt2(out, 0);
      ByteUtils.writeString(out, "user");
      ByteUtils.writeString(out, user);
      ByteUtils.writeString(out, "database");
      ByteUtils.writeString(out, database);
      for (Entry<Object, Object> entry : props.entrySet()) {
        ByteUtils.writeString(out, entry.getKey().toString());
        ByteUtils.writeString(out, entry.getValue().toString());
      }
      out.write('\0');
    } catch (Exception e) {
      // we cannot be here
    }
    this.bytes = out.toByteArray();
    ByteUtils.fixLength(bytes, 0);
  }

  @Override
  public int getLength() {
    return bytes.length;
  }

  @Override
  public MessageType getType() {
    return MessageType.StartupMessage;
  }

  @Override
  public ByteBuffer toBytes() {
    return ByteBuffer.wrap(bytes);
  }
}
