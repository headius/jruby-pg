package org.jruby.pg.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

/**
 * A {@link FlushableByteChannel} wrapper around a {@link ByteChannel}.
 * This implementation's {@link SocketByteChannel#flush()}
 * always return true.
 */
public class SocketByteChannel implements FlushableByteChannel {
  private final ByteChannel socket;

  public SocketByteChannel(ByteChannel socket) {
    this.socket = socket;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return socket.read(dst);
  }

  @Override
  public boolean isOpen() {
    return socket.isOpen();
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return socket.write(src);
  }

  @Override
  public boolean flush() throws IOException {
    return true;
  }
}
