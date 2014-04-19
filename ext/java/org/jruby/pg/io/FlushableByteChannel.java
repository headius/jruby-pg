package org.jruby.pg.io;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public interface FlushableByteChannel extends ReadableByteChannel, WritableByteChannel {

  /**
   * Flush all buffered data
   *
   * @return true if all data has been flushed successfully, false
   * otherwise
   * @throws IOException
   */
  public boolean flush() throws IOException;
}
