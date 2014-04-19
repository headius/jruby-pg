package org.jruby.pg.io;

/**
 * Denote the current status of the SSL handshake
 */
public enum HandshakeStatus {
  /** The handshake needs to write more data to the socket */
  WRITING,
  /** The handshake needs to read more data from the socket */
  READING,
  /** The handshake is finished */
  FINISHED;
}
