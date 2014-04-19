package org.jruby.pg.internal;

public enum ConnectionStatus {
  CONNECTION_OK, CONNECTION_BAD,

  /* Non-blocking states */

  CONNECTION_NEEDED, // Internal state: connect() needed
  CONNECTION_STARTED, // Waiting for connection to be made.
  CONNECTION_MADE, // Connection OK; waiting to send.
  CONNECTION_SSL_STARTUP, // Negotiating SSL.
  CONNECTION_AWAITING_RESPONSE, // Sent StartupRequest and waiting for
  // response
  CONNECTION_AUTH_OK // Received authentication; waiting for backend startup
}
