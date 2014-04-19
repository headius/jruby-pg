package org.jruby.pg.internal;

/**
 * The state of the connection
 */
public enum AsyncStatus {
  /** Idle and can execute a new query */
  Idle,
  /** Waiting for query result, isBusy() is true */
  Busy,
  /** Query result received and can be retrieved using getResult() */
  Ready,
  /** Doing CopyIn */
  CopyIn,
  /** Doing CopyOut */
  CopyOut,
  /** UNUSED: Doing CopyBoth */
  CopyBoth,
}
