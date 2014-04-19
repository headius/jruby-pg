package org.jruby.pg.internal;

/**
 * The state of the connection
 */
public enum AsyncState {
  /** Idle and can execute a new query */
  Idle,
  /** Waiting for query result, isBusy() is true */
  Busy,
  /** Query result received and can be retrieved using getResult() */
  Ready,
  /** Doing Copy In */
  CopyIn,
  /** Doing Copy Out */
  CopyOut,
  /** UNUSED: Doing Copy In/Out */
  CopyBoth,
}
