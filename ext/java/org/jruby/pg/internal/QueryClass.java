package org.jruby.pg.internal;

/**
 * This enum denotes the type of query that is currently being
 * executed by the connection
 */
public enum QueryClass {
  Simple,
  Extended,
  Prepare,
  Describe,
}
