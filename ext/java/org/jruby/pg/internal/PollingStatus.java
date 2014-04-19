package org.jruby.pg.internal;

public enum PollingStatus {
  PGRES_POLLING_FAILED,
  PGRES_POLLING_READING,      /* These two indicate that one may    */
  PGRES_POLLING_WRITING,      /* use select before polling again.   */
  PGRES_POLLING_OK
}
