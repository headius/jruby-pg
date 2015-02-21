package org.jruby.pg.internal;

public interface NoticeReceiver {
  public void receive(ResultSet result);
}
