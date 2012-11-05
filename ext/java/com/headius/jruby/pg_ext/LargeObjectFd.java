package com.headius.jruby.pg_ext;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.postgresql.largeobject.LargeObject;

public class LargeObjectFd extends RubyObject {

  private final LargeObject object;

  public LargeObjectFd(Ruby runtime, RubyClass metaClass, LargeObject object) {
    super(runtime, metaClass);
    this.object = object;
  }

  public LargeObject getObject() {
    return object;
  }
}
