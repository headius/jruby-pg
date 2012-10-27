package com.headius.jruby.pg_ext;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyInteger;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;

import java.io.IOException;

public class Postgresql implements Library {
    enum ConnectionStatus {
      CONNECTION_OK, CONNECTION_BAD;
    };

    @Override
    public void load(Ruby ruby, boolean wrap) throws IOException {
        RubyModule pg = ruby.defineModule("PG");
        RubyClass pgError = ruby.defineClassUnder("Error", ruby.getStandardError(), ruby.getStandardError().getAllocator(), pg);
        RubyModule pgConstants = ruby.defineModuleUnder("Constants", pg);

        // create the connection status constants
        for(ConnectionStatus status : ConnectionStatus.values())
          pg.defineConstant(status.name(), new RubyFixnum(ruby, status.ordinal()));

        pg.getSingletonClass().defineAnnotatedMethods(Postgresql.class);

        Connection.define(ruby, pg, pgConstants);
        Result.define(ruby, pg, pgConstants);
    }

    @JRubyMethod
    public static IRubyObject library_version(ThreadContext context, IRubyObject self) {
        return context.nil;
    }

    @JRubyMethod
    public static IRubyObject isthreadsafe(ThreadContext context, IRubyObject self) {
        return context.runtime.getTrue();
    }
}
