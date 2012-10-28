package com.headius.jruby.pg_ext;

import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class Postgresql implements Library {
    public static enum ConnectionStatus {
      CONNECTION_OK, CONNECTION_BAD;
    };

    @Override
    public void load(Ruby ruby, boolean wrap) throws IOException {
        RubyModule pg = ruby.defineModule("PG");
        ruby.defineClassUnder("Error", ruby.getStandardError(), ruby.getStandardError().getAllocator(), pg);
        RubyModule pgConstants = ruby.defineModuleUnder("Constants", pg);

        // create the connection status constants
        for(ConnectionStatus status : ConnectionStatus.values())
          pg.defineConstant(status.name(), new RubyFixnum(ruby, status.ordinal()));

        // create the large object constants
        pg.defineConstant("INV_READ", new RubyFixnum(ruby, LargeObjectManager.READ));
        pg.defineConstant("INV_WRITE", new RubyFixnum(ruby, LargeObjectManager.WRITE));
        pg.defineConstant("SEEK_SET", new RubyFixnum(ruby, LargeObject.SEEK_SET));
        pg.defineConstant("SEEK_END", new RubyFixnum(ruby, LargeObject.SEEK_END));
        pg.defineConstant("SEEK_CUR", new RubyFixnum(ruby, LargeObject.SEEK_CUR));

        pg.getSingletonClass().defineAnnotatedMethods(Postgresql.class);

        Connection.define(ruby, pg, pgConstants);
        Result.define(ruby, pg, pgConstants);
    }

    @JRubyMethod
    public static IRubyObject library_version(ThreadContext context, IRubyObject self) {
      // FIXME: we should detect the version of the jdbc driver and return it instead
      return context.runtime.newFixnum(91903);
    }

    @JRubyMethod(alias = {"threadsafe?"})
    public static IRubyObject isthreadsafe(ThreadContext context, IRubyObject self) {
        return context.runtime.getTrue();
    }
}
