package com.headius.jruby.pg_ext;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;

import java.io.IOException;

public class Postgresql implements Library {
    @Override
    public void load(Ruby ruby, boolean wrap) throws IOException {
        RubyModule pg = ruby.defineModule("PG");
        RubyClass pgError = ruby.defineClassUnder("Error", ruby.getStandardError(), ruby.getStandardError().getAllocator(), pg);
        RubyModule pgConstants = ruby.defineModuleUnder("Constants", pg);

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
