package org.jruby.pg;

import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.pg.internal.ConnectionState;
import org.jruby.pg.internal.LargeObjectAPI;
import org.jruby.pg.internal.Oid;
import org.jruby.pg.internal.ResultSet.ResultStatus;
import org.jruby.pg.internal.messages.ErrorResponse;
import org.jruby.pg.internal.messages.ErrorResponse.ErrorField;
import org.jruby.pg.internal.messages.TransactionStatus;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;

public class Postgresql implements Library {
    @Override
    public void load(Ruby ruby, boolean wrap) throws IOException {
        RubyModule pg = ruby.defineModule("PG");
        ruby.defineClassUnder("Error", ruby.getStandardError(), ruby.getStandardError().getAllocator(), pg);
        RubyModule pgConstants = ruby.defineModuleUnder("Constants", pg);

        // create the connection status constants
        for(ConnectionState status : ConnectionState.values())
          pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));

        for (TransactionStatus status: TransactionStatus.values())
          pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));

        for (ResultStatus status : ResultStatus.values())
          pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));

        // create the large object constants
        pgConstants.defineConstant("INV_READ", new RubyFixnum(ruby, LargeObjectAPI.READ));
        pgConstants.defineConstant("INV_WRITE", new RubyFixnum(ruby, LargeObjectAPI.WRITE));
        pgConstants.defineConstant("SEEK_SET", new RubyFixnum(ruby, LargeObjectAPI.SEEK_SET));
        pgConstants.defineConstant("SEEK_END", new RubyFixnum(ruby, LargeObjectAPI.SEEK_END));
        pgConstants.defineConstant("SEEK_CUR", new RubyFixnum(ruby, LargeObjectAPI.SEEK_CUR));

        // create error fields objects
        for (ErrorField field : ErrorResponse.ErrorField.values())
          pgConstants.defineConstant(field.name(), ruby.newFixnum(field.getCode()));

        pg.getSingletonClass().defineAnnotatedMethods(Postgresql.class);

        try {
          for (java.lang.reflect.Field field : Oid.class.getDeclaredFields()) {
            String name = field.getName();
            int value = field.getInt(null);
            pgConstants.defineConstant("OID_" + name, ruby.newFixnum(value));
          }
        } catch (Exception e) {
          ruby.newRuntimeError(e.getLocalizedMessage());
        }

        pgConstants.defineConstant("INVALID_OID", ruby.newFixnum(Oid.UNSPECIFIED));
        pg.includeModule(pgConstants);
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
