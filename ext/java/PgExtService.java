import java.io.IOException;

import org.jruby.*;
import org.jruby.anno.JRubyMethod;
import org.jruby.pg.Connection;
import org.jruby.pg.Result;
import org.jruby.pg.internal.ConnectionStatus;
import org.jruby.pg.internal.LargeObjectAPI;
import org.jruby.pg.internal.PollingStatus;
import org.jruby.pg.internal.PingStatus;
import org.jruby.pg.internal.ResultSet.ResultStatus;
import org.jruby.pg.messages.ErrorResponse;
import org.jruby.pg.messages.ErrorResponse.ErrorField;
import org.jruby.pg.messages.Oid;
import org.jruby.pg.messages.TransactionStatus;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

/**
 * Class to provide pg_ext. This class is used to make "require
 * 'pg_ext'" work in JRuby.
 */
public class PgExtService implements BasicLibraryService {
  public boolean basicLoad(Ruby ruby) {
    try {
      load(ruby);
    } catch(IOException ex) {}
    return true;
  }

  private void load(Ruby ruby) throws IOException {
    RubyModule pg = ruby.defineModule("PG");
    RubyClass pgError = ruby.defineClassUnder("Error", ruby.getStandardError(), ruby.getStandardError().getAllocator(), pg);
    pgError.defineAlias("error", "message");
    pgError.addReadAttribute(ruby.getCurrentContext(), "connection");
    pgError.addReadAttribute(ruby.getCurrentContext(), "result");
    RubyModule pgConstants = ruby.defineModuleUnder("Constants", pg);

    // create the server error
    RubyHash errors = new RubyHash(ruby);
    pg.defineConstant("ERROR_CLASSES", errors);
    ruby.defineClassUnder("ServerError", pgError, ruby.getStandardError().getAllocator(), pg);
    RubyClass UnableToSendClass = ruby.defineClassUnder("UnableToSend", pgError, ruby.getStandardError().getAllocator(), pg);
    registerErrorClass(ruby, "UnableToSend", UnableToSendClass);
    RubyClass ConnectionBadClass = ruby.defineClassUnder("ConnectionBad", pgError, ruby.getStandardError().getAllocator(), pg);
    registerErrorClass(ruby, "ConnectionBad", ConnectionBadClass);
    defineErrors(ruby);

    Errors.initializeError(ruby);

    // create the connection status constants
    for(ConnectionStatus status : ConnectionStatus.values()) {
      pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));
    }

    for(TransactionStatus status : TransactionStatus.values()) {
      pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));
    }

    for(PollingStatus status : PollingStatus.values()) {
      pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));
    }

    for(PingStatus status : PingStatus.values()) {
      pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));
    }

    for(ResultStatus status : ResultStatus.values()) {
      pgConstants.defineConstant(status.name(), ruby.newFixnum(status.ordinal()));
    }

    // create the large object constants
    pgConstants.defineConstant("INV_READ", new RubyFixnum(ruby, LargeObjectAPI.READ));
    pgConstants.defineConstant("INV_WRITE", new RubyFixnum(ruby, LargeObjectAPI.WRITE));
    pgConstants.defineConstant("SEEK_SET", new RubyFixnum(ruby, LargeObjectAPI.SEEK_SET));
    pgConstants.defineConstant("SEEK_END", new RubyFixnum(ruby, LargeObjectAPI.SEEK_END));
    pgConstants.defineConstant("SEEK_CUR", new RubyFixnum(ruby, LargeObjectAPI.SEEK_CUR));

    // create error fields objects
    for(ErrorField field : ErrorResponse.ErrorField.values()) {
      pgConstants.defineConstant(field.name(), ruby.newFixnum(field.getCode()));
    }

    pg.getSingletonClass().defineAnnotatedMethods(PgExtService.class);

    try {
      for(java.lang.reflect.Field field : Oid.class.getDeclaredFields()) {
        String name = field.getName();
        int value = field.getInt(null);
        pgConstants.defineConstant("OID_" + name, ruby.newFixnum(value));
      }
    } catch(Exception e) {
      ruby.newRuntimeError(e.getLocalizedMessage());
    }

    pgConstants.defineConstant("INVALID_OID", ruby.newFixnum(Oid.UNSPECIFIED));
    pg.includeModule(pgConstants);
    Connection.define(ruby, pg, pgConstants);
    Result.define(ruby, pg, pgConstants);
  }

  public static RubyClass defineErrorClass(Ruby ruby, String className, String baseClassCode) {
    RubyClass baseRubyClass = (RubyClass) ruby.getClassFromPath("PG::ServerError");
    if(baseClassCode != null) {
      RubyHash errors = (RubyHash) ruby.getModule("PG").getConstant("ERROR_CLASSES");
      baseClassCode = baseClassCode.toUpperCase();
      baseRubyClass = (RubyClass) errors.op_aref(ruby.getCurrentContext(), ruby.newString(baseClassCode));
    }

    return ruby.getModule("PG").defineClassUnder(className, baseRubyClass, ruby.getStandardError().getAllocator());
  }

  public static void registerErrorClass(Ruby ruby, String classCode, RubyClass klass) {
    RubyHash errors = (RubyHash) ruby.getModule("PG").getConstant("ERROR_CLASSES");
    classCode = classCode.toUpperCase();
    errors.op_aset(ruby.getCurrentContext(), ruby.newString(classCode), klass);
  }

  public void defineErrors(Ruby ruby) {
    RubyModule pg = ruby.getModule("PG");

    for(ErrorField f : ErrorResponse.ErrorField.values()) {
      String name = f.name();
      pg.defineConstant(name, RubyInteger.int2fix(ruby, f.getCode()));
    }
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
