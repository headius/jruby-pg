package com.headius.jruby.pg_ext;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.jcodings.Encoding;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyEncoding;
import org.jruby.RubyFixnum;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

public class Result extends RubyObject {
    protected final ResultSet jdbcResultSet;
    protected final Encoding encoding;
    protected final boolean binary; // return results in binary format

    public Result(Ruby ruby, RubyClass rubyClass, ResultSet resultSet, Encoding encoding, boolean binary) {
        super(ruby, rubyClass);

        this.jdbcResultSet = resultSet;
        this.encoding = encoding;
        this.binary = binary;
    }

    public static void define(Ruby ruby, RubyModule pg, RubyModule constants) {
        RubyClass result = pg.defineClassUnder("Result", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);

        result.includeModule(ruby.getEnumerable());
        result.includeModule(constants);

        pg.defineClassUnder("LargeObjectFd", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);

        result.defineAnnotatedMethods(Result.class);
    }

    /******     PG::Result INSTANCE METHODS: libpq     ******/

    @JRubyMethod
    public IRubyObject result_status(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject res_status(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"error_message", "result_error_message"})
    public IRubyObject error_message(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"error_field", "result_error_field"})
    public IRubyObject error_field(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject clear(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"check", "check_result"})
    public IRubyObject check(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"ntuples", "num_tuples"})
    public IRubyObject ntuples(ThreadContext context) {
      // FIXME: this may not be the best way to count the result set, but I don't
      // know of any other way
      try {
        int curr = jdbcResultSet.getRow();
        jdbcResultSet.last();
        int count = jdbcResultSet.getRow();
        jdbcResultSet.absolute(curr);
        return new RubyFixnum(context.runtime, count);
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    @JRubyMethod(name = {"nfields", "num_fields"})
    public IRubyObject nfields(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject fname(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject fnumber(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject ftable(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject ftablecol(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject fformat(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject ftype(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject fmod(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject fsize(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(required = 2, argTypes = {RubyFixnum.class, RubyFixnum.class})
    public IRubyObject getvalue(ThreadContext context, IRubyObject _row, IRubyObject _column) {
      int row = (int) ((RubyFixnum) _row).getLongValue() + 1;
      int column = (int) ((RubyFixnum) _column).getLongValue() + 1;

      try {
        jdbcResultSet.absolute(row);
        IRubyObject value = getObjectAsString(context, column);
        return value;
      } catch (SQLException e) {
        throw Connection.newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod
    public IRubyObject getisnul(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject getlength(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject nparams(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject paramtype(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject cmd_status(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"cmd_tuples", "cmdtuples"})
    public IRubyObject cmd_tuples(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject old_value(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Result INSTANCE METHODS: other     ******/

    @JRubyMethod
    public IRubyObject values(ThreadContext context) {
      RubyArray array = context.runtime.newArray();

      try {
        jdbcResultSet.beforeFirst();

        while(jdbcResultSet.next()) {
          array.append(currentRowToArray(context));
        }

        return array;
      } catch (SQLException e) {
        throw Connection.newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(name = "[]")
    public IRubyObject op_aref(ThreadContext context, IRubyObject arg0) {
      Ruby runtime = context.runtime;
      int index = (int)arg0.convertToInteger().getLongValue() + 1;

      try {
        boolean success = jdbcResultSet.absolute(index);
        if (!success) return context.nil;
        return currentRowToHash(context);
      } catch (Exception e) {
          throw runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    @JRubyMethod
    public IRubyObject each(ThreadContext context, Block block) {
      try {
        jdbcResultSet.beforeFirst();
        while (jdbcResultSet.next()) {
            block.yieldSpecific(context, currentRowToHash(context));
        }
      } catch (Exception e) {
          throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
      return this;
    }

    @JRubyMethod
    public IRubyObject fields(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(required = 1, argTypes = {RubyFixnum.class})
    public IRubyObject column_values(ThreadContext context, IRubyObject index) {
      if (!(index instanceof RubyFixnum))
        throw context.runtime.newTypeError("argument must be a FixNum");

      RubyArray array = context.runtime.newArray();
      int columnIndex = (int) ((RubyFixnum) index).getLongValue() + 1;

      try {
        jdbcResultSet.beforeFirst();
        while (jdbcResultSet.next()) {
          IRubyObject value = getObjectAsString(context, columnIndex);
          array.append(value);
        }
        return array;
      } catch (SQLException e) {
        if (e.getLocalizedMessage().contains("The column index"))
          throw context.runtime.newIndexError(e.getLocalizedMessage());
        else
          throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    @JRubyMethod(required = 1, argTypes = {RubyString.class})
    public IRubyObject field_values(ThreadContext context, IRubyObject name) {
      if (!(name instanceof RubyString))
        throw context.runtime.newTypeError("name must be a string");

      RubyArray array = context.runtime.newArray();
      String fieldName = ((RubyString) name).asJavaString();

      try {
        jdbcResultSet.beforeFirst();
        while (jdbcResultSet.next()) {
          IRubyObject value = getObjectAsString(context, fieldName);
          array.append(value);
        }
        return array;
      } catch (SQLException e) {
        if (e.getLocalizedMessage().contains("The column name"))
          throw context.runtime.newIndexError(e.getLocalizedMessage());
        else
          throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    private RubyArray currentRowToArray(ThreadContext context) throws SQLException {
      ResultSetMetaData metaData = jdbcResultSet.getMetaData();

      RubyArray array = context.runtime.newArray();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        array.append(getObjectAsString(context, i));
      }

      return array;
    }

    private RubyHash currentRowToHash(ThreadContext context) throws SQLException {
        ResultSetMetaData metaData = jdbcResultSet.getMetaData();

        RubyHash hash = RubyHash.newHash(context.runtime);

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            hash.put(metaData.getColumnName(i), getObjectAsString(context, i));
        }

        return hash;
    }

    private IRubyObject getObjectAsString(ThreadContext context, int fieldNumber) throws SQLException {
      switch (jdbcResultSet.getMetaData().getColumnType(fieldNumber)) {
      case Types.BINARY:
      case Types.BLOB:
        RubyString string = context.runtime.newString(new ByteList(jdbcResultSet.getBytes(fieldNumber)));
        if (!binary)
          return string;
        return Connection.unescapeBytes(context, string);
      case Types.NULL:
        return context.nil;
      }
      RubyString value = context.runtime.newString(jdbcResultSet.getObject(fieldNumber).toString());
      if (encoding == null)
        return value;

      return value.encode(context, RubyEncoding.newEncoding(context.runtime, encoding));
    }

    private IRubyObject getObjectAsString(ThreadContext context, String fieldName) throws SQLException {
      ResultSetMetaData metaData = jdbcResultSet.getMetaData();
      int columns = metaData.getColumnCount();
      for (int i = 1; i <= columns; i++)
        if (metaData.getColumnName(i).equals(fieldName))
          return getObjectAsString(context, i);
      throw context.runtime.newIndexError("Invalid field name");
    }
}
