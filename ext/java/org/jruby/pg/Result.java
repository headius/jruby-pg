package org.jruby.pg;

import java.util.List;

import org.jcodings.Encoding;
import org.jruby.*;
import org.jruby.anno.JRubyMethod;
import org.jruby.pg.internal.ResultSet;
import org.jruby.pg.messages.Column;
import org.jruby.pg.messages.DataRow;
import org.jruby.pg.messages.Format;
import org.jruby.pg.messages.RowDescription;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

@SuppressWarnings("serial")
public class Result extends RubyObject {

  private ResultSet res;
  private Connection conn;
  private final Encoding encoding;

  public Result(Ruby ruby, RubyClass rubyClass, Connection connection, ResultSet resultSet, Encoding encoding) {
    super(ruby, rubyClass);
    this.conn = connection;
    this.res = resultSet;
    this.encoding = encoding;
  }

  public static void define(Ruby ruby, RubyModule pg, RubyModule constants) {
    RubyClass result = pg.defineClassUnder("Result", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    result.includeModule(ruby.getEnumerable());
    result.includeModule(constants);
    result.defineAnnotatedMethods(Result.class);
  }

  /******     PG::Result INSTANCE METHODS    ******/

  @JRubyMethod
  public IRubyObject result_status(ThreadContext context) {
    return context.runtime.newFixnum(res.getStatus().ordinal());
  }

  @JRubyMethod
  public IRubyObject res_status(ThreadContext context, IRubyObject arg0) {
    return context.runtime.newString(res.getStatus().toString());
  }

  @JRubyMethod
  public IRubyObject error_message(ThreadContext context) {
    String error = res.getError();
    if(error == null) {
      error = "";
    }
    return context.getRuntime().newString(error);
  }

  @JRubyMethod(name = {"error_field", "result_error_field"})
  public IRubyObject error_field(ThreadContext context, IRubyObject arg0) {
    byte errorCode = (byte)((RubyFixnum) arg0).getLongValue();
    String field = res.getErrorField(errorCode);
    return field == null ? context.nil : context.runtime.newString(field);
  }

  @JRubyMethod
  public IRubyObject clear(ThreadContext context) {
    this.res = null;
    this.conn = null;
    return context.nil;
  }

  /**
  * Check if the given result is in a good state, otherwise raise an
  * error
  */
  @JRubyMethod(alias = {"check_result"})
  public IRubyObject check(ThreadContext context) {
    String error;
    if(res == null) {
      error = conn.error_message(context).asJavaString();
    }

    else {
      switch(this.res.getStatus()) {
      case PGRES_TUPLES_OK:
      case PGRES_COPY_OUT:
      case PGRES_COPY_IN:
      case PGRES_COPY_BOTH:
      case PGRES_SINGLE_TUPLE:
      case PGRES_EMPTY_QUERY:
      case PGRES_COMMAND_OK:
        return this;
      case PGRES_BAD_RESPONSE:
      case PGRES_FATAL_ERROR:
      case PGRES_NONFATAL_ERROR:
        error = res.getError();
        break;
      default:
        error = "internal error : unknown result status.";
        break;
      }
    }

    throw conn.newPgError(context, error, res);
  }

  @JRubyMethod(name = {"ntuples", "num_tuples"})
  public IRubyObject ntuples(ThreadContext context) {
    return context.runtime.newFixnum(res.getRows().size());
  }

  @JRubyMethod(name = {"nfields", "num_fields"})
  public IRubyObject nfields(ThreadContext context) {
    if(res == null) {
      throw context.runtime.newTypeError("foo");
    }
    if(res.getDescription() == null) {
      return context.runtime.newFixnum(0);
    }
    return context.runtime.newFixnum(res.getDescription().getColumns().length);
  }

  @JRubyMethod
  public IRubyObject fname(ThreadContext context, IRubyObject _columnIndex) {
    int columnIndex = (int)((RubyFixnum) _columnIndex).getLongValue();
    Column[] columns = res.getDescription().getColumns();
    if(columnIndex >= columns.length || columnIndex < 0) {
      throw context.runtime.newArgumentError("invalid field number " + columnIndex);
    }

    return context.runtime.newString(columns[columnIndex].getName());
  }

  @JRubyMethod
  public IRubyObject fnumber(ThreadContext context, IRubyObject arg0) {
    return context.nil;
  }

  @JRubyMethod(required = 1)
  public IRubyObject ftable(ThreadContext context, IRubyObject _columnIndex) {
    int columnIndex = (int)((RubyFixnum) _columnIndex).getLongValue();
    Column[] columns = res.getDescription().getColumns();
    if(columnIndex >= columns.length || columnIndex < 0) {
      throw context.runtime.newArgumentError("column " + columnIndex + " is out of range");
    }

    int oid = columns[columnIndex].getTableOid();
    return context.runtime.newFixnum(oid);
  }

  @JRubyMethod(required = 1)
  public IRubyObject ftablecol(ThreadContext context, IRubyObject _columnIndex) {
    int columnIndex = (int)((RubyFixnum) _columnIndex).getLongValue();
    Column[] columns = res.getDescription().getColumns();
    if(columnIndex >= columns.length || columnIndex < 0) {
      throw context.runtime.newArgumentError("column " + columnIndex + " is out of range");
    }

    int tableIndex = columns[columnIndex].getTableIndex();
    return context.runtime.newFixnum(tableIndex);
  }

  @JRubyMethod
  public IRubyObject fformat(ThreadContext context, IRubyObject arg0) {
    int index = (int)((RubyFixnum) arg0).getLongValue();
    Column[] columns = res.getDescription().getColumns();
    if(index >= columns.length || index < 0) {
      throw context.runtime.newArgumentError("column number " + index + " is out of range");
    }
    return context.runtime.newFixnum(columns[index].getFormat());
  }

  @JRubyMethod(required = 1, argTypes = {RubyFixnum.class})
  public IRubyObject ftype(ThreadContext context, IRubyObject fieldNumber) {
    RowDescription description = res.getDescription();
    int field = (int)((RubyFixnum) fieldNumber).getLongValue();
    if(field >= description.getColumns().length) {
      throw context.runtime.newIndexError("field " + field + " is out of range");
    }
    return context.runtime.newFixnum(description.getColumns()[field].getOid());
  }

  @JRubyMethod
  public IRubyObject fmod(ThreadContext context, IRubyObject arg0) {
    Column[] columns = res.getDescription().getColumns();
    int index = (int)((RubyFixnum) arg0).getLongValue();
    if(index < 0 || index >= columns.length) {
      throw context.runtime.newArgumentError("column number " + index + " is out of range");
    }
    return context.runtime.newFixnum(columns[index].getTypmod());
  }

  @JRubyMethod
  public IRubyObject fsize(ThreadContext context, IRubyObject arg0) {
    return context.nil;
  }

  @JRubyMethod(required = 2, argTypes = {RubyFixnum.class, RubyFixnum.class})
  public IRubyObject getvalue(ThreadContext context, IRubyObject _row, IRubyObject _column) {
    int row = (int)((RubyFixnum) _row).getLongValue();
    int column = (int)((RubyFixnum) _column).getLongValue();

    List<DataRow> rows = res.getRows();
    if(row >= rows.size()) {
      throw context.runtime.newIndexError("row " + row + " is out of range");
    }
    DataRow dataRow = rows.get(row);
    byte[][] columns = dataRow.getValues();
    if(column >= columns.length) {
      throw context.runtime.newIndexError("column " + column + " is out of range");
    }
    return valueAsString(context, row, column);
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

  @JRubyMethod(required = 1)
  public IRubyObject paramtype(ThreadContext context, IRubyObject arg0) {
    int index = (int)((RubyFixnum) arg0).getLongValue();
    if(res.getParameterDescription() == null) {
      throw conn.newPgError(context, "Parameter desciption not available", null);
    }
    int[] oids = res.getParameterDescription().getOids();
    if(index >= oids.length) {
      throw context.runtime.newIndexError("index " + index + " is out of range");
    }
    return context.runtime.newFixnum(oids[index]);
  }

  @JRubyMethod
  public IRubyObject cmd_status(ThreadContext context) {
    return context.nil;
  }

  @JRubyMethod(name = {"cmd_tuples", "cmdtuples"})
  public IRubyObject cmd_tuples(ThreadContext context) {
    int rows = res.getCmdStatus().getRows();
    return context.runtime.newFixnum(rows);
  }

  @JRubyMethod
  public IRubyObject old_value(ThreadContext context) {
    return context.nil;
  }

  /******     PG::Result INSTANCE METHODS: other     ******/

  @JRubyMethod
  public IRubyObject values(ThreadContext context) {
    int len = res.getRows().size();
    RubyArray array = context.runtime.newArray();
    for(int i = 0; i < len; i++) {
      array.append(rowToArray(context, i));
    }
    return array;
  }

  @JRubyMethod(name = "[]", required = 1)
  public IRubyObject op_aref(ThreadContext context, IRubyObject row) {
    int index = (int)((RubyFixnum) row).getLongValue();
    return rowToHash(context, index);
  }

  @JRubyMethod
  public IRubyObject each(ThreadContext context, Block block) {
    for(int i = 0; i < res.getRows().size(); i++) {
      block.yield(context, rowToHash(context, i));
    }
    return context.nil;
  }

  @JRubyMethod
  public IRubyObject each_row(ThreadContext context, Block block) {
    for(int i = 0; i < res.getRows().size(); i++) {
      block.yield(context, rowToArray(context, i));
    }
    return context.nil;
  }

  @JRubyMethod
  public IRubyObject fields(ThreadContext context) {
    RowDescription description = res.getDescription();
    if(description == null) {
      return context.runtime.newArray();
    }
    Column[] columns = description.getColumns();
    RubyArray fields = context.runtime.newArray(columns.length);
    for(int i = 0; i < columns.length; i++) {
      fields.append(context.runtime.newString(columns[i].getName()));
    }
    return fields;
  }

  @JRubyMethod(required = 1, argTypes = {RubyFixnum.class})
  public IRubyObject column_values(ThreadContext context, IRubyObject index) {
    if(!(index instanceof RubyFixnum)) {
      throw context.runtime.newTypeError("argument should be a Fixnum");
    }

    int column = (int)((RubyFixnum) index).getLongValue();

    List<DataRow> rows = res.getRows();
    if(rows.size() > 0 && column >= rows.get(0).getValues().length) {
      throw context.runtime.newIndexError("column " + column + " is out of range");
    }
    RubyArray array = context.runtime.newArray();
    for(int i = 0; i < rows.size(); i++) {
      array.append(valueAsString(context, i, column));
    }
    return array;
  }

  @JRubyMethod(required = 1, argTypes = {RubyString.class})
  public IRubyObject field_values(ThreadContext context, IRubyObject name) {
    if(!(name instanceof RubyString)) {
      throw context.runtime.newTypeError("argument isn't a string");
    }

    String fieldName = ((RubyString) name).asJavaString();
    Column[] columns = res.getDescription().getColumns();
    for(int j = 0; j < columns.length; j++) {
      if(columns[j].getName().equals(fieldName)) {
        RubyArray array = context.runtime.newArray();
        for(int i = 0; i < res.getRows().size(); i++) {
          array.append(valueAsString(context, i, j));
        }
        return array;
      }
    }
    throw context.runtime.newIndexError("Unknown column " + fieldName);
  }

  private RubyArray rowToArray(ThreadContext context, int rowIndex) {
    List<DataRow> rows = res.getRows();
    if(rowIndex >= rows.size()) {
      throw context.runtime.newIndexError("row " + rowIndex);
    }

    RubyArray array = context.runtime.newArray();

    for(int i = 0; i < rows.get(rowIndex).getValues().length; i++) {
      IRubyObject value = valueAsString(context, rowIndex, i);
      array.append(value);
    }
    return array;
  }

  private RubyHash rowToHash(ThreadContext context, int rowIndex) {
    List<DataRow> rows = res.getRows();
    Column[] columns = res.getDescription().getColumns();
    if(rowIndex < 0 || rowIndex >= rows.size()) {
      throw context.runtime.newIndexError("row " + rowIndex + " is out of range");
    }

    RubyHash hash = new RubyHash(context.runtime);

    for(int i = 0; i < rows.get(rowIndex).getValues().length; i++) {
      IRubyObject name = context.runtime.newString(columns[i].getName());
      IRubyObject value = valueAsString(context, rowIndex, i);
      hash.op_aset(context, name, value);
    }
    return hash;
  }

  private IRubyObject valueAsString(ThreadContext context, int row, int column) {
    byte[][] values = res.getRows().get(row).getValues();
    if(values[column] == null) {
      return context.nil;
    }

    byte[] bytes = values[column];

    if(isBinary(column)) {
      return context.runtime.newString(new ByteList(bytes));
    } else {
      return context.runtime.newString(new ByteList(bytes, encoding, false));
    }
  }

  private boolean isBinary(int column) {
    int format = res.getDescription().getColumns()[column].getFormat();
    return Format.isBinary(format);
  }
}
