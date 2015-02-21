package org.jruby.pg.messages;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class ErrorResponse extends BackendMessage {
  public static final String ERRCODE_CANNOT_CONNECT_NOW = "57P03";

  public static enum ErrorField {

    PG_DIAG_SEVERITY('S'),
    PG_DIAG_SQLSTATE('C'),
    PG_DIAG_MESSAGE_PRIMARY('M'),
    PG_DIAG_MESSAGE_DETAIL('D'),
    PG_DIAG_MESSAGE_HINT('H'),
    PG_DIAG_STATEMENT_POSITION('P'),
    PG_DIAG_INTERNAL_POSITION('p'),
    PG_DIAG_INTERNAL_QUERY('q'),
    PG_DIAG_CONTEXT('W'),
    PG_DIAG_SCHEMA_NAME('s'),
    PG_DIAG_TABLE_NAME('t'),
    PG_DIAG_COLUMN_NAME('c'),
    PG_DIAG_DATATYPE_NAME('d'),
    PG_DIAG_CONSTRAINT_NAME('n'),
    PG_DIAG_SOURCE_FILE('F'),
    PG_DIAG_SOURCE_LINE('L'),
    PG_DIAG_SOURCE_FUNCTION('R');

    private byte code;

    private ErrorField(int code) {
      this.code = (byte) code;
    }

    public byte getCode() {
      return code;
    }
  }

  private final Map<Byte, String> fields;
  private final String err;

  // the first byte of each array element is the code followed by the value
  public ErrorResponse(Map<Byte, String> fields, int length) {
    this.fields = fields;

    // construct the error message
    StringWriter sWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(sWriter);
    String fieldVal = null;
    if((fieldVal = getErrorField(ErrorField.PG_DIAG_SEVERITY)) != null) {
      writer.printf("%s: ", fieldVal);
    }
    // only do this for verbose errors
    // if ((fieldVal = getErrorField(PG_DIAG_SQLSTATE)) != null) {
    //   writer.printf("%s: ", fieldVal);
    // }

    if((fieldVal = getErrorField(ErrorField.PG_DIAG_MESSAGE_PRIMARY)) != null) {
      writer.append(fieldVal);
    }

    writer.flush();
    err = sWriter.toString();
  }

  public String getErrorMessage() {
    return err;
  }

  @Override
  public MessageType getType() {
    return MessageType.ErrorResponse;
  }

  public Map<Byte, String> getFields() {
    return fields;
  }

  public String getErrorField(ErrorField field) {
    return fields.get(field.code);
  }

  public boolean isFatal() {
    String string = fields.get('S');
    return string != null && string.equals("FATAL");
  }
}
