package org.jruby.pg.messages;

import java.util.HashMap;
import java.util.Map;

public class ProtocolMessageParser {
  public static ProtocolMessage parseMessage(ProtocolReader reader) {
    if(!reader.hasCompleteMessage()) {
      return null;
    }

    byte type = reader.getByte();
    int len = reader.getInt();

    switch(type) {
    case 'R':
      switch(reader.getInt()) {
      case 0:
        return new AuthenticationOk();
      case 2:
        return new AuthenticationKerbosV5();
      case 3:
        return new AuthenticationCleartextPassword();
      case 4:
        byte[] salt = reader.getNChar(2);
        return new AuthenticationCryptPassword(salt);
      case 5:
        byte[] md5Salt = reader.getNChar(4);
        return new AuthenticationMD5Password(md5Salt);
      case 6:
        return new AuthenticationSCMCredential();
      default:
        throw new IllegalArgumentException("Unknown authentication type");
      }

    case 'S':
      return new ParameterStatus(reader.getString(), reader.getString());

    case 'T':
      int numberOfColumns = reader.getShort();
      Column[] columns = new Column[numberOfColumns];
      for(int i = 0; i < numberOfColumns; i++) {
        String name = reader.getString();
        int tableOid = reader.getInt();
        int tableIndex = reader.getShort();
        int oid = reader.getInt();
        int size = reader.getShort();
        int typmod = reader.getInt();
        int format = reader.getShort();
        columns[i] = new Column(name, tableOid, tableIndex, oid, size, typmod, format);
      }
      return new RowDescription(columns, len);

    case 'D':
      int numberOfDataColumns = reader.getShort();
      byte[][] data = new byte[numberOfDataColumns][];
      for(int i = 0; i < numberOfDataColumns; i++) {
        int byteLength = reader.getInt();
        if(byteLength == -1) {
          data[i] = null;
        } else {
          // create a new ByteBuffer that will share with `buf' the
          // bytes constituting the value of the ith column
          data[i] = reader.getNChar(byteLength);
        }
      }
      return new DataRow(data, len);

    case 'K':
      int pid = reader.getInt();
      int secret = reader.getInt();
      return new BackendKeyData(pid, secret);

    case 'E':
    case 'N':
      byte code;
      Map<Byte, String> fields = new HashMap<Byte, String>();
      while((code = reader.getByte()) != '\0') {
        fields.put(code, reader.getString());
      }
      if(type == 'E') {
        return new ErrorResponse(fields, len);
      } else {
        return new NoticeResponse(fields, len);
      }

    case 'C':
      return new CommandComplete(reader.getString());

    case 't':
      short length = reader.getShort();
      int [] oids = new int[length];
      for(int i = 0; i < length; i++) {
        oids[i] = reader.getInt();
      }
      return new ParameterDescription(oids, len);

    case '1':
      return new ParseComplete();

    case '2':
      return new BindComplete();

    case 'A':
      pid = reader.getInt();
      return new NotificationResponse(pid, reader.getString(), reader.getString());

    case 'G':
      Format overallFormat = Format.isBinary(reader.getByte()) ? Format.Binary : Format.Text;
      short numberOfFormats = reader.getShort();
      Format [] formats = new Format[numberOfFormats];
      for(int i = 0; i < numberOfFormats; i++) {
        formats[i] = Format.isBinary(reader.getShort()) ? Format.Binary : Format.Text;
      }
      return new CopyInResponse(overallFormat, formats);

    case 'H':
      overallFormat = Format.isBinary(reader.getByte()) ? Format.Binary : Format.Text;
      numberOfFormats = reader.getShort();
      formats = new Format[numberOfFormats];
      for(int i = 0; i < numberOfFormats; i++) {
        formats[i] = Format.isBinary(reader.getShort()) ? Format.Binary : Format.Text;
      }
      return new CopyOutResponse(overallFormat, formats);

    case 'd':
      return new CopyData(reader.getNChar(len - 4));

    case 'c':
      return new CopyDone();

    case 'n':
      return new NoData();

    case 'Z':
      byte transactionStatus = reader.getByte();
      return new ReadyForQuery(TransactionStatus.fromByte(transactionStatus), len);

    default:
      throw new IllegalArgumentException(
        "Cannot translate buffer to message for type '" + ((char) type) + "'");
    }
  }
}
