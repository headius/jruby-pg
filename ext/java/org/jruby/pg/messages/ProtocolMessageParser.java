package org.jruby.pg.messages;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ProtocolMessageParser {
  public static ProtocolMessage parseMessage(ByteBuffer buf) {
    if(buf.remaining() < 5) {
      return null;
    }

    buf.mark();
    byte type = buf.get();
    int len = buf.getInt();
    if(buf.remaining() < len - 4) {
      // there isn't enough data, return for now
      buf.reset();
      return null;
    }

    switch(type) {
    case 'R':
      switch(buf.getInt()) {
      case 0:
        return new AuthenticationOk();
      case 2:
        return new AuthenticationKerbosV5();
      case 3:
        return new AuthenticationCleartextPassword();
      case 4:
        byte[] salt = new byte[2];
        buf.get(salt);
        return new AuthenticationCryptPassword(salt);
      case 5:
        byte[] md5Salt = new byte[4];
        buf.get(md5Salt);
        return new AuthenticationMD5Password(md5Salt);
      case 6:
        return new AuthenticationSCMCredential();
      default:
        throw new IllegalArgumentException("Unknown authentication type");
      }

    case 'S':
      ByteBuffer _parameterName = ByteUtils.getNullTerminatedBytes(buf);
      ByteBuffer _parameterValue = ByteUtils.getNullTerminatedBytes(buf);
      String parameterName = ByteUtils.byteBufferToString(_parameterName);
      String parameterValue = ByteUtils.byteBufferToString(_parameterValue);
      return new ParameterStatus(parameterName, parameterValue);

    case 'T':
      int numberOfColumns = buf.getShort();
      Column[] columns = new Column[numberOfColumns];
      for(int i = 0; i < numberOfColumns; i++) {
        ByteBuffer _name = ByteUtils.getNullTerminatedBytes(buf);
        String name = ByteUtils.byteBufferToString(_name);
        int tableOid = buf.getInt();
        int tableIndex = buf.getShort();
        int oid = buf.getInt();
        int size = buf.getShort();
        int typmod = buf.getInt();
        int format = buf.getShort();
        columns[i] = new Column(name, tableOid, tableIndex, oid, size, typmod, format);
      }
      return new RowDescription(columns, buf.capacity() + 4);

    case 'D':
      int numberOfDataColumns = buf.getShort();
      ByteBuffer[] data = new ByteBuffer[numberOfDataColumns];
      for(int i = 0; i < numberOfDataColumns; i++) {
        int byteLength = buf.getInt();
        if(byteLength == -1) {
          data[i] = null;
        } else {
          // create a new ByteBuffer that will share with `buf' the
          // bytes constituting the value of the ith column
          ByteBuffer newBuf = ((ByteBuffer)buf.slice().limit(byteLength)).slice();
          buf.position(buf.position() + byteLength);
          data[i] = ByteBuffer.allocate(byteLength);
          data[i].put(newBuf);
          data[i].flip();
        }
      }
      return new DataRow(data, buf.capacity());

    case 'K':
      int pid = buf.getInt();
      int secret = buf.getInt();
      return new BackendKeyData(pid, secret);

    case 'E':
    case 'N':
      byte code;
      Map<Byte, String> fields = new HashMap<Byte, String>();
      while((code = buf.get()) != '\0') {
        ByteBuffer value = ByteUtils.getNullTerminatedBytes(buf);
        fields.put(code, ByteUtils.byteBufferToString(value));
      }
      if(type == 'E') {
        return new ErrorResponse(fields, buf.capacity() + 4);
      } else {
        return new NoticeResponse(fields, buf.capacity() + 4);
      }

    case 'C':
      ByteBuffer buffer = ByteUtils.getNullTerminatedBytes(buf);
      return new CommandComplete(ByteUtils.byteBufferToString(buffer));

    case 't':
      short length = buf.getShort();
      int [] oids = new int[length];
      for(int i = 0; i < length; i++) {
        oids[i] = buf.getInt();
      }
      return new ParameterDescription(oids, buf.capacity());

    case '1':
      return new ParseComplete();

    case '2':
      return new BindComplete();

    case 'A':
      pid = buf.getInt();
      String condition = ByteUtils.byteBufferToString(ByteUtils.getNullTerminatedBytes(buf));
      ByteBuffer payloadBytes = ByteUtils.getNullTerminatedBytes(buf);
      String payload;
      if(payloadBytes.remaining() == 0) {
        payload = null;
      } else {
        payload = ByteUtils.byteBufferToString(payloadBytes);
      }
      return new NotificationResponse(pid, condition, payload);

    case 'G':
      Format overallFormat = Format.isBinary(buf.get()) ? Format.Binary : Format.Text;
      short numberOfFormats = buf.getShort();
      Format [] formats = new Format[numberOfFormats];
      for(int i = 0; i < numberOfFormats; i++) {
        formats[i] = Format.isBinary(buf.getShort()) ? Format.Binary : Format.Text;
      }
      return new CopyInResponse(overallFormat, formats);

    case 'H':
      overallFormat = Format.isBinary(buf.get()) ? Format.Binary : Format.Text;
      numberOfFormats = buf.getShort();
      formats = new Format[numberOfFormats];
      for(int i = 0; i < numberOfFormats; i++) {
        formats[i] = Format.isBinary(buf.getShort()) ? Format.Binary : Format.Text;
      }
      return new CopyOutResponse(overallFormat, formats);

    case 'd':
      byte[] copy = new byte[len - 4];
      buf.get(copy);
      return new CopyData(copy);

    case 'c':
      return new CopyDone();

    case 'n':
      return new NoData();

    case 'Z':
      byte transactionStatus = buf.get();
      return new ReadyForQuery(TransactionStatus.fromByte(transactionStatus),
                               buf.capacity() + 4);

    default:
      throw new IllegalArgumentException(
        "Cannot translate buffer to message for type '" + ((char) type) + "'");
    }
  }
}
