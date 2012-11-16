
package org.jruby.pg;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyEncoding;
import org.jruby.RubyException;
import org.jruby.RubyFixnum;
import org.jruby.RubyFloat;
import org.jruby.RubyHash;
import org.jruby.RubyIO;
import org.jruby.RubyModule;
import org.jruby.RubyNumeric;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.pg.internal.ConnectionState;
import org.jruby.pg.internal.LargeObjectAPI;
import org.jruby.pg.internal.PostgresqlConnection;
import org.jruby.pg.internal.PostgresqlException;
import org.jruby.pg.internal.ResultSet;
import org.jruby.pg.internal.Value;
import org.jruby.pg.internal.messages.CopyData;
import org.jruby.pg.internal.messages.Format;
import org.jruby.pg.internal.messages.NotificationResponse;
import org.jruby.runtime.Arity;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

public class Connection extends RubyObject {
    private final static Pattern ENCODING_PATTERN = Pattern.compile("(?i).*set\\s+client_encoding\\s+(?:TO|=)\\s+'?(\\S+)'?.*");
    private final static Map<String, String> postgresEncodingToRubyEncoding = new HashMap<String, String>();
    protected final static int FORMAT_TEXT = 0;
    protected final static int FORMAT_BINARY = 1;

    protected static Connection LAST_CONNECTION = null;

    protected PostgresqlConnection postgresqlConnection;
    protected org.jcodings.Encoding encoding;
    protected IRubyObject rubyEncoding;
    protected Map<String, org.jruby.pg.PgPreparedStatement> preparedQueries = new HashMap<String, org.jruby.pg.PgPreparedStatement>();

    static {
      postgresEncodingToRubyEncoding.put("BIG5",          "Big5"        );
      postgresEncodingToRubyEncoding.put("EUC_CN",        "GB2312"      );
      postgresEncodingToRubyEncoding.put("EUC_JP",        "EUC-JP"      );
      postgresEncodingToRubyEncoding.put("EUC_JIS_2004",  "EUC-JP"      );
      postgresEncodingToRubyEncoding.put("EUC_KR",        "EUC-KR"      );
      postgresEncodingToRubyEncoding.put("EUC_TW",        "EUC-TW"      );
      postgresEncodingToRubyEncoding.put("GB18030",       "GB18030"     );
      postgresEncodingToRubyEncoding.put("GBK",           "GBK"         );
      postgresEncodingToRubyEncoding.put("ISO_8859_5",    "ISO-8859-5"  );
      postgresEncodingToRubyEncoding.put("ISO_8859_6",    "ISO-8859-6"  );
      postgresEncodingToRubyEncoding.put("ISO_8859_7",    "ISO-8859-7"  );
      postgresEncodingToRubyEncoding.put("ISO_8859_8",    "ISO-8859-8"  );
      postgresEncodingToRubyEncoding.put("KOI8",          "KOI8-R"      );
      postgresEncodingToRubyEncoding.put("KOI8R",         "KOI8-R"      );
      postgresEncodingToRubyEncoding.put("KOI8U",         "KOI8-U"      );
      postgresEncodingToRubyEncoding.put("LATIN1",        "ISO-8859-1"  );
      postgresEncodingToRubyEncoding.put("LATIN2",        "ISO-8859-2"  );
      postgresEncodingToRubyEncoding.put("LATIN3",        "ISO-8859-3"  );
      postgresEncodingToRubyEncoding.put("LATIN4",        "ISO-8859-4"  );
      postgresEncodingToRubyEncoding.put("LATIN5",        "ISO-8859-9"  );
      postgresEncodingToRubyEncoding.put("LATIN6",        "ISO-8859-10" );
      postgresEncodingToRubyEncoding.put("LATIN7",        "ISO-8859-13" );
      postgresEncodingToRubyEncoding.put("LATIN8",        "ISO-8859-14" );
      postgresEncodingToRubyEncoding.put("LATIN9",        "ISO-8859-15" );
      postgresEncodingToRubyEncoding.put("LATIN10",       "ISO-8859-16" );
      postgresEncodingToRubyEncoding.put("MULE_INTERNAL", "Emacs-Mule"  );
      postgresEncodingToRubyEncoding.put("SJIS",          "Windows-31J" );
      postgresEncodingToRubyEncoding.put("SHIFT_JIS_2004","Windows-31J" );
      postgresEncodingToRubyEncoding.put("UHC",           "CP949"       );
      postgresEncodingToRubyEncoding.put("UTF8",          "UTF-8"       );
      postgresEncodingToRubyEncoding.put("WIN866",        "IBM866"      );
      postgresEncodingToRubyEncoding.put("WIN874",        "Windows-874" );
      postgresEncodingToRubyEncoding.put("WIN1250",       "Windows-1250");
      postgresEncodingToRubyEncoding.put("WIN1251",       "Windows-1251");
      postgresEncodingToRubyEncoding.put("WIN1252",       "Windows-1252");
      postgresEncodingToRubyEncoding.put("WIN1253",       "Windows-1253");
      postgresEncodingToRubyEncoding.put("WIN1254",       "Windows-1254");
      postgresEncodingToRubyEncoding.put("WIN1255",       "Windows-1255");
      postgresEncodingToRubyEncoding.put("WIN1256",       "Windows-1256");
      postgresEncodingToRubyEncoding.put("WIN1257",       "Windows-1257");
      postgresEncodingToRubyEncoding.put("WIN1258",       "Windows-1258");
    }

    public Connection(Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);

        encoding = null;
    }

    public static void define(Ruby ruby, RubyModule pg, RubyModule constants) {
        RubyClass connection = pg.defineClassUnder("Connection", ruby.getObject(), CONNECTION_ALLOCATOR);

        connection.includeModule(constants);

        connection.defineAnnotatedMethods(Connection.class);

        connection.getSingletonClass().defineAlias("connect", "new");
        connection.getSingletonClass().defineAlias("open", "new");
        connection.getSingletonClass().defineAlias("setdb", "new");
        connection.getSingletonClass().defineAlias("setdblogin", "new");
    }

    /******     PG::Connection CLASS METHODS     ******/

    @JRubyMethod(alias = {"escape, escape_string"}, meta = true)
    public static IRubyObject escape_literal_native(ThreadContext context, IRubyObject self, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(meta = true, required = 1, argTypes = {RubyArray.class})
    public static IRubyObject escape_bytea(ThreadContext context, IRubyObject self, IRubyObject array) {
      if (LAST_CONNECTION != null)
        return LAST_CONNECTION.escape_bytea(context, array);
      return escapeBytes(context, array, context.nil, false);
    }

    @JRubyMethod(meta = true)
    public static IRubyObject unescape_bytea(ThreadContext context, IRubyObject self, IRubyObject array) {
      return unescapeBytes(context, array);
    }

    @JRubyMethod(meta = true, required = 2, argTypes = {RubyString.class, RubyString.class})
    public static IRubyObject encrypt_password(ThreadContext context, IRubyObject self, IRubyObject password, IRubyObject username) {
      if (username.isNil() || password.isNil())
        throw context.runtime.newTypeError("usernamd ane password cannot be nil");

      try {
        byte[] cryptedPassword = PostgresqlConnection.encrypt(((RubyString) username).getBytes(), ((RubyString) password).getBytes());
        return context.runtime.newString(new ByteList(cryptedPassword));
      } catch (Exception e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    @JRubyMethod(meta = true)
    public static IRubyObject quote_ident(ThreadContext context, IRubyObject self, IRubyObject identifier) {
      return quoteIdentifier(context, identifier);
    }

    @JRubyMethod(rest = true, meta = true)
    public static IRubyObject connect_start(ThreadContext context, IRubyObject self, IRubyObject[] args, Block block) {
      try {
        Connection connection = new Connection(context.runtime, context.runtime.getModule("PG").getClass("Connection"));
        Properties props = parse_args(context, args);
        connection.postgresqlConnection = PostgresqlConnection.connectStart(props);
        if (block.isGiven()) {
          IRubyObject value = block.yield(context, connection);
          connection.finish(context);
          return value;
        }
        return connection;
      } catch (Exception e) {
        throw context.runtime.newIOError(e.getLocalizedMessage());
      }
    }

    @JRubyMethod(meta = true)
    public static IRubyObject reset_last_conn(ThreadContext context, IRubyObject self) {
      LAST_CONNECTION = null;
      return context.nil;
    }

    @JRubyMethod(rest = true, meta = true)
    public static IRubyObject ping(ThreadContext context, IRubyObject self, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(meta = true)
    public static IRubyObject connectdefaults(ThreadContext context, IRubyObject self) {
        return context.nil;
    }

    /**
     * binary data is received from the jdbc driver after being unescaped
     *
     * @param context
     * @param _array
     * @return
     */
    public static IRubyObject unescapeBytes (ThreadContext context, IRubyObject _array) {
      RubyString string = (RubyString) _array;
      byte[] bytes = string.getBytes();
      if (bytes[0] == '\\' && bytes[1] == 'x') {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 2; i < bytes.length; i += 2) {
          int b = charToInt(bytes[i]) * 16 + charToInt(bytes[i + 1]);
          out.write(b);
        }
        return context.runtime.newString(new ByteList(out.toByteArray()));
      } else {
        return _array;
      }
    }

    private static int charToInt(byte b) {
      if (Character.isLetter(b))
        return Character.toUpperCase(b) - 'A' + 10;
      else
        return b - '0';
    }

    private static IRubyObject quoteIdentifier(ThreadContext context, IRubyObject _identifier) {
      RubyString identifier = (RubyString) _identifier;
      byte[] bytes = identifier.getBytes();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write('"');
      for (int i = 0; i < bytes.length; i++) {
        if (bytes[i] == '"')
          out.write('"');
        out.write(bytes[i]);
      }
      out.write('"');
      byte[] newBytes = out.toByteArray();
      return context.runtime.newString(new ByteList(newBytes));
    }

    private static IRubyObject escapeBytes(ThreadContext context, IRubyObject _array, IRubyObject encoding, boolean standardConforminStrings) {
      RubyString array = (RubyString) _array;
      byte[] bytes = array.getBytes();

      return escapeBytes(context, bytes, encoding, standardConforminStrings);
    }

    private static IRubyObject escapeBytes(ThreadContext context, byte[] bytes, IRubyObject encoding, boolean standardConformingStrings) {
      return escapeBytes(context, bytes, 0, bytes.length, encoding, standardConformingStrings);
    }

    private static IRubyObject escapeBytes(ThreadContext context, byte[] bytes, int offset, int len,
        IRubyObject encoding, boolean standardConformingStrings) {
      if (len < 0 || offset < 0 || offset + len > bytes.length) {
        throw context.runtime.newArgumentError("Oops array offset or length isn't correct");
      }

      String prefix = "";
      if (!standardConformingStrings)
        prefix = "\\";

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      PrintWriter writer = new PrintWriter(out);
      for (int i = offset; i < (offset + len); i++) {
        int byteValue= bytes[i] & 0xFF;
        if (byteValue == 39) {
          // escape the single quote
          writer.append(prefix).append("\\047");
        } else if (byteValue == 92) {
          // escape the backslash
          writer.append(prefix).append("\\134");
        } else if (byteValue >= 0 && byteValue <= 31 || byteValue >= 127 && byteValue <= 255) {
          writer.append(prefix).printf("\\%03o", byteValue);
        } else {
          // all other characters, print as themselves
          writer.write(byteValue);
        }
      }

      writer.close();
      byte[] outBytes = out.toByteArray();
      if (encoding.isNil())
        return context.runtime.newString(new ByteList(outBytes));
      return context.runtime.newString(new ByteList(outBytes, ((RubyEncoding) encoding).getEncoding()));
    }

    @SuppressWarnings("unchecked")
    private static Properties parse_args(ThreadContext context, IRubyObject[] args) {
      if (args.length > 7)
        throw context.getRuntime().newArgumentError("extra positional parameter");

      Properties argumentsHash = new Properties();

      int last_index = 0;
      // handle a hash argument first
      if (args.length >= 1 && args[0] instanceof RubyHash) {
        last_index = 1;
        RubyHash hash = (RubyHash)args[0];

        for (Object _entry : hash.entrySet()) {
          Entry<String, Object> entry = (Entry<String, Object>) _entry;
          argumentsHash.put(PostgresHelpers.stringify(entry.getKey()), PostgresHelpers.stringify(entry.getValue()));
        }
      }

      if (args.length == last_index + 1) {
        // handle a string argument
        // we have a connection string, parse it
        String connectionString = args[0].asJavaString();
        String[] options = connectionString.split(" ");
        for (String option : options) {
          String[] keyValuePair = option.split("=");
          if (keyValuePair.length != 2)
            throw context.runtime.newRuntimeError("Connection string doesn't have the right format");
          argumentsHash.put(keyValuePair[0], keyValuePair[1]);
        }
      } else if (args.length > last_index + 1) {
        // assume positional arguments in the following order
        // host, port, options, tty, dbname, user, password
        argumentsHash.put("host", args[last_index].asJavaString());
        if (args.length >= last_index + 1)
          argumentsHash.put("port", ((RubyFixnum) args[last_index + 1]).to_s().toString());
        // FIXME: what is the options argument ?
        // FIXME: what is the tty argument ?
        if (args.length >= last_index + 4 && !args[last_index + 4].isNil())
          argumentsHash.put("dbname", args[last_index + 4].asJavaString());
        if (args.length >= last_index + 5 && !args[last_index + 5].isNil())
          argumentsHash.put("user", args[last_index + 5].asJavaString());
        if (args.length >= last_index + 6 && !args[last_index + 6].isNil())
          argumentsHash.put("password", args[last_index + 6].asJavaString());
      }
      return argumentsHash;
    }

    private static ObjectAllocator CONNECTION_ALLOCATOR = new ObjectAllocator() {
        @Override
        public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
            return new Connection(ruby, rubyClass);
        }
    };

    public RaiseException newPgError(ThreadContext context, String message, ResultSet result, org.jcodings.Encoding encoding) {
      RubyClass klass = context.runtime.getModule("PG").getClass("Error");
      RubyString rubyMessage = context.runtime.newString(message);
      if (encoding != null) {
        RubyEncoding rubyEncoding = RubyEncoding.newEncoding(context.runtime, encoding);
        rubyMessage = (RubyString) rubyMessage.encode(context, rubyEncoding);
      }
      IRubyObject rubyResult = result == null ? context.nil : createResult(context, result, NULL_ARRAY, Block.NULL_BLOCK);
      IRubyObject exception = klass.newInstance(context, rubyMessage, rubyResult, Block.NULL_BLOCK);
      return new RaiseException((RubyException) exception);
    }

    /******     PG::Connection INSTANCE METHODS: Connection Control     ******/

    @JRubyMethod(rest = true, required = 1)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        this.rubyEncoding = context.nil;

        Properties props = parse_args(context, args);

        // to make testing possible
        if (System.getenv("PG_TEST_SSL") != null) {
          props.setProperty("ssl", "require");
        }

        try {
            // connection = (BaseConnection)driver.connect(connectionString, props);
            postgresqlConnection = PostgresqlConnection.connectDb(props);
            // set the encoding if the default internal_encoding is set
            set_default_encoding(context);

            LAST_CONNECTION = this;
        } catch (Exception e) {
            throw context.runtime.newRuntimeError(e.getLocalizedMessage());
        }
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject connect_poll(ThreadContext context) {
      try {
        ConnectionState state = postgresqlConnection.connectPoll();
        return context.runtime.newFixnum(state.ordinal());
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(name = {"finish", "close"})
    public IRubyObject finish(ThreadContext context) {
      try {
        if (postgresqlConnection.closed())
          throw newPgError(context, "connection is closed", null, encoding);
        postgresqlConnection.close();
        return context.nil;
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod
    public IRubyObject status(ThreadContext context) {
      return context.runtime.newFixnum(postgresqlConnection.status().ordinal());
    }

    @JRubyMethod(name = "finished?")
    public IRubyObject finished_p(ThreadContext context) {
        return postgresqlConnection.closed() ? context.runtime.getTrue() : context.runtime.getFalse();
    }

    @JRubyMethod
    public IRubyObject reset(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject reset_start(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject reset_poll(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject conndefaults(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: Connection Status     ******/

    @JRubyMethod
    public IRubyObject db(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject user(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject pass(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject host(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject port(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject tty(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject options(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject transaction_status(ThreadContext context) {
      return context.runtime.newFixnum(postgresqlConnection.getTransactionStatus().getValue());
    }

    @JRubyMethod(required = 1)
    public IRubyObject parameter_status(ThreadContext context, IRubyObject arg0) {
      String name = arg0.asJavaString();
      return context.runtime.newString(postgresqlConnection.getParameterStatus(name));
    }

    @JRubyMethod
    public IRubyObject protocol_version(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject server_version(ThreadContext context) {
      return context.runtime.newFixnum(postgresqlConnection.getServerVersion());
    }

    @JRubyMethod
    public IRubyObject error_message(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject socket(ThreadContext context) {
      SelectableChannel socket = postgresqlConnection.getSocket();
      RubyIO rubyIO = RubyIO.newIO(context.runtime, socket);
      return rubyIO.fileno(context);
    }

    @JRubyMethod
    public IRubyObject backend_pid(ThreadContext context) {
      return context.runtime.newFixnum(postgresqlConnection.getBackendPid());
    }

    @JRubyMethod
    public IRubyObject connection_needs_password(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject connection_used_password(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: Command Execution     ******/

    @JRubyMethod(alias = {"query", "async_exec", "async_query"}, required = 1, optional = 2)
    public IRubyObject exec(ThreadContext context, IRubyObject[] args, Block block) {
        String query = args[0].convertToString().toString();
        ResultSet set = null;
        try {
            if (args.length == 1) {
              set = postgresqlConnection.exec(query);
            } else {

              RubyArray params = (RubyArray) args[1];

              Value [] values = new Value[params.getLength()];
              int [] oids = new int[params.getLength()];
              fillValuesAndFormat(context, params, values, oids);
              Format resultFormat = getFormat(context, args);
              set = postgresqlConnection.execQueryParams(query, values, resultFormat, oids);
            }

            Matcher matcher = ENCODING_PATTERN.matcher(query);
            if (matcher.matches()) {
              internal_encoding_set(context, context.runtime.newString(matcher.group(1)));
            }

            if (set == null)
              return context.nil;
        } catch (PostgresqlException e) {
          throw newPgError(context, e.getLocalizedMessage(), e.getResultSet(), encoding);
        } catch (Exception sqle) {
            throw newPgError(context, sqle.getLocalizedMessage(), null, encoding);
        }

        return createResult(context, set, args, block);
    }

    private Format getFormat(ThreadContext context, IRubyObject [] args) {
      Format resultFormat = Format.Text;
      if (args.length == 3)
        resultFormat = ((RubyFixnum) args[2]).getLongValue() == 1 ? Format.Binary : Format.Text;
      return resultFormat;
    }

    private void fillValuesAndFormat(ThreadContext context, RubyArray params, Value[] values, int [] oids) {
      RubySymbol value_s = context.runtime.newSymbol("value");
      RubySymbol type_s = context.runtime.newSymbol("type");
      RubySymbol format_s = context.runtime.newSymbol("format");
      for (int i = 0; i < params.getLength(); i++) {
        IRubyObject param = params.entry(i);
        Format valueFormat = Format.Text;
        if (param.isNil()) {
          values[i] = new Value(null, valueFormat);
        } else if (param instanceof RubyHash) {
          RubyHash hash = (RubyHash) params.get(i);
          IRubyObject value = hash.op_aref(context, value_s);
          IRubyObject type = hash.op_aref(context, type_s);
          IRubyObject format = hash.op_aref(context, format_s);
          if (!type.isNil())
            oids[i] = (int) ((RubyFixnum) type).getLongValue();
          if (!format.isNil())
            valueFormat = ((RubyFixnum) format).getLongValue() == 1 ? Format.Binary : Format.Text;
          if (value.isNil())
            values[i] = new Value(null, valueFormat);
          else
            values[i] = new Value(((RubyString) value).getBytes(), valueFormat);
        } else {
          RubyString rubyString;
          if (param instanceof RubyString)
            rubyString = (RubyString) param;
          else
            rubyString = (RubyString) ((RubyObject) param).to_s();
          values[i] = new Value(rubyString.getBytes(), valueFormat);
        }
      }
    }

    private IRubyObject createResult(ThreadContext context, ResultSet set, IRubyObject [] args, Block block) {
      // by default we return results in text format
      boolean binary = false;
      if (args.length == 3)
        binary = ((RubyFixnum) args[2]).getLongValue() == FORMAT_BINARY;

      Result result = new Result(context.runtime, (RubyClass)context.runtime.getClassFromPath("PG::Result"), this, set, encoding, binary);
      if (block.isGiven())
        return block.call(context, result);
      return result;
    }

    @JRubyMethod(required = 2, rest = true)
    public IRubyObject prepare(ThreadContext context, IRubyObject[] args) {
      try {
        String name = args[0].asJavaString();
        String query = args[1].asJavaString();
        int [] oids = null;
        if (args.length == 3) {
          RubyArray array = ((RubyArray) args[2]);
          oids = new int[array.getLength()];
          for (int i = 0; i < oids.length; i++)
            oids[i] = (int) ((RubyFixnum) array.get(i)).getLongValue();
        }
        oids = oids == null ? new int [0] : oids;
        ResultSet result = postgresqlConnection.prepare(name, query, oids);
        return createResult(context, result, NULL_ARRAY, Block.NULL_BLOCK);
      } catch (PostgresqlException e) {
        throw newPgError(context, e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(required = 1, optional = 2)
    public IRubyObject exec_prepared(ThreadContext context, IRubyObject[] args, Block block) {
      try {
        ResultSet set = execPreparedCommon(context, args, false);
        return createResult(context, set, args, block);
      } catch (PostgresqlException e) {
        throw newPgError(context, e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    private ResultSet execPreparedCommon(ThreadContext context, IRubyObject[] args, boolean async) throws IOException, PostgresqlException {
      String queryName = args[0].asJavaString();
      Value[] values;
      int[] oids;
      if (args.length > 1) {
        RubyArray array = (RubyArray) args[1];
        values = new Value[array.getLength()];
        oids = new int[array.getLength()];
        fillValuesAndFormat(context, array, values, oids);
      } else {
        values = new Value[0];
        oids = new int[0];
      }
      Format format = getFormat(context, args);
      if (!async)
        return postgresqlConnection.execPrepared(queryName, values, format);
      postgresqlConnection.sendExecPrepared(queryName, values, format);
      return null;
    }

    @JRubyMethod(required = 1)
    public IRubyObject describe_prepared(ThreadContext context, IRubyObject query_name) {
      try {
        ResultSet resultSet = postgresqlConnection.describePrepared(query_name.asJavaString());
        return createResult(context, resultSet, NULL_ARRAY, Block.NULL_BLOCK);
      } catch (PostgresqlException e) {
        throw newPgError(context, e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(required = 1)
    public IRubyObject describe_portal(ThreadContext context, IRubyObject arg0) {
      try {
        String name = arg0.asJavaString();
        ResultSet resultSet = postgresqlConnection.describePortal(name);
        return createResult(context, resultSet, NULL_ARRAY, Block.NULL_BLOCK);
      } catch (PostgresqlException e) {
        throw newPgError(context, e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod
    public IRubyObject make_empty_pgresult(ThreadContext context, IRubyObject arg0) {
      return createResult(context, new ResultSet(), NULL_ARRAY, Block.NULL_BLOCK);
    }

    @JRubyMethod(alias = {"escape_string", "escape"}, required = 1, argTypes = {RubyString.class} )
    public IRubyObject escape_literal_native(ThreadContext context, IRubyObject _str) {
      RubyString str = (RubyString) _str;
      byte[] bytes = str.getBytes();
      int i;
      for (i = 0; i < bytes.length && bytes[i] != '\0'; i++);
      return escapeBytes(context, bytes, 0, i, rubyEncoding, postgresqlConnection.getStandardConformingStrings());
    }

    @JRubyMethod
    public IRubyObject escape_bytea(ThreadContext context, IRubyObject array) {
      return escapeBytes(context, array, rubyEncoding, postgresqlConnection.getStandardConformingStrings());
    }

    @JRubyMethod
    public IRubyObject unescape_bytea(ThreadContext context, IRubyObject array) {
      return unescapeBytes(context, array);
    }

    /******     PG::Connection INSTANCE METHODS: Asynchronous Command Processing     ******/

    @JRubyMethod(rest = true)
    public IRubyObject send_query(ThreadContext context, IRubyObject[] args) {
      try {
        if (args.length == 1) {
          String query = args[0].asJavaString();
          postgresqlConnection.sendQuery(query);
        }
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
      return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject send_prepare(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject send_query_prepared(ThreadContext context, IRubyObject[] args) {
      try {
        execPreparedCommon(context, args, true);
        return context.nil;
      } catch (PostgresqlException e) {
        throw newPgError(context, e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod
    public IRubyObject send_describe_prepared(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject send_describe_portal(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject get_result(ThreadContext context, Block block) {
      try {
        ResultSet set = postgresqlConnection.getResult();
        return createResult(context, set, NULL_ARRAY, block);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod
    public IRubyObject consume_input(ThreadContext context) {
      try {
        postgresqlConnection.consumeInput();
        return context.nil;
      } catch (IOException e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod
    public IRubyObject is_busy(ThreadContext context) {
      return context.runtime.newBoolean(postgresqlConnection.isBusy());
    }

    @JRubyMethod
    public IRubyObject set_nonblocking(ThreadContext context, IRubyObject arg0) {
      if (arg0.isTrue())
        postgresqlConnection.setNonBlocking(true);
      postgresqlConnection.setNonBlocking(false);
      return arg0;
    }

    @JRubyMethod(name = {"isnonblocking", "nonblocking?"})
    public IRubyObject isnonblocking(ThreadContext context) {
      return context.runtime.newBoolean(postgresqlConnection.isNonBlocking());
    }

    @JRubyMethod
    public IRubyObject flush(ThreadContext context) {
      try {
        return context.runtime.newBoolean(postgresqlConnection.flush());
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    /******     PG::Connection INSTANCE METHODS: Cancelling Queries in Progress     ******/

    @JRubyMethod
    public IRubyObject cancel(ThreadContext context) {
      try {
        postgresqlConnection.cancel();
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
      return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: NOTIFY     ******/

    @JRubyMethod
    public IRubyObject notifies(ThreadContext context) {
      NotificationResponse notification = postgresqlConnection.notifications();
      if (notification == null)
        return context.nil;
      RubyHash hash = new RubyHash(context.runtime);

      RubySymbol relname = context.runtime.newSymbol("relname");
      RubySymbol pid = context.runtime.newSymbol("be_pid");
      RubySymbol extra = context.runtime.newSymbol("extra");

      hash.op_aset(context, relname, context.runtime.newString(notification.getCondition()));
      hash.op_aset(context, pid, context.runtime.newFixnum(notification.getPid()));
      hash.op_aset(context, extra, context.runtime.newString(notification.getPayload()));

      return hash;
    }

    /******     PG::Connection INSTANCE METHODS: COPY     ******/

    @JRubyMethod
    public IRubyObject put_copy_data(ThreadContext context, IRubyObject arg0) {
      try {
        byte[] bytes = ((RubyString) arg0).getBytes();
        ByteBuffer data = ByteBuffer.wrap(bytes);
        return context.runtime.newBoolean(postgresqlConnection.putCopyData(data));
      } catch (IOException e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(rest = true)
    public IRubyObject put_copy_end(ThreadContext context, IRubyObject[] args) {
      try {
        return context.runtime.newBoolean(postgresqlConnection.putCopyDone());
      } catch (IOException e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(rest = true)
    public IRubyObject get_copy_data(ThreadContext context, IRubyObject[] args) {
      try {
        boolean async = false;
        if (args.length == 1)
          async = args[0].isTrue();
        CopyData data = postgresqlConnection.getCopyData(async);
        if (data == PostgresqlConnection.COPY_DATA_NOT_READY)
          return context.runtime.getFalse();
        else if (data == null)
          return context.nil;
        ByteBuffer value = data.getValue();
        return context.runtime.newString(new ByteList(value.array(), value.arrayOffset() + value.position(), value.remaining()));
      } catch (IOException e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    /******     PG::Connection INSTANCE METHODS: Control Functions     ******/

    @JRubyMethod
    public IRubyObject set_error_visibility(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject trace(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject untrace(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: Notice Processing     ******/

    @JRubyMethod
    public IRubyObject set_notice_receiver(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject set_notice_processor(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: Other    ******/

    @JRubyMethod()
    public IRubyObject transaction(ThreadContext context, Block block) {
      if (!block.isGiven())
        throw context.runtime.newArgumentError("Must supply block for PG::Connection#transaction");

      try {
        try {
          postgresqlConnection.exec("BEGIN");
          if (block.arity() == Arity.NO_ARGUMENTS)
            block.yieldSpecific(context);
          else
            block.yieldSpecific(context, this);
          postgresqlConnection.exec("COMMIT");
        } catch (RuntimeException ex) {
          postgresqlConnection.exec("ROLLBACK");
          throw ex;
        }
      } catch (Exception e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
      return context.nil;
    }

    @JRubyMethod(optional = 1)
    public IRubyObject block(ThreadContext context, IRubyObject[] args) {
      try {
        if (args.length == 0)
          postgresqlConnection.block();
        else {
          RubyFloat timeout = ((RubyNumeric) args[0]).convertToFloat();
          int timeoutMs = (int) (timeout.getDoubleValue() * 1000);
          postgresqlConnection.block(timeoutMs);
        }
        return context.nil;
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(name = {"wait_for_notify", "notifies_wait"}, optional = 1)
    public IRubyObject wait_for_notify(ThreadContext context, IRubyObject[] args, Block block) {
      try {
        NotificationResponse notification = postgresqlConnection.waitForNotify();
        if (block.isGiven()) {
          if (block.arity() == Arity.NO_ARGUMENTS) return block.call(context);
          RubyString condition = context.runtime.newString(notification.getCondition());
          RubyFixnum pid = context.runtime.newFixnum(notification.getPid());
          String javaPayload = notification.getPayload();
          IRubyObject payload = javaPayload == null ? context.nil : context.runtime.newString(javaPayload);
          if (!block.arity().isFixed()) {
            return block.call(context, condition, pid, payload);
          } else if (block.arity().required() == 2) {
            return block.call(context, condition, pid);
          } else if (block.arity().required() == 3) {
            return block.call(context, condition, pid, payload);
          }
          throw context.runtime.newArgumentError("Expected a block with arity 2 or 3");
        } else {
          return context.runtime.newString(notification.getCondition());
        }
      } catch (IOException e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(required = 1)
    public IRubyObject quote_ident(ThreadContext context, IRubyObject identifier) {
      return quoteIdentifier(context, identifier);
    }

    @JRubyMethod
    public IRubyObject get_last_result(ThreadContext context) {
      try {
        ResultSet set = postgresqlConnection.getLastResult();
        return createResult(context, set, NULL_ARRAY, Block.NULL_BLOCK);
      } catch (Exception e) {
        throw newPgError(context, e.getLocalizedMessage(), null, encoding);
      }
    }

    /******     PG::Connection INSTANCE METHODS: Large Object Support     ******/

    @JRubyMethod(name = {"lo_creat", "locreat"}, optional = 1)
    public IRubyObject lo_creat(ThreadContext context, IRubyObject[] args) {
      try {
        LargeObjectAPI manager = postgresqlConnection.getLargeObjectAPI();
        int oid;
        if (args.length == 1)
          oid = manager.loCreat((Integer) args[0].toJava(Integer.class));
        else
          oid = manager.loCreat(0);
        return new RubyFixnum(context.runtime, oid);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (IOException e) {
        throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(name = {"lo_create", "locreate"})
    public IRubyObject lo_create(ThreadContext context, IRubyObject arg0) {
      try {
        LargeObjectAPI manager = postgresqlConnection.getLargeObjectAPI();
        int oid = manager.loCreate((Integer) arg0.toJava(Integer.class));
        return new RubyFixnum(context.runtime, oid);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (IOException e) {
        throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(name = {"lo_import", "loimport"})
    public IRubyObject lo_import(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_export", "loexport"})
    public IRubyObject lo_export(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_open", "loopen"}, required = 1, optional = 1)
    public IRubyObject lo_open(ThreadContext context, IRubyObject [] args) {
      try {
        int fd;
        long oidLong = (Long) args[0].toJava(Long.class);
        if (args.length == 1)
          fd = postgresqlConnection.getLargeObjectAPI().loOpen((int) oidLong);
        else
          fd = postgresqlConnection.getLargeObjectAPI().loOpen((int) oidLong, (Integer) args[1].toJava(Integer.class));

        return context.runtime.newFixnum(fd);
      } catch (IOException e) {
        throw newPgError(context, "lo_open failed: " + e.getLocalizedMessage(), null, encoding);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_open failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_write", "lowrite"}, required = 2, argTypes = {RubyFixnum.class, RubyString.class})
    public IRubyObject lo_write(ThreadContext context, IRubyObject object, IRubyObject buffer) {
      try {
        long fd = ((RubyFixnum) object).getLongValue();
        RubyString bufferString = (RubyString) buffer;
        int count = postgresqlConnection.getLargeObjectAPI().loWrite((int) fd, bufferString.getBytes());
        return context.runtime.newFixnum(count);
      } catch (IOException e) {
        throw newPgError(context, "lo_write failed: " + e.getLocalizedMessage(), null, encoding);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_write failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_read", "loread"}, required = 2, argTypes = {RubyFixnum.class, RubyFixnum.class})
    public IRubyObject lo_read(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
      try {
        int fd = (int) ((RubyFixnum) arg0).getLongValue();
        int count = (int) ((RubyFixnum) arg1).getLongValue();
        byte[] b = postgresqlConnection.getLargeObjectAPI().loRead(fd, count);
        if (b.length == 0)
          return context.nil;
        return context.runtime.newString(new ByteList(b));
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_read failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (IOException e) {
        throw newPgError(context, "lo_read failed: " + e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(name = {"lo_lseek", "lolseek", "lo_seek", "loseek"}, required = 3,
        argTypes = {RubyFixnum.class, RubyFixnum.class, RubyFixnum.class})
    public IRubyObject lo_lseek(ThreadContext context, IRubyObject _fd, IRubyObject _offset, IRubyObject _whence) {
      try {
        int offset = (int) ((RubyFixnum) _offset).getLongValue();
        int fd = (int) ((RubyFixnum) _fd).getLongValue();
        int whence = (int) ((RubyFixnum) _whence).getLongValue();
        int where = postgresqlConnection.getLargeObjectAPI().loSeek(fd, offset, whence);
        return new RubyFixnum(context.runtime, where);
      } catch (IOException e) {
        throw newPgError(context, "lo_lseek failed: " + e.getLocalizedMessage(), null, encoding);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_lseek failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_tell", "lotell"}, required = 1, argTypes = {RubyFixnum.class})
    public IRubyObject lo_tell(ThreadContext context, IRubyObject _fd) {
      try {
        int fd = (int) ((RubyFixnum) _fd).getLongValue();
        int where = postgresqlConnection.getLargeObjectAPI().loTell(fd);
        return context.runtime.newFixnum(where);
      } catch (IOException e) {
        throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), null, encoding);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_truncate", "lotruncate"}, required = 2, argTypes = {RubyFixnum.class, RubyFixnum.class})
    public IRubyObject lo_truncate(ThreadContext context, IRubyObject _fd, IRubyObject _len) {
      try {
        int fd = (int) ((RubyFixnum) _fd).getLongValue();
        int len = (int) ((RubyFixnum) _len).getLongValue();
        int value = postgresqlConnection.getLargeObjectAPI().loTruncate(fd, len);
        return context.runtime.newFixnum(value);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      } catch (IOException e) {
        throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), null, encoding);
      }
    }

    @JRubyMethod(name = {"lo_close", "loclose"}, required = 1, argTypes = {RubyFixnum.class})
    public IRubyObject lo_close(ThreadContext context, IRubyObject _fd) {
      try {
        int fd = (int) ((RubyFixnum) _fd).getLongValue();
        int value = postgresqlConnection.getLargeObjectAPI().loClose(fd);
        return context.runtime.newFixnum(value);
      } catch (IOException e) {
        throw newPgError(context, "lo_close failed: " + e.getLocalizedMessage(), null, encoding);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_close failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_unlink", "lounlink"}, required = 1, argTypes = {RubyFixnum.class})
    public IRubyObject lo_unlink(ThreadContext context, IRubyObject _fd) {
      try {
        int fd = (int) ((RubyFixnum) _fd).getLongValue();
        int value = postgresqlConnection.getLargeObjectAPI().loUnlink(fd);
        return context.runtime.newFixnum(value);
      } catch (IOException e) {
        throw newPgError(context, "lo_unlink failed: " + e.getLocalizedMessage(), null, encoding);
      } catch (PostgresqlException e) {
        throw newPgError(context, "lo_unlink failed: " + e.getLocalizedMessage(), e.getResultSet(), encoding);
      }
    }

    /******     M17N     ******/

    @JRubyMethod(alias = {"client_encoding"})
    public IRubyObject internal_encoding(ThreadContext context) {
      return rubyEncoding;
    }

    @JRubyMethod(name = {"internal_encoding=", "set_client_encoding", "client_encoding=" }, required = 1)
    public IRubyObject internal_encoding_set(ThreadContext context, IRubyObject encoding) {
      IRubyObject rubyEncoding = context.nil;
      if (encoding instanceof RubyString) {
        rubyEncoding = findEncoding(context, encoding);
      } else if (encoding instanceof RubyEncoding) {
        rubyEncoding = encoding;
      }

      if (!rubyEncoding.isNil()) {
        this.encoding = ((RubyEncoding) rubyEncoding).getEncoding();
        this.rubyEncoding = rubyEncoding;
      }

      return rubyEncoding;
    }

    @JRubyMethod
    public IRubyObject external_encoding(ThreadContext context) {
      return context.nil;
    }

    @JRubyMethod
    public IRubyObject set_default_encoding(ThreadContext context) {
      IRubyObject internal_encoding = RubyEncoding.getDefaultInternal(this);
      if (!internal_encoding.isNil()) {
        return internal_encoding_set(context, internal_encoding);
      }
      return internal_encoding;
    }

    private IRubyObject findEncoding(ThreadContext context, String encodingName) {
      return findEncoding(context, context.runtime.newString(encodingName));
    }

    private IRubyObject findEncoding(ThreadContext context, IRubyObject encodingName) {
      try {
        String javaName = encodingName.asJavaString().toUpperCase();
        if (postgresEncodingToRubyEncoding.containsKey(javaName)) {
          String rubyName = postgresEncodingToRubyEncoding.get(javaName);
          return findEncoding(context, rubyName);
        }
        return context.runtime.getClass("Encoding").callMethod("find", encodingName);
      } catch (RuntimeException e) {
        return context.runtime.getClass("Encoding").getConstant("ASCII_8BIT");
      }
    }
}
