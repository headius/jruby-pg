package com.headius.jruby.pg_ext;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyEncoding;
import org.jruby.RubyException;
import org.jruby.RubyFixnum;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Encoding;
import org.postgresql.core.Field;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.util.UnixCrypt;

public class Connection extends RubyObject {
    private final static Pattern ENCODING_PATTERN = Pattern.compile("(?i).*set\\s+client_encoding\\s+(?:TO|=)\\s+'?(\\S+)'?.*");
    private final static Pattern PARAMS_PATTERN = Pattern.compile("(?:[^\\$]*(?:\\$(\\d+))[^\\$]*)");
    private final static Map<String, String> postgresEncodingToRubyEncoding = new HashMap<String, String>();
    protected final static int FORMAT_TEXT = 0;
    protected final static int FORMAT_BINARY = 1;

    protected static Connection LAST_CONNECTION = null;
    protected static Result EMPTY_RESULT;

    protected BaseConnection connection;
    protected org.jcodings.Encoding encoding;
    protected IRubyObject rubyEncoding;
    protected Map<String, com.headius.jruby.pg_ext.PgPreparedStatement> preparedQueries = new HashMap<String, com.headius.jruby.pg_ext.PgPreparedStatement>();

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

      byte[] cryptedPassword = UnixCrypt.crypt(((RubyString) username).getBytes(), ((RubyString) password).getBytes());
      return context.runtime.newString(new ByteList(cryptedPassword));
    }

    @JRubyMethod(meta = true)
    public static IRubyObject quote_ident(ThreadContext context, IRubyObject self, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(rest = true, meta = true)
    public static IRubyObject connect_start(ThreadContext context, IRubyObject self, IRubyObject[] args) {
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
      return _array;
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

    public static RaiseException newPgError(ThreadContext context, String message, org.jcodings.Encoding encoding) {
      RubyClass klass = context.runtime.getModule("PG").getClass("Error");
      RubyString rubyMessage = context.runtime.newString(message);
      if (encoding != null) {
        RubyEncoding rubyEncoding = RubyEncoding.newEncoding(context.runtime, encoding);
        rubyMessage = (RubyString) rubyMessage.encode(context, rubyEncoding);
      }
      IRubyObject exception = klass.newInstance(context, rubyMessage, null);
      return new RaiseException((RubyException) exception);
    }

    private IRubyObject get_empty_result(ThreadContext context) {
      try {
        if (EMPTY_RESULT == null) {
          BaseStatement st = ((BaseStatement) connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
          ResultSet set = st.createDriverResultSet(new Field[0], new Vector());
          EMPTY_RESULT = new Result(context.runtime, (RubyClass) context.runtime.getClassFromPath("PG::Result"), set, null, false);
        }
        return EMPTY_RESULT;
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    /******     PG::Connection INSTANCE METHODS: Connection Control     ******/

    @JRubyMethod(rest = true, required = 1)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        this.rubyEncoding = context.nil;

        String host = null;
        String dbname = null;
        Integer port = null;

        Properties props = parse_args(context, args);
        Iterator<Entry<Object, Object>> iterator = props.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<Object, Object> entry = iterator.next();
          if (entry.getKey().equals("host") || entry.getKey().equals("hostaddr")) {
            host = (String) entry.getValue();
            iterator.remove();
          } else if (entry.getKey().equals("port")) {
            port = Integer.parseInt((String) entry.getValue());
            iterator.remove();
          } else if (entry.getKey().equals("dbname")) {
            dbname = (String) entry.getValue();
            iterator.remove();
          }
        }

        try {
            Driver driver = DriverManager.getDriver("jdbc:postgresql");

            String connectionString = "";
            if (host != null && port != null)
              connectionString = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            else if (host != null)
              connectionString = "jdbc:postgresql://" + host + "/" + dbname;
            else
              connectionString = "jdbc:postgresql:" + dbname;

            // enable change in client encoding by issuing 'set client_encoding = foo' command
            props.setProperty("allowEncodingChanges", "true");

            connection = (BaseConnection)driver.connect(connectionString, props);
            // set the encoding if the default internal_encoding is set
            set_default_encoding(context);

            LAST_CONNECTION = this;
        } catch (SQLException sqle) {
            throw context.runtime.newRuntimeError(sqle.getLocalizedMessage());
        }
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject connect_poll(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"finish", "close"})
    public IRubyObject finish(ThreadContext context) {
      try {
        if (connection.isClosed()) {
          throw newPgError(context, "The connection is closed", encoding);
        }
        connection.close();
        return context.nil;
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

  @JRubyMethod
  public IRubyObject status(ThreadContext context) {
    try {
      if (connection.isClosed()) {
        return context.getConstant("PG::CONNECTION_BAD");
      } else {
        return context.getRuntime().getModule("PG").getConstant("CONNECTION_OK");
      }
    } catch (SQLException ex) {
      throw context.getRuntime().newRuntimeError(ex.getMessage());
    }
  }

    @JRubyMethod(name = "finished?")
    public IRubyObject finished_p(ThreadContext context) {
        return context.nil;
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
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject parameter_status(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject protocol_version(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject server_version(ThreadContext context) {
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        int databaseMajorVersion = metaData.getDatabaseMajorVersion();
        int databaseMinorVersion = metaData.getDatabaseMinorVersion();
        return new RubyFixnum(context.runtime, databaseMajorVersion * 100000 + databaseMinorVersion * 1000);
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

    @JRubyMethod
    public IRubyObject error_message(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject socket(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject backend_pid(ThreadContext context) {
        return context.nil;
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

    @JRubyMethod(name = {"exec", "query"}, required = 1, optional = 2)
    public IRubyObject exec(ThreadContext context, IRubyObject[] args, Block block) {
        String query = args[0].convertToString().toString();
        ResultSet set = null;
        try {
            if (args.length == 1 || ((RubyArray) args[1]).getLength() == 0) {
              Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
              statement.execute(query);
              set = statement.getResultSet();
            } else {
              // change the parameters from $1 to ? and keep track of where each parameter is used
              Map<Integer, List<Integer> > indexToQueryParameter = new HashMap<Integer, List<Integer>>();
              query = fixQueryParametersSyntax(query, indexToQueryParameter);
              PreparedStatement statement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
              com.headius.jruby.pg_ext.PgPreparedStatement st = new com.headius.jruby.pg_ext.PgPreparedStatement(statement, indexToQueryParameter);
              set = getPreparedStatmentResult(context, st, args);
            }

            Matcher matcher = ENCODING_PATTERN.matcher(query);
            if (matcher.matches()) {
              internal_encoding_set(context, context.runtime.newString(matcher.group(1)));
            }

            if (set == null)
              return context.nil;
        } catch (SQLException sqle) {
            throw newPgError(context, sqle.getLocalizedMessage(), encoding);
        }

        return createResult(context, set, args, block);
    }

    private IRubyObject createResult(ThreadContext context, ResultSet set, IRubyObject [] args, Block block) {
      // by default we return results in text format
      boolean binary = false;
      if (args.length == 3)
        binary = ((RubyFixnum) args[2]).getLongValue() == FORMAT_BINARY;

      Result result = new Result(context.runtime, (RubyClass)context.runtime.getClassFromPath("PG::Result"), set, encoding, binary);
      if (block.isGiven())
        return block.call(context, result);
      return result;
    }

    private ResultSet getPreparedStatmentResult(ThreadContext context, PgPreparedStatement st, IRubyObject[] args) throws SQLException {
      Map<Integer, List<Integer>> indexToQueryParameter = st.indexMapping;
      PreparedStatement statement = st.st;

      if (args.length == 1)
        return statement.executeQuery();

      IRubyObject[] params = ((RubyArray) args[1]).toJavaArrayUnsafe();

      for (int i = 1; i <= params.length; i++) {
        IRubyObject param = params[i - 1];
        List<Integer> list = indexToQueryParameter.get(i);

        for (int columnIndex : list) {
          if (param == null || param.isNil()) {
            statement.setNull(columnIndex, Types.OTHER);
          } else if (param instanceof RubyString) {
            statement.setString(columnIndex, ((RubyString) param).asJavaString());
          } else if (param instanceof RubyHash) {
            RubyHash hash = (RubyHash) param;

            RubySymbol value_s = context.runtime.newSymbol("value");
            RubySymbol format_s = context.runtime.newSymbol("format");

            RubyString value = (RubyString) hash.op_aref(context, value_s);
            RubyFixnum format = (RubyFixnum) hash.op_aref(context, format_s);

            if (format.getLongValue() == FORMAT_TEXT) {
              statement.setString(columnIndex, value.asJavaString());
            } else if (format.getLongValue() == FORMAT_BINARY) {
              statement.setBytes(columnIndex, value.getBytes());
            }
          } else {
            throw context.runtime.newArgumentError("parameters must be a string or hash");
          }
        }
      }
      return statement.executeQuery();
    }

    /**
     * the following method undo what the jdbc driver does; the driver expect "?" for query parameters
     * and convert it to "$i" (where i is some integer). we have to convert "$i" to "?" for the driver to convert
     * it back which is annoying.
     *
     * FIXME: This method doesn't respect escaping or quoting rules, i.e. if the dollar sign is in single quotes
     * we shouldn't touch it.
     *
     * @param query
     * @param indexToQueryParameter
     * @return
     */
    private String fixQueryParametersSyntax(String query, Map<Integer, List<Integer>> indexToQueryParameter) {
      Matcher matcher = PARAMS_PATTERN.matcher(query);
      if (!matcher.matches())
        return query;
      for (int i = 1; i <= matcher.groupCount(); i++) {
        int index = Integer.parseInt(matcher.group(i));
        List<Integer> list = indexToQueryParameter.get(index);
        if (list == null) {
          list = new ArrayList<Integer>();
          indexToQueryParameter.put(index, list);
        }
        list.add(i);
      }
      return query.replaceAll("\\$\\d+", "?");
    }

    @JRubyMethod(required = 2, rest = true)
    public IRubyObject prepare(ThreadContext context, IRubyObject[] args) {
      try {
        String queryName = args[0].asJavaString();
        String query = args[1].asJavaString();

        Map<Integer, List<Integer>> indexMapping = new HashMap<Integer, List<Integer>>();
        query = fixQueryParametersSyntax(query, indexMapping);
        PreparedStatement _st = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        com.headius.jruby.pg_ext.PgPreparedStatement st = new com.headius.jruby.pg_ext.PgPreparedStatement(_st, indexMapping);
        preparedQueries.put(queryName, st);

        return get_empty_result(context);
      } catch (SQLException e) {
        throw newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(required = 1, optional = 2)
    public IRubyObject exec_prepared(ThreadContext context, IRubyObject[] args, Block block) {
      try {
        String queryName = args[0].asJavaString();
        PgPreparedStatement st = preparedQueries.get(queryName);
        if (st == null)
          throw context.runtime.newRuntimeError("Unknown query " + queryName);
        ResultSet set = getPreparedStatmentResult(context, st, args);
        return createResult(context, set, args, block);
      } catch (SQLException e) {
        throw newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(required = 1)
    public IRubyObject describe_prepared(ThreadContext context, IRubyObject query_name) {
      return context.nil;
    }

    @JRubyMethod
    public IRubyObject describe_portal(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject make_empty_pgresult(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(alias = {"escape_string", "escape"}, required = 1, argTypes = {RubyString.class} )
    public IRubyObject escape_literal_native(ThreadContext context, IRubyObject _str) {
      RubyString str = (RubyString) _str;
      byte[] bytes = str.getBytes();
      int i;
      for (i = 0; i < bytes.length && bytes[i] != '\0'; i++);
      return escapeBytes(context, bytes, 0, i, rubyEncoding, connection.getStandardConformingStrings());
    }

    @JRubyMethod
    public IRubyObject escape_identifier(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject escape_bytea(ThreadContext context, IRubyObject array) {
      return escapeBytes(context, array, rubyEncoding, connection.getStandardConformingStrings());
    }

    @JRubyMethod
    public IRubyObject unescape_bytea(ThreadContext context, IRubyObject array) {
      return unescapeBytes(context, array);
    }

    /******     PG::Connection INSTANCE METHODS: Asynchronous Command Processing     ******/

    @JRubyMethod(rest = true)
    public IRubyObject send_query(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject send_prepare(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject send_query_prepared(ThreadContext context, IRubyObject[] args) {
        return context.nil;
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
    public IRubyObject get_result(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject consume_input(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject is_busy(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject set_nonblocking(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"isnonblocking", "nonblocking?"})
    public IRubyObject isnonblocking(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject flush(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: Cancelling Queries in Progress     ******/

    @JRubyMethod
    public IRubyObject cancel(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: NOTIFY     ******/

    @JRubyMethod
    public IRubyObject notifies(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: COPY     ******/

    @JRubyMethod
    public IRubyObject put_copy_data(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject put_copy_end(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject get_copy_end(ThreadContext context, IRubyObject[] args) {
        return context.nil;
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
          connection.setAutoCommit(false);
          if (block.arity().getValue() == 0)
            block.call(context);
          else
            block.call(context, this);
          connection.commit();
        } catch (RuntimeException ex) {
          connection.rollback();
          throw ex;
        } finally {
          connection.setAutoCommit(true);
        }
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
      return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject block(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(name = {"wait_for_notify", "notifies_wait"}, rest = true)
    public IRubyObject wait_for_notify(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject quote_ident(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"async_exec", "async_query"}, rest = true)
    public IRubyObject async_exec(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject get_last_result(ThreadContext context) {
        return context.nil;
    }

    /******     PG::Connection INSTANCE METHODS: Large Object Support     ******/

    @JRubyMethod(name = {"lo_creat", "locreat"}, optional = 1)
    public IRubyObject lo_creat(ThreadContext context, IRubyObject[] args) {
      try {
        LargeObjectManager manager = connection.getLargeObjectAPI();
        long oid;
        if (args.length == 1)
          oid = manager.createLO((Integer) args[0].toJava(Integer.class));
        else
          oid = manager.createLO();
        return new RubyFixnum(context.runtime, oid);
      } catch (SQLException e) {
        throw newPgError(context, "lo_create failed", encoding);
      }
    }

    @JRubyMethod(name = {"lo_create", "locreate"})
    public IRubyObject lo_create(ThreadContext context, IRubyObject arg0) {
      return lo_creat(context, new IRubyObject[0]);
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
        LargeObject object;
        long oidLong = (Long) args[0].toJava(Long.class);
        if (args.length == 1)
          object = connection.getLargeObjectAPI().open(oidLong);
        else
          object = connection.getLargeObjectAPI().open(oidLong, (Integer) args[1].toJava(Integer.class));

        return new LargeObjectFd(context.runtime, (RubyClass)context.runtime.getClassFromPath("PG::LargeObjectFd"), object);
      } catch (SQLException e) {
        throw newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_write", "lowrite"}, required = 2, argTypes = {LargeObjectFd.class, RubyString.class})
    public IRubyObject lo_write(ThreadContext context, IRubyObject object, IRubyObject buffer) {
      try {
        LargeObject largeObject = ((LargeObjectFd) object).getObject();
        RubyString bufferString = (RubyString) buffer;
        largeObject.write(bufferString.getBytes());
        return bufferString.length();
      } catch (SQLException e) {
        throw newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_read", "loread"}, required = 2, argTypes = {LargeObjectFd.class, RubyFixnum.class})
    public IRubyObject lo_read(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
      try {
        LargeObject largeObject = ((LargeObjectFd) arg0).getObject();
        byte[] b = largeObject.read((int) ((RubyFixnum) arg1).getLongValue());
        if (b.length == 0)
          return context.nil;
        return context.runtime.newString(new ByteList(b));
      } catch (SQLException e) {
        throw newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_lseek", "lolseek", "lo_seek", "loseek"}, required = 3,
        argTypes = {LargeObjectFd.class, RubyFixnum.class, RubyFixnum.class})
    public IRubyObject lo_lseek(ThreadContext context, IRubyObject object, IRubyObject offset, IRubyObject whence) {
      try {
        LargeObject largeObject = ((LargeObjectFd) object).getObject();
        largeObject.seek((int) ((RubyFixnum) offset).getLongValue(),
            (int) ((RubyFixnum) whence).getLongValue());
        return new RubyFixnum(context.runtime, largeObject.tell());
      } catch (SQLException e) {
        throw newPgError(context, e.getLocalizedMessage(), encoding);
      }
    }

    @JRubyMethod(name = {"lo_tell", "lotell"})
    public IRubyObject lo_tell(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_truncate", "lotruncate"})
    public IRubyObject lo_truncate(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_close", "loclose"})
    public IRubyObject lo_close(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_unlink", "lounlink"})
    public IRubyObject lo_unlink(ThreadContext context, IRubyObject arg0) {
        return context.nil;
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
      try {
        Encoding encoding = connection.getEncoding();
        IRubyObject rubyEncoding = findEncoding(context, encoding.name());
        return rubyEncoding;
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
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
