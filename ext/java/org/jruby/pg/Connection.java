package org.jruby.pg;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map.Entry;

import org.jcodings.Encoding;
import org.jruby.*;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.pg.internal.*;
import org.jruby.pg.internal.ResultSet.ResultStatus;
import org.jruby.pg.messages.ErrorResponse.ErrorField;
import org.jruby.pg.messages.Format;
import org.jruby.pg.messages.NotificationResponse;
import org.jruby.pg.messages.Value;
import org.jruby.runtime.Arity;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

@SuppressWarnings("serial")
public class Connection extends RubyObject {

  private final static String[] POSITIONAL_ARGS = {
    "host", "port", "options", "tty", "dbname", "user", "password"
  };
  private final static Map<String, String> postgresEncodingToRubyEncoding = new HashMap<String, String>();
  private final static Map<String, String> rubyEncodingToPostgresEncoding = new HashMap<String, String>();

  // private state
  private  PostgresqlConnection postgresConnection;
  private Properties props;
  private IRubyObject proc;

  private PostgresqlString BEGIN_QUERY = new PostgresqlString("BEGIN");
  private PostgresqlString COMMIT_QUERY = new PostgresqlString("COMMIT");
  private PostgresqlString ROLLBACK_QUERY = new PostgresqlString("ROLLBACK");

  private NoticeReceiver defaultReceiver;

  // the cached rubyIO that is returned by socket_io
  private RubyIO rubyIO;

  static {
    postgresEncodingToRubyEncoding.put("BIG5",          "Big5");
    postgresEncodingToRubyEncoding.put("EUC_CN",        "GB2312");
    postgresEncodingToRubyEncoding.put("EUC_JP",        "EUC-JP");
    postgresEncodingToRubyEncoding.put("EUC_JIS_2004",  "EUC-JP");
    postgresEncodingToRubyEncoding.put("EUC_KR",        "EUC-KR");
    postgresEncodingToRubyEncoding.put("EUC_TW",        "EUC-TW");
    postgresEncodingToRubyEncoding.put("GB18030",       "GB18030");
    postgresEncodingToRubyEncoding.put("GBK",           "GBK");
    postgresEncodingToRubyEncoding.put("ISO_8859_5",    "ISO8859-5");
    postgresEncodingToRubyEncoding.put("ISO_8859_6",    "ISO8859-6");
    postgresEncodingToRubyEncoding.put("ISO_8859_7",    "ISO8859-7");
    postgresEncodingToRubyEncoding.put("ISO_8859_8",    "ISO8859-8");
    postgresEncodingToRubyEncoding.put("KOI8",          "KOI8-R");
    postgresEncodingToRubyEncoding.put("KOI8R",         "KOI8-R");
    postgresEncodingToRubyEncoding.put("KOI8U",         "KOI8-U");
    postgresEncodingToRubyEncoding.put("LATIN1",        "ISO8859-1");
    postgresEncodingToRubyEncoding.put("LATIN2",        "ISO8859-2");
    postgresEncodingToRubyEncoding.put("LATIN3",        "ISO8859-3");
    postgresEncodingToRubyEncoding.put("LATIN4",        "ISO8859-4");
    postgresEncodingToRubyEncoding.put("LATIN5",        "ISO8859-9");
    postgresEncodingToRubyEncoding.put("LATIN6",        "ISO8859-10");
    postgresEncodingToRubyEncoding.put("LATIN7",        "ISO8859-13");
    postgresEncodingToRubyEncoding.put("LATIN8",        "ISO8859-14");
    postgresEncodingToRubyEncoding.put("LATIN9",        "ISO8859-15");
    postgresEncodingToRubyEncoding.put("LATIN10",       "ISO8859-16");
    postgresEncodingToRubyEncoding.put("MULE_INTERNAL", "Emacs-Mule");
    postgresEncodingToRubyEncoding.put("SJIS",          "Windows-31J");
    postgresEncodingToRubyEncoding.put("SHIFT_JIS_2004", "Windows-31J");
    postgresEncodingToRubyEncoding.put("UHC",           "CP949");
    postgresEncodingToRubyEncoding.put("UTF8",          "UTF-8");
    postgresEncodingToRubyEncoding.put("WIN866",        "IBM866");
    postgresEncodingToRubyEncoding.put("WIN874",        "Windows-874");
    postgresEncodingToRubyEncoding.put("WIN1250",       "Windows-1250");
    postgresEncodingToRubyEncoding.put("WIN1251",       "Windows-1251");
    postgresEncodingToRubyEncoding.put("WIN1252",       "Windows-1252");
    postgresEncodingToRubyEncoding.put("WIN1253",       "Windows-1253");
    postgresEncodingToRubyEncoding.put("WIN1254",       "Windows-1254");
    postgresEncodingToRubyEncoding.put("WIN1255",       "Windows-1255");
    postgresEncodingToRubyEncoding.put("WIN1256",       "Windows-1256");
    postgresEncodingToRubyEncoding.put("WIN1257",       "Windows-1257");
    postgresEncodingToRubyEncoding.put("WIN1258",       "Windows-1258");

    // set the mapping from ruby encoding to postgresql encoding
    rubyEncodingToPostgresEncoding.put("Big5",          "BIG5");
    rubyEncodingToPostgresEncoding.put("GB2312",        "EUC_CN");
    rubyEncodingToPostgresEncoding.put("EUC-JP",        "EUC_JP");
    rubyEncodingToPostgresEncoding.put("EUC-KR",        "EUC_KR");
    rubyEncodingToPostgresEncoding.put("EUC-TW",        "EUC_TW");
    rubyEncodingToPostgresEncoding.put("GB18030",       "GB18030");
    rubyEncodingToPostgresEncoding.put("GBK",           "GBK");
    rubyEncodingToPostgresEncoding.put("KOI8-R",        "KOI8");
    rubyEncodingToPostgresEncoding.put("KOI8-U",        "KOI8U");
    rubyEncodingToPostgresEncoding.put("ISO8859-1",    "LATIN1");
    rubyEncodingToPostgresEncoding.put("ISO8859-2",    "LATIN2");
    rubyEncodingToPostgresEncoding.put("ISO8859-3",    "LATIN3");
    rubyEncodingToPostgresEncoding.put("ISO8859-4",    "LATIN4");
    rubyEncodingToPostgresEncoding.put("ISO8859-5",    "ISO_8859_5");
    rubyEncodingToPostgresEncoding.put("ISO8859-6",    "ISO_8859_6");
    rubyEncodingToPostgresEncoding.put("ISO8859-7",    "ISO_8859_7");
    rubyEncodingToPostgresEncoding.put("ISO8859-8",    "ISO_8859_8");
    rubyEncodingToPostgresEncoding.put("ISO8859-9",    "LATIN5");
    rubyEncodingToPostgresEncoding.put("ISO8859-10",   "LATIN6");
    rubyEncodingToPostgresEncoding.put("ISO8859-13",   "LATIN7");
    rubyEncodingToPostgresEncoding.put("ISO8859-14",   "LATIN8");
    rubyEncodingToPostgresEncoding.put("ISO8859-15",   "LATIN9");
    rubyEncodingToPostgresEncoding.put("ISO8859-16",   "LATIN10");
    rubyEncodingToPostgresEncoding.put("Emacs-Mule",    "MULE_INTERNAL");
    rubyEncodingToPostgresEncoding.put("Windows-31J",   "SJIS");
    rubyEncodingToPostgresEncoding.put("Windows-31J",   "SHIFT_JIS_2004");
    rubyEncodingToPostgresEncoding.put("UHC",           "CP949");
    rubyEncodingToPostgresEncoding.put("UTF-8",         "UTF8");
    rubyEncodingToPostgresEncoding.put("IBM866",        "WIN866");
    rubyEncodingToPostgresEncoding.put("Windows-874",   "WIN874");
    rubyEncodingToPostgresEncoding.put("Windows-1250",  "WIN1250");
    rubyEncodingToPostgresEncoding.put("Windows-1251",  "WIN1251");
    rubyEncodingToPostgresEncoding.put("Windows-1252",  "WIN1252");
    rubyEncodingToPostgresEncoding.put("Windows-1253",  "WIN1253");
    rubyEncodingToPostgresEncoding.put("Windows-1254",  "WIN1254");
    rubyEncodingToPostgresEncoding.put("Windows-1255",  "WIN1255");
    rubyEncodingToPostgresEncoding.put("Windows-1256",  "WIN1256");
    rubyEncodingToPostgresEncoding.put("Windows-1257",  "WIN1257");
    rubyEncodingToPostgresEncoding.put("Windows-1258",  "WIN1258");
  }

  public Connection(Ruby ruby, RubyClass rubyClass) {
    super(ruby, rubyClass);
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

  @JRubyMethod(meta = true, required = 1, argTypes = {RubyArray.class})
  public static IRubyObject escape_bytea(ThreadContext context, IRubyObject self, IRubyObject array) {
    byte[] bytes = array.convertToString().getBytes();
    bytes = PostgresqlConnection.escapeBytesStatic(bytes);
    return context.runtime.newString(new ByteList(bytes));
  }

  @JRubyMethod(meta = true)
  public static IRubyObject unescape_bytea(ThreadContext context, IRubyObject self, IRubyObject array) {
    return unescapeBytes(context, array);
  }

  @JRubyMethod(meta = true, required = 2, argTypes = {RubyString.class, RubyString.class})
  public static IRubyObject encrypt_password(ThreadContext context, IRubyObject self, IRubyObject password, IRubyObject username) {
    if(username.isNil() || password.isNil()) {
      throw context.runtime.newTypeError("usernamd ane password cannot be nil");
    }

    try {
      byte[] cryptedPassword = PostgresqlConnection.encrypt(
                                 username.convertToString().getBytes(),
                                 password.convertToString().getBytes());
      return context.runtime.newString(new ByteList(cryptedPassword));
    } catch(NoSuchAlgorithmException e) {
      throw context.runtime.newRuntimeError(e.getLocalizedMessage());
    }
  }

  @JRubyMethod(rest = true, meta = true)
  public static IRubyObject connect_start(ThreadContext context, IRubyObject self, IRubyObject[] args, Block block) {
    Connection connection = new Connection(context.runtime, context.runtime.getModule("PG").getClass("Connection"));
    connection.props = parse_args(context, args);
    return connection.connectStart(context, block);
  }

  @JRubyMethod(rest = true, meta = true)
  public static IRubyObject ping(ThreadContext context, IRubyObject self, IRubyObject[] args) {
    Properties props = Connection.parse_args(context, args);
    try {
      return context.runtime.newFixnum(PostgresqlConnection.ping(props).ordinal());
    } catch(IOException ex) {
      throw context.runtime.newRuntimeError(ex.getLocalizedMessage());
    }
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
  public static IRubyObject unescapeBytes(ThreadContext context, IRubyObject _array) {
    RubyString string = (RubyString) _array;
    byte[] bytes = string.getBytes();
    if(bytes[0] == '\\' && bytes[1] == 'x') {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for(int i = 2; i < bytes.length; i += 2) {
        int b = charToInt(bytes[i]) * 16 + charToInt(bytes[i + 1]);
        out.write(b);
      }
      return context.runtime.newString(new ByteList(out.toByteArray()));
    } else {
      return _array;
    }
  }

  private static int charToInt(byte b) {
    if(Character.isLetter(b)) {
      return Character.toUpperCase(b) - 'A' + 10;
    } else {
      return b - '0';
    }
  }

  @SuppressWarnings("unchecked")
  private static Properties parse_args(ThreadContext context, IRubyObject[] args) {
    Properties argumentsHash = new Properties();
    if(args.length == 0) {
      return argumentsHash;
    }
    if(args.length > 7) {
      throw context.runtime.newArgumentError("extra positional parameter");
    }
    if(args.length != 7 && args.length != 1)
      throw context.runtime.newArgumentError(
        "Wrong number of arguments, see the documentation");


    if(args.length == 1) {
      // we have a string or hash
      if(args[0] instanceof RubyHash) {
        RubyHash hash = (RubyHash)args[0];

        for(Object _entry : hash.entrySet()) {
          Entry<String, Object> entry = (Entry<String, Object>) _entry;
          argumentsHash.put(PostgresHelpers.stringify(entry.getKey()), PostgresHelpers.stringify(entry.getValue()));
        }
      } else if(args[0] instanceof RubyString) {
        String[] tokens = tokenizeString(args[0].asJavaString());
        if(tokens.length % 2 != 0) {
          throw context.runtime.newArgumentError("wrong connection string");
        }
        for(int i = 0; i < tokens.length; i += 2) {
          argumentsHash.put(tokens[i], tokens[i + 1]);
        }
      } else {
        throw context.runtime.newArgumentError("Wrong type/number of arguments, see the documentation");
      }
    } else {
      // we have positional parameters
      for(int i = 0 ; i < POSITIONAL_ARGS.length ; i++) {
        if(!args[i].isNil())
          argumentsHash.put(POSITIONAL_ARGS[i], ((RubyObject) args[i]).to_s()
                            .asJavaString());
      }
    }
    return argumentsHash;
  }

  private static ObjectAllocator CONNECTION_ALLOCATOR = new ObjectAllocator() {
    @Override
    public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
      return new Connection(ruby, rubyClass);
    }
  };

  private static RubyClass lookupErrorClass(ThreadContext context, String state) {
    Ruby ruby = context.runtime;

    if(state == null) {
      return (RubyClass) ruby.getClassFromPath("PG::UnableToSend");
    }

    state = state.toUpperCase();

    RubyHash errors = (RubyHash) ruby.getModule("PG").getConstant("ERROR_CLASSES");
    IRubyObject klass = errors.op_aref(context, ruby.newString(state));
    if(klass.isNil()) {
      klass = errors.op_aref(context, ruby.newString(state.substring(0, 2)));
    }

    if(klass.isNil()) {
      klass = ruby.getClassFromPath("PG::ServerError");
    }
    return (RubyClass) klass;
  }

  private static String[] tokenizeString(String asJavaString) {
    List<String> tokens = new ArrayList<String>();
    StringBuffer currentToken = new StringBuffer();
    boolean insideSingleQuote = false;
    boolean escapeNextCharacter = false;
    for(int i = 0; i < asJavaString.length(); i++) {
      char currentChar = asJavaString.charAt(i);

      // finish the current token and append it if:
      //   - we just hit an equal sign or a space and we're not inside single quotes
      //   - we just hit a single quote and we're inside a token
      //   - we're at the end of the input string
      if((currentChar == '=' || Character.isWhitespace(currentChar)) && !insideSingleQuote ||
          (currentChar == '\'' && !escapeNextCharacter && insideSingleQuote)) {
        if(insideSingleQuote) {
          insideSingleQuote = false;
        }
        tokens.add(currentToken.toString());
        currentToken = new StringBuffer();
        // get rid of all the whitespaces and equal sign that follow
        i = consumeWhiteSpaceAndEqual(asJavaString, i);
      } else if(currentChar == '\\' && !escapeNextCharacter) {
        escapeNextCharacter = true;
      } else if(currentChar == '\'' && !escapeNextCharacter) {
        // don't add the last single quote. we just started a new token
        // surrounded by single quotes
        insideSingleQuote = true;
      } else {
        escapeNextCharacter = false;
        currentToken.append(currentChar);
      }
    }
    if(currentToken.length() != 0) {
      tokens.add(currentToken.toString());
    }
    return tokens.toArray(new String[tokens.size()]);
  }

  private static int consumeWhiteSpaceAndEqual(String string, int currentIndex) {
    for(int i = currentIndex + 1; i < string.length(); i++) {
      char currentCharacter = string.charAt(i);
      if(!Character.isWhitespace(currentCharacter) && currentCharacter != '=') {
        return i - 1;
      }
    }
    return string.length();
  }

  /******     PG::Connection INSTANCE METHODS: Connection Control     ******/

  @JRubyMethod(rest = true)
  public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
    proc = context.nil;
    props = parse_args(context, args);
    return connectSync(context);
  }

  @JRubyMethod(alias = "reset_poll")
  public IRubyObject connect_poll(ThreadContext context) {
    PollingStatus state = postgresConnection.connectPoll();
    return context.runtime.newFixnum(state.ordinal());
  }

  @JRubyMethod(alias = {"close"})
  public IRubyObject finish(ThreadContext context) {
    try {
      getConnection(context).close();
      if(rubyIO != null) {
        // close the rubyIO without raising any exceptions fixme: is
        // this the right way to do it ? we need to close the connection
        // since the expectation is that the IO object returned from
        // {@link socket_io} should be closed.
        rubyIO.getOpenFile().cleanup(context.runtime, false);
        rubyIO = null;
      }
      postgresConnection = null;
      return context.nil;
    } catch(IOException e) {
      throw newPgError(context, e, null);
    }
  }

  @JRubyMethod
  public IRubyObject status(ThreadContext context) {
    return context.runtime.newFixnum(postgresConnection.getStatus().ordinal());
  }

  @JRubyMethod(name = "finished?")
  public IRubyObject finished_p(ThreadContext context) {
    if(postgresConnection == null) {
      return context.runtime.getTrue();
    }
    return  context.runtime.getFalse();
  }

  @JRubyMethod
  public IRubyObject reset(ThreadContext context) {
    finish(context);
    return connectSync(context);
  }

  @JRubyMethod
  public IRubyObject reset_start(ThreadContext context) {
    finish(context);
    return connectStart(context, Block.NULL_BLOCK);
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
    return context.runtime.newFixnum(postgresConnection.getTransactionStatus().getValue());
  }

  @JRubyMethod(required = 1)
  public IRubyObject parameter_status(ThreadContext context, IRubyObject arg0) {
    String name = arg0.asJavaString();
    return context.runtime.newString(postgresConnection.getParameterStatus(name));
  }

  @JRubyMethod
  public IRubyObject protocol_version(ThreadContext context) {
    return context.nil;
  }

  @JRubyMethod
  public IRubyObject server_version(ThreadContext context) {
    return context.runtime.newFixnum(postgresConnection.getServerVersion());
  }

  @JRubyMethod
  public IRubyObject error_message(ThreadContext context) {
    return context.nil;
  }

  @JRubyMethod
  public IRubyObject socket(ThreadContext context) {
    return socket_io(context).fileno(context);
  }

  @JRubyMethod
  public RubyIO socket_io(ThreadContext context) {
    if(rubyIO == null) {
      SelectableChannel socket = getConnection(context).getSocket();
      rubyIO = RubyIO.newIO(context.runtime, socket);
      rubyIO.setAutoclose(false);
    }
    return rubyIO;
  }

  @JRubyMethod
  public IRubyObject backend_pid(ThreadContext context) {
    return context.runtime.newFixnum(postgresConnection.getBackendPid());
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

  @JRubyMethod(alias = {"query", "exec_params", "async_exec", "async_query"}, required = 1, optional = 2)
  public IRubyObject exec(ThreadContext context, IRubyObject[] args, Block block) {
    if(postgresConnection == null) {
      throw newPgError(context, "closed connection used", null);
    }

    PostgresqlString query = rubyStringAsPostgresqlString(args[0]);
    ResultSet set = null;
    try {
      if(args.length == 1 || args[1].isNil()) {
        set = postgresConnection.exec(query);
      } else {

        RubyArray params = (RubyArray) args[1];

        Value [] values = new Value[params.getLength()];
        int [] oids = new int[params.getLength()];
        fillValuesAndFormat(context, params, values, oids);
        Format resultFormat = getFormat(context, args);
        set = postgresConnection.execQueryParams(query, values, resultFormat, oids);
      }

      if(set == null) {
        return context.nil;
      }
    } catch(IOException sqle) {
      throw newPgError(context, sqle.getLocalizedMessage(), null);
    }

    IRubyObject res = createResult(context, set);
    if(!res.isNil()) {
      ((Result)res).check(context);
    }
    if(block.isGiven() && !res.isNil()) {
      return block.call(context, res);
    }
    return res;
  }

  @JRubyMethod(required = 2, rest = true)
  public IRubyObject prepare(ThreadContext context, IRubyObject[] args, Block block) {
    try {
      PostgresqlString name = rubyStringAsPostgresqlString(args[0]);
      PostgresqlString query = rubyStringAsPostgresqlString(args[1]);
      int [] oids = null;
      if(args.length == 3) {
        RubyArray array = ((RubyArray) args[2]);
        oids = new int[array.getLength()];
        for(int i = 0; i < oids.length; i++) {
          oids[i] = (int)((RubyFixnum) array.get(i)).getLongValue();
        }
      }
      oids = oids == null ? new int [0] : oids;
      ResultSet result = postgresConnection.prepare(name, query, oids);
      IRubyObject res = createResult(context, result);
      if(!res.isNil()) {
        ((Result)res).check(context);
      }
      return res;
    } catch(IOException ex) {
      throw newPgError(context, ex.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod(required = 1, optional = 2)
  public IRubyObject exec_prepared(ThreadContext context, IRubyObject[] args, Block block) {
    try {
      ResultSet set = execPreparedCommon(context, args, false);
      IRubyObject res = createResult(context, set);
      if(!res.isNil()) {
        ((Result)res).check(context);
      }
      if(block.isGiven() && !res.isNil()) {
        return block.call(context, res);
      }
      return res;
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  @JRubyMethod(required = 1)
  public IRubyObject describe_prepared(ThreadContext context, IRubyObject query_name) {
    try {
      PostgresqlString queryName = rubyStringAsPostgresqlString(query_name);
      ResultSet resultSet = postgresConnection.describePrepared(queryName);
      IRubyObject res = createResult(context, resultSet);
      if(!res.isNil()) {
        ((Result)res).check(context);
      }
      return res;
    } catch(IOException ex) {
      throw newPgError(context, ex.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod(required = 1)
  public IRubyObject describe_portal(ThreadContext context, IRubyObject arg0) {
    try {
      PostgresqlString name = rubyStringAsPostgresqlString(arg0);
      ResultSet resultSet = postgresConnection.describePortal(name);
      IRubyObject res = createResult(context, resultSet);
      if(!res.isNil()) {
        ((Result)res).check(context);
      }
      return res;
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod
  public IRubyObject make_empty_pgresult(ThreadContext context, IRubyObject arg0) {
    long status = ((RubyFixnum)arg0).getLongValue();
    for(ResultStatus st : ResultStatus.values()) {
      if(st.ordinal() == status) {
        return createResult(context, ResultSet.createWithStatus(st));
      }
    }
    throw newPgError(context, "unknown result status", null);
  }

  @JRubyMethod(meta = true, alias = "escape")
  public static IRubyObject escape_string(ThreadContext context,
                                          IRubyObject self, IRubyObject _str) {
    RubyString str = (RubyString) _str;
    String javaString = str.toString();
    byte[] bytes = PostgresqlConnection.escapeStringStatic(javaString).getBytes();
    return context.runtime.newString(new ByteList(bytes, str.getByteList().getEncoding()));
  }

  @JRubyMethod(alias = "escape")
  public IRubyObject escape_string(ThreadContext context, IRubyObject _str) {
    RubyString str = (RubyString) _str;
    String javaString = str.toString();
    byte[] bytes = postgresConnection.escapeString(javaString).getBytes();
    RubyEncoding encoding = (RubyEncoding) internal_encoding(context);
    return context.runtime.newString(new ByteList(bytes, encoding.getEncoding()));
  }

  @JRubyMethod(required = 1, argTypes = {RubyString.class})
  public IRubyObject escape_literal(ThreadContext context, IRubyObject literal) {
    RubyString lit = (RubyString) literal;
    String escaped = PostgresqlConnection.escapeLiteral(lit.asJavaString());
    Encoding encoding = getClientEncodingAsJavaEncoding(context);
    return context.runtime.newString(new ByteList(escaped.getBytes(), encoding));
  }

  @JRubyMethod
  public IRubyObject escape_bytea(ThreadContext context, IRubyObject array) {
    RubyString str = (RubyString) array;
    byte[] bytes = PostgresqlConnection.escapeBytesStatic(str.getBytes());
    return context.runtime.newString(new ByteList(bytes));
  }

  @JRubyMethod
  public IRubyObject unescape_bytea(ThreadContext context, IRubyObject array) {
    return unescapeBytes(context, array);
  }

  /******     PG::Connection INSTANCE METHODS: Asynchronous Command Processing     ******/

  @JRubyMethod(rest = true)
  public IRubyObject send_query(ThreadContext context, IRubyObject[] args) {
    try {
      PostgresqlString query = rubyStringAsPostgresqlString(args[0]);
      if(args.length == 1) {
        postgresConnection.sendQuery(query);
      } else {
        RubyArray params = (RubyArray) args[1];

        Value [] values = new Value[params.getLength()];
        int [] oids = new int[params.getLength()];
        fillValuesAndFormat(context, params, values, oids);
        Format resultFormat = getFormat(context, args);
        postgresConnection.sendQueryParams(query, values, resultFormat, oids);
      }
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
    return context.nil;
  }

  @JRubyMethod(rest = true)
  public IRubyObject send_prepare(ThreadContext context, IRubyObject[] args) {
    throw newPgError(context, "Not implemented", null);
  }

  @JRubyMethod(rest = true)
  public IRubyObject send_query_prepared(ThreadContext context, IRubyObject[] args) {
    try {
      execPreparedCommon(context, args, true);
      return context.nil;
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  @JRubyMethod
  public IRubyObject send_describe_prepared(ThreadContext context, IRubyObject arg0) {
    throw newPgError(context, "not implemented", null);
  }

  @JRubyMethod
  public IRubyObject send_describe_portal(ThreadContext context, IRubyObject arg0) {
    throw newPgError(context, "not implemented", null);
  }

  @JRubyMethod
  public IRubyObject get_result(ThreadContext context, Block block) {
    try {
      ResultSet set = postgresConnection.getResult();
      IRubyObject res = createResult(context, set);
      if(block.isGiven() && !res.isNil()) {
        return block.call(context, res);
      }
      return res;
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod
  public IRubyObject consume_input(ThreadContext context) {
    try {
      postgresConnection.consumeInput();
      return context.nil;
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod
  public IRubyObject is_busy(ThreadContext context) {
    try {
      return context.runtime.newBoolean(postgresConnection.isBusy());
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  @JRubyMethod
  public IRubyObject set_nonblocking(ThreadContext context, IRubyObject arg0) {
    if(arg0.isTrue()) {
      postgresConnection.setNonBlocking(true);
    }
    postgresConnection.setNonBlocking(false);
    return arg0;
  }

  @JRubyMethod
  public IRubyObject set_single_row_mode(ThreadContext context) {
    try {
      postgresConnection.setSingleRowMode();
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
    return this;
  }

  @JRubyMethod(name = {"isnonblocking", "nonblocking?"})
  public IRubyObject isnonblocking(ThreadContext context) {
    return context.runtime.newBoolean(postgresConnection.isNonBlocking());
  }

  @JRubyMethod
  public IRubyObject flush(ThreadContext context) {
    try {
      return context.runtime.newBoolean(postgresConnection.flush());
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
  }

  /******     PG::Connection INSTANCE METHODS: Cancelling Queries in Progress     ******/

  @JRubyMethod
  public IRubyObject cancel(ThreadContext context) {
    try {
      postgresConnection.cancel();
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
    return context.nil;
  }

  /******     PG::Connection INSTANCE METHODS: NOTIFY     ******/

  @JRubyMethod
  public IRubyObject notifies(ThreadContext context) {
    try {
      NotificationResponse notification = postgresConnection.notifies();
      if(notification == null) {
        return context.nil;
      }
      RubyHash hash = new RubyHash(context.runtime);

      RubySymbol relname = context.runtime.newSymbol("relname");
      RubySymbol pid = context.runtime.newSymbol("be_pid");
      RubySymbol extra = context.runtime.newSymbol("extra");

      hash.op_aset(context, relname, context.runtime.newString(notification.getCondition()));
      hash.op_aset(context, pid, context.runtime.newFixnum(notification.getPid()));
      hash.op_aset(context, extra, context.runtime.newString(notification.getPayload()));

      return hash;
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  /******     PG::Connection INSTANCE METHODS: COPY     ******/

  @JRubyMethod
  public IRubyObject put_copy_data(ThreadContext context, IRubyObject arg0) {
    byte[] bytes = ((RubyString) arg0).getBytes();
    try {
      postgresConnection.putCopyData(bytes);
    } catch(IOException ex) {
      // todo: raise ??
    }
    return context.runtime.getTrue();
  }

  @JRubyMethod(rest = true)
  public IRubyObject put_copy_end(ThreadContext context, IRubyObject[] args) {
    try {
      String err = null;
      if(args.length > 0) {
        err = ((RubyString)args[0]).asJavaString();
      }
      postgresConnection.putCopyEnd(err);
      return context.runtime.getTrue();
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  @JRubyMethod(rest = true)
  public IRubyObject get_copy_data(ThreadContext context, IRubyObject[] args) {
    try {
      boolean async = false;
      if(args.length == 1) {
        async = args[0].isTrue();
      }
      byte[] data = postgresConnection.getCopyData(async);
      if(data == null) {
        // copy mode is over return a nil
        return context.nil;
      } else if(data.length == 0) {
        // we will receive an empty array if the data isn't ready
        return context.runtime.getFalse();
      }
      return context.runtime.newString(new ByteList(data));
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  /******     PG::Connection INSTANCE METHODS: Control Functions     ******/

  @JRubyMethod
  public IRubyObject set_error_visibility(ThreadContext context, IRubyObject arg0) {
    return context.nil;
  }

  @JRubyMethod(required = 1)
  public IRubyObject trace(final ThreadContext context, IRubyObject arg0) {
    IRubyObject fd = arg0.callMethod(context, "fileno");
    if(!(fd instanceof RubyFixnum)) {
      throw context.runtime.newArgumentError("expected fileno to return a fixnum");
    }
    RubyString mode = context.runtime.newString("w");
    RubyClass rubyIO = (RubyClass) context.runtime.getClassFromPath("IO");
    IRubyObject origIO = rubyIO.newInstance(context, new IRubyObject[] {fd, mode},
                                            Block.NULL_BLOCK);
    origIO.callMethod(context, "autoclose=", context.runtime.getFalse());
    final IRubyObject io = origIO.callMethod(context, "dup");

    postgresConnection.trace(new Writer() {
      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        IRubyObject str = context.runtime.newString(new String(cbuf, off, len));
        io.callMethod(context, "write", str);
        flush();
      }

      @Override
      public void flush() throws IOException {
        io.callMethod(context, "flush");
      }

      @Override
      public void close() throws IOException {
        io.callMethod(context, "close");
      }
    });
    return context.nil;
  }

  @JRubyMethod
  public IRubyObject untrace(ThreadContext context) {
    postgresConnection.untrace();
    return context.nil;
  }

  /******     PG::Connection INSTANCE METHODS: Notice Processing     ******/

  @JRubyMethod
  public IRubyObject set_notice_receiver(final ThreadContext context, Block block) {
    if(defaultReceiver == null) {
      defaultReceiver = postgresConnection.setNoticeReceiver(null);
    }

    // if the proc is nil, reset the notice receiver and return the
    // previous proc
    IRubyObject oldProc = proc;
    if(block == Block.NULL_BLOCK) {
      postgresConnection.setNoticeReceiver(defaultReceiver);
      proc = context.nil;
      return oldProc;
    }

    proc = RubyProc.newProc(context.runtime, block, Block.Type.PROC);
    postgresConnection.setNoticeReceiver(new NoticeReceiver() {
      @Override
      public void receive(ResultSet result) {
        IRubyObject resultSet = createResult(context, result);
        ((RubyProc)proc).call(context, new IRubyObject[] {resultSet});
      }
    });
    return oldProc;
  }

  @JRubyMethod
  public IRubyObject set_notice_processor(final ThreadContext context, Block block) {
    if(defaultReceiver == null) {
      defaultReceiver = postgresConnection.setNoticeReceiver(null);
    }

    // if the proc is nil, reset the notice receiver and return the
    // previous proc
    IRubyObject oldProc = proc;
    if(block == Block.NULL_BLOCK) {
      postgresConnection.setNoticeReceiver(defaultReceiver);
      proc = context.nil;
      return oldProc;
    }

    proc = RubyProc.newProc(context.runtime, block, Block.Type.PROC);
    postgresConnection.setNoticeReceiver(new NoticeReceiver() {
      @Override
      public void receive(ResultSet result) {
        IRubyObject err = context.runtime.newString(result.getError());
        ((RubyProc)proc).call(context, new IRubyObject[] {err});
      }
    });
    return oldProc;
  }

  /******     PG::Connection INSTANCE METHODS: Other    ******/

  @JRubyMethod()
  public IRubyObject transaction(ThreadContext context, Block block) {
    if(!block.isGiven()) {
      throw context.runtime.newArgumentError("Must supply block for PG::Connection#transaction");
    }

    try {
      try {
        postgresConnection.exec(BEGIN_QUERY);
        IRubyObject yieldResult;
        if(block.arity() == Arity.NO_ARGUMENTS) {
          yieldResult = block.yieldSpecific(context);
        } else {
          yieldResult = block.yieldSpecific(context, this);
        }
        postgresConnection.exec(COMMIT_QUERY);
        return yieldResult;
      } catch(IOException ex) {
        postgresConnection.exec(ROLLBACK_QUERY);
        throw newPgError(context, ex, null);
      } catch(RaiseException ex) {
        postgresConnection.exec(ROLLBACK_QUERY);
        throw ex;
      }
    } catch(IOException e) {
      throw context.runtime.newRuntimeError(e.getLocalizedMessage());
    }
  }

  @JRubyMethod(optional = 1)
  public IRubyObject block(final ThreadContext context, IRubyObject[] args)
  throws IOException {
    long timeout = 0;
    if(args.length >= 1) {
      double timeoutFraction = ((RubyNumeric)args[0]).getDoubleValue();
      timeout = (long)(timeoutFraction * 1000);
    }

    Object retVal = waitForReadable(context, timeout,
    new Readable() {
      @Override
      public Object isReadable() throws IOException {
        return getConnection(context).isBusy() ? null : Boolean.TRUE ;
      }
    });

    return retVal == null ? context.runtime.getFalse() : context.runtime.getTrue();
  }

  @JRubyMethod(alias = {"notifies_wait"}, optional = 1)
  public IRubyObject wait_for_notify(final ThreadContext context, IRubyObject[] args,
                                     Block block) {
    NotificationResponse notification;
    long timeout = 0;

    // calculate the aborttime which will be used to set the timeout
    // of the blocking select() syscall
    if(args.length >= 1 && !args[0].isNil()) {
      double timeoutFractional = ((RubyNumeric) args[0]).getDoubleValue();
      timeout = (long)(timeoutFractional * 1000);
    }

    try {
      notification = (NotificationResponse) waitForReadable(context, timeout,
      new Readable() {
        @Override
        public Object isReadable() throws IOException {
          return getConnection(context).notifies();
        }
      });

      if(!block.isGiven() || notification == null) {
        if(notification == null) {
          return context.nil;
        }
        return context.runtime.newString(notification.getCondition());
      }

      RubyString condition = context.runtime.newString(notification.getCondition());
      RubyFixnum pid = context.runtime.newFixnum(notification.getPid());
      String _payload = notification.getPayload();
      IRubyObject payload = context.nil;
      if(_payload != null) {
        payload = context.runtime.newString(_payload);
      }

      if(!block.arity().isFixed()) {
        return block.call(context, condition, pid, payload);
      } else if(block.arity().required() == 2) {
        return block.call(context, condition, pid);
      } else if(block.arity().required() == 3) {
        return block.call(context, condition, pid, payload);
      } else {
        return block.call(context);
      }
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  @JRubyMethod(required = 1, alias = {"quote_ident"})
  public IRubyObject escape_identifier(ThreadContext context, IRubyObject identifier) {
    RubyString ident = (RubyString) identifier;
    String escaped = PostgresqlConnection.escapeIdentifier(ident.asJavaString());
    // use the client encoding
    Encoding encoding = getClientEncodingAsJavaEncoding(context);
    return context.runtime.newString(new ByteList(escaped.getBytes(), encoding));
  }

  @JRubyMethod(meta = true, required = 1)
  public static IRubyObject quote_ident(ThreadContext context,
                                        IRubyObject self,
                                        IRubyObject identifier) {
    RubyString ident = (RubyString) identifier;
    String escaped = PostgresqlConnection.escapeIdentifier(ident.asJavaString());
    Encoding encoding = ident.getByteList().getEncoding();
    return context.runtime.newString(new ByteList(escaped.getBytes(), encoding));
  }

  @JRubyMethod
  public IRubyObject get_last_result(ThreadContext context) {
    try {
      ResultSet result, prevResult = null;
      while((result = postgresConnection.getResult()) != null) {
        prevResult = result;
        if(result.getStatus().isCopyStatus()) {
          break;
        }
      }
      IRubyObject res = createResult(context, prevResult);
      if(!res.isNil()) {
        ((Result)res).check(context);
      }
      return res;
    } catch(IOException e) {
      throw newPgError(context, e.getLocalizedMessage(), null);
    }
  }

  /******     PG::Connection INSTANCE METHODS: Large Object Support     ******/

  @JRubyMethod(name = {"lo_creat", "locreat"}, optional = 1)
  public IRubyObject lo_creat(ThreadContext context, IRubyObject[] args) {
    try {
      LargeObjectAPI manager = postgresConnection.getLargeObjectAPI();
      int oid;
      if(args.length == 1) {
        oid = manager.loCreat((Integer) args[0].toJava(Integer.class));
      } else {
        oid = manager.loCreat(0);
      }
      return new RubyFixnum(context.runtime, oid);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), e.getResultSet());
    } catch(IOException e) {
      throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod(name = {"lo_create", "locreate"})
  public IRubyObject lo_create(ThreadContext context, IRubyObject arg0) {
    try {
      LargeObjectAPI manager = postgresConnection.getLargeObjectAPI();
      int oid = manager.loCreate((Integer) arg0.toJava(Integer.class));
      return new RubyFixnum(context.runtime, oid);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), e.getResultSet());
    } catch(IOException e) {
      throw newPgError(context, "lo_create failed: " + e.getLocalizedMessage(), null);
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
      if(args.length == 1) {
        fd = postgresConnection.getLargeObjectAPI().loOpen((int) oidLong);
      } else {
        fd = postgresConnection.getLargeObjectAPI().loOpen((int) oidLong, (Integer) args[1].toJava(Integer.class));
      }

      return context.runtime.newFixnum(fd);
    } catch(IOException e) {
      throw newPgError(context, "lo_open failed: " + e.getLocalizedMessage(), null);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_open failed: " + e.getLocalizedMessage(), e.getResultSet());
    }
  }

  @JRubyMethod(name = {"lo_write", "lowrite"}, required = 2, argTypes = {RubyFixnum.class, RubyString.class})
  public IRubyObject lo_write(ThreadContext context, IRubyObject object, IRubyObject buffer) {
    try {
      long fd = ((RubyFixnum) object).getLongValue();
      RubyString bufferString = (RubyString) buffer;
      int count = postgresConnection.getLargeObjectAPI().loWrite((int) fd, bufferString.getBytes());
      return context.runtime.newFixnum(count);
    } catch(IOException e) {
      throw newPgError(context, "lo_write failed: " + e.getLocalizedMessage(), null);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_write failed: " + e.getLocalizedMessage(), e.getResultSet());
    }
  }

  @JRubyMethod(name = {"lo_read", "loread"}, required = 2, argTypes = {RubyFixnum.class, RubyFixnum.class})
  public IRubyObject lo_read(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
    try {
      int fd = (int)((RubyFixnum) arg0).getLongValue();
      int count = (int)((RubyFixnum) arg1).getLongValue();
      byte[] b = postgresConnection.getLargeObjectAPI().loRead(fd, count);
      if(b.length == 0) {
        return context.nil;
      }
      return context.runtime.newString(new ByteList(b));
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_read failed: " + e.getLocalizedMessage(), e.getResultSet());
    } catch(IOException e) {
      throw newPgError(context, "lo_read failed: " + e.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod(name = {"lo_lseek", "lolseek", "lo_seek", "loseek"}, required = 3,
               argTypes = {RubyFixnum.class, RubyFixnum.class, RubyFixnum.class})
  public IRubyObject lo_lseek(ThreadContext context, IRubyObject _fd, IRubyObject _offset, IRubyObject _whence) {
    try {
      int offset = (int)((RubyFixnum) _offset).getLongValue();
      int fd = (int)((RubyFixnum) _fd).getLongValue();
      int whence = (int)((RubyFixnum) _whence).getLongValue();
      int where = postgresConnection.getLargeObjectAPI().loSeek(fd, offset, whence);
      return new RubyFixnum(context.runtime, where);
    } catch(IOException e) {
      throw newPgError(context, "lo_lseek failed: " + e.getLocalizedMessage(), null);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_lseek failed: " + e.getLocalizedMessage(), e.getResultSet());
    }
  }

  @JRubyMethod(name = {"lo_tell", "lotell"}, required = 1, argTypes = {RubyFixnum.class})
  public IRubyObject lo_tell(ThreadContext context, IRubyObject _fd) {
    try {
      int fd = (int)((RubyFixnum) _fd).getLongValue();
      int where = postgresConnection.getLargeObjectAPI().loTell(fd);
      return context.runtime.newFixnum(where);
    } catch(IOException e) {
      throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), null);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), e.getResultSet());
    }
  }

  @JRubyMethod(name = {"lo_truncate", "lotruncate"}, required = 2, argTypes = {RubyFixnum.class, RubyFixnum.class})
  public IRubyObject lo_truncate(ThreadContext context, IRubyObject _fd, IRubyObject _len) {
    try {
      int fd = (int)((RubyFixnum) _fd).getLongValue();
      int len = (int)((RubyFixnum) _len).getLongValue();
      int value = postgresConnection.getLargeObjectAPI().loTruncate(fd, len);
      return context.runtime.newFixnum(value);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), e.getResultSet());
    } catch(IOException e) {
      throw newPgError(context, "lo_tell failed: " + e.getLocalizedMessage(), null);
    }
  }

  @JRubyMethod(name = {"lo_close", "loclose"}, required = 1, argTypes = {RubyFixnum.class})
  public IRubyObject lo_close(ThreadContext context, IRubyObject _fd) {
    try {
      int fd = (int)((RubyFixnum) _fd).getLongValue();
      int value = postgresConnection.getLargeObjectAPI().loClose(fd);
      return context.runtime.newFixnum(value);
    } catch(IOException e) {
      throw newPgError(context, "lo_close failed: " + e.getLocalizedMessage(), null);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_close failed: " + e.getLocalizedMessage(), e.getResultSet());
    }
  }

  @JRubyMethod(name = {"lo_unlink", "lounlink"}, required = 1, argTypes = {RubyFixnum.class})
  public IRubyObject lo_unlink(ThreadContext context, IRubyObject _fd) {
    try {
      int fd = (int)((RubyFixnum) _fd).getLongValue();
      int value = postgresConnection.getLargeObjectAPI().loUnlink(fd);
      return context.runtime.newFixnum(value);
    } catch(IOException e) {
      throw newPgError(context, "lo_unlink failed: " + e.getLocalizedMessage(), null);
    } catch(PostgresqlException e) {
      throw newPgError(context, "lo_unlink failed: " + e.getLocalizedMessage(), e.getResultSet());
    }
  }

  /******     M17N     ******/

  @JRubyMethod
  public IRubyObject get_client_encoding(ThreadContext context) {
    return context.runtime.newString(postgresConnection.getClientEncoding());
  }

  @JRubyMethod(required = 1, alias = {"client_encoding="})
  public IRubyObject set_client_encoding(ThreadContext context, IRubyObject encoding) {
    try {
      return setClientEncodingCommon(context, encoding.asJavaString());
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
  }

  @JRubyMethod
  public IRubyObject internal_encoding(ThreadContext context) {
    if(postgresConnection == null) {
      return context.nil;
    }

    String encoding = postgresConnection.getClientEncoding();
    return findEncoding(context, postgresEncodingToRubyEncoding.get(encoding));
  }

  @JRubyMethod(name = "internal_encoding=")
  public IRubyObject set_internal_encoding(ThreadContext context, IRubyObject encoding) {
    String postgresEncoding;
    if(encoding instanceof RubyString) {
      postgresEncoding = encoding.asJavaString().toUpperCase();
    } else if(encoding instanceof RubyEncoding) {
      postgresEncoding = ((RubyEncoding) encoding).to_s(context).asJavaString();
    } else {
      postgresEncoding = "SQL_ASCII";
    }
    if(rubyEncodingToPostgresEncoding.containsKey(postgresEncoding)) {
      postgresEncoding = rubyEncodingToPostgresEncoding.get(postgresEncoding);
    } else {
      postgresEncoding = "SQL_ASCII";
    }
    try {
      postgresConnection.setClientEncoding(postgresEncoding);
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }
    return context.nil;
  }

  @JRubyMethod
  public IRubyObject set_default_encoding(ThreadContext context)  {
    IRubyObject _internal_encoding = RubyEncoding.getDefaultInternal(context, this);
    if(_internal_encoding.isNil()) {
      return context.nil;
    }
    return set_internal_encoding(context, _internal_encoding);
  }

  /**
   * An interface used by waitForReadable to determine when the
   * connection is ready
   */
  private static interface Readable {
    /**
     * Returns an object that will be returned by waitForReadable or
     * null if the connection isn't ready
     * @throws Exception
     */
    Object isReadable() throws IOException;
  }

  /**
   * Wait for Readable to return a value for the given timeout. If the
   * timeout elapses a null value is returned. Otherwise the value
   * returned by the readable is returned.
   *
   * @param timeout the timeout to wait for the connection to return a
   *        value, 0 means block forever
   * @param readable
   * @return the value returned by readable or null if the timeout elapses
   * @throws IOException
   */
  private Object waitForReadable(ThreadContext context, long timeout,
                                 Readable readable) throws IOException {
    // check for errors before we start
    try {
      getConnection(context).consumeInput();
    } catch(IOException ex) {
      throw newPgErrorCommon(context, ex.getLocalizedMessage(),
                             "ConnectionBad", null);
    }

    long abortTime = 0;
    if(timeout > 0) {
      abortTime = System.currentTimeMillis() + timeout;
    }
    Object retVal;
    Selector selector = Selector.open();
    try {
      SelectionKey key = getConnection(context).getSocket().register(selector, 0);
      // wait until the connection is ready
      while((retVal = readable.isReadable()) == null) {
        key.interestOps(SelectionKey.OP_READ);
        long waitTime = abortTime - System.currentTimeMillis();

        if(abortTime > 0 && waitTime <= 0) {
          // break if we ran out of time
          break;
        } else if(abortTime > 0) {
          // otherwise wait `waitTime' for the socket to be readable
          selector.select(waitTime);
        } else {
          selector.select();
        }
        // read more data
        getConnection(context).consumeInput();
      }
    } finally {
      selector.close();
    }

    return retVal;
  }

  private IRubyObject connectStart(ThreadContext context, Block block) {
    try {
      postgresConnection = PostgresqlConnection.connectStart(props);
      if(block.isGiven()) {
        IRubyObject value = block.yield(context, this);
        finish(context);
        return value;
      }
      return this;
    } catch(IOException ex) {
      throw newPgErrorCommon(context, ex.getLocalizedMessage(), "ConnectionBad",
                             getClientEncodingAsJavaEncoding(context));
    } catch(GeneralSecurityException ex) {
      throw newPgErrorCommon(context, ex.getLocalizedMessage(), "ConnectionBad",
                             getClientEncodingAsJavaEncoding(context));
    }
  }

  private RaiseException newPgErrorCommon(ThreadContext context,
                                          String message, String sqlstate,
                                          org.jcodings.Encoding encoding) {
    Ruby runtime = context.runtime;
    RubyClass klass = lookupErrorClass(context, sqlstate);

    if(message == null) {
      message = "Unknown error";
    }

    RubyString rubyMessage = context.runtime.newString(message);
    if(encoding != null) {
      RubyEncoding rubyEncoding = (RubyEncoding) runtime.getEncodingService().convertEncodingToRubyEncoding(encoding);
      rubyMessage = (RubyString) rubyMessage.encode(context, rubyEncoding);
    }
    Block eBlock = Block.NULL_BLOCK;
    RubyObject exception = (RubyObject) klass.newInstance(context, rubyMessage, eBlock);
    exception.setInstanceVariable("@connection", this);
    return new RaiseException((RubyException) exception);
  }

  RaiseException newPgError(ThreadContext context, String message, ResultSet result) {
    String sqlstate = null;
    if(result != null) {
      sqlstate = result.getErrorField(ErrorField.PG_DIAG_SQLSTATE.getCode());
    }

    Encoding encoding = getClientEncodingAsJavaEncoding(context);
    RaiseException error = newPgErrorCommon(context, message, sqlstate, encoding);
    IRubyObject rubyResult = result == null ? context.nil : createResult(context, result);
    error.getException().setInstanceVariable("@result", rubyResult);
    return error;
  }

  RaiseException newPgError(ThreadContext context, Exception ex, ResultSet result) {
    return newPgErrorCommon(context, ex.getLocalizedMessage(), null, getClientEncodingAsJavaEncoding(context));
  }

  private IRubyObject connectSync(ThreadContext context) {
    try {
      // to make testing possible
      // connection = (BaseConnection)driver.connect(connectionString, props);
      postgresConnection = PostgresqlConnection.connectDb(props);
    } catch(IOException ex) {
      throw newPgError(context, ex, null);
    }

    if(postgresConnection.getStatus() == ConnectionStatus.CONNECTION_BAD) {
      throw newPgErrorCommon(context, postgresConnection.getErrorMessage(),
                             "ConnectionBad", null);
    }

    // set the encoding if the default internal_encoding is set
    set_default_encoding(context);

    return context.nil;
  }

  private Format getFormat(ThreadContext context, IRubyObject [] args) {
    Format resultFormat = Format.Text;
    if(args.length == 3) {
      resultFormat = ((RubyFixnum) args[2]).getLongValue() == 1 ? Format.Binary : Format.Text;
    }
    return resultFormat;
  }

  private PostgresqlConnection getConnection(ThreadContext context) {
    if(postgresConnection != null && !postgresConnection.closed()) {
      return postgresConnection;
    }
    throw newPgErrorCommon(context, "connection is closed", "ConnectionBad",
                           getClientEncodingAsJavaEncoding(context));
  }

  private void fillValuesAndFormat(ThreadContext context, RubyArray params, Value[] values, int [] oids) {
    RubySymbol value_s = context.runtime.newSymbol("value");
    RubySymbol type_s = context.runtime.newSymbol("type");
    RubySymbol format_s = context.runtime.newSymbol("format");
    for(int i = 0; i < params.getLength(); i++) {
      IRubyObject param = params.entry(i);
      Format valueFormat = Format.Text;
      if(param.isNil()) {
        values[i] = new Value(null, valueFormat);
      } else if(param instanceof RubyHash) {
        RubyHash hash = (RubyHash) params.get(i);
        IRubyObject value = hash.op_aref(context, value_s);
        IRubyObject type = hash.op_aref(context, type_s);
        IRubyObject format = hash.op_aref(context, format_s);
        if(!type.isNil()) {
          oids[i] = (int)((RubyFixnum) type).getLongValue();
        }
        if(!format.isNil()) {
          valueFormat = ((RubyFixnum) format).getLongValue() == 1 ? Format.Binary : Format.Text;
        }
        if(value.isNil()) {
          values[i] = new Value(null, valueFormat);
        } else {
          RubyString str = value.asString();
          values[i] = new Value(str.getBytes(), valueFormat);
        }
      } else {
        RubyString str = param.asString();
        values[i] = new Value(str.getBytes(), valueFormat);
      }
    }
  }

  private IRubyObject createResult(ThreadContext context, ResultSet set) {
    if(set == null) {
      return context.nil;
    }
    Encoding encoding = getClientEncodingAsJavaEncoding(context);
    RubyClass klass = (RubyClass)context.runtime.getClassFromPath("PG::Result");
    return new Result(context.runtime, klass, this, set, encoding);
  }

  private ResultSet execPreparedCommon(ThreadContext context, IRubyObject[] args, boolean async) throws IOException {
    PostgresqlString queryName = rubyStringAsPostgresqlString(args[0]);
    Value[] values;
    int[] oids;
    if(args.length > 1) {
      RubyArray array = (RubyArray) args[1];
      values = new Value[array.getLength()];
      oids = new int[array.getLength()];
      fillValuesAndFormat(context, array, values, oids);
    } else {
      values = new Value[0];
      oids = new int[0];
    }
    Format format = getFormat(context, args);
    if(!async) {
      return postgresConnection.execPrepared(queryName, values, format);
    }
    postgresConnection.sendQueryPrepared(queryName, values, format);
    return null;
  }

  private PostgresqlString rubyStringAsPostgresqlString(IRubyObject str) {
    return new PostgresqlString(str.convertToString().getBytes());
  }

  private IRubyObject setClientEncodingCommon(ThreadContext context, String encoding) throws IOException  {
    postgresConnection.setClientEncoding(encoding);
    return context.nil;
  }

  private Encoding getClientEncodingAsJavaEncoding(ThreadContext context) {
    IRubyObject encoding = internal_encoding(context);
    if(encoding.isNil()) {
      return null;
    }
    return ((RubyEncoding) encoding).getEncoding();
  }

  private IRubyObject findEncoding(ThreadContext context, String encodingName) {
    IRubyObject rubyEncodingName = encodingName == null ? context.nil : context.runtime.newString(encodingName);
    return findEncoding(context, rubyEncodingName);
  }

  private IRubyObject findEncoding(ThreadContext context, IRubyObject encodingName) {
    IRubyObject encoding = context.nil;
    try {
      if(!encodingName.isNil()) {
        encoding = context.runtime.getClass("Encoding").callMethod("find", encodingName);
      }
    } catch(RuntimeException e) {
    }
    if(encoding.isNil()) {
      encoding = context.runtime.getClass("Encoding").getConstant("ASCII_8BIT");
    }
    return encoding;
  }
}
