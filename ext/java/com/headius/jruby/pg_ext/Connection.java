package com.headius.jruby.pg_ext;

import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyException;
import org.jruby.RubyFixnum;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class Connection extends RubyObject {
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

    @JRubyMethod(name = {"escape, escape_string"}, meta = true)
    public static IRubyObject escape_string(ThreadContext context, IRubyObject self, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(meta = true)
    public static IRubyObject escape_bytea(ThreadContext context, IRubyObject self, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(meta = true)
    public static IRubyObject unescape_bytea(ThreadContext context, IRubyObject self, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(meta = true)
    public static IRubyObject encrypt_password(ThreadContext context, IRubyObject self, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
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

    /******     PG::Connection INSTANCE METHODS: Connection Control     ******/

    @JRubyMethod(rest = true, required = 1)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
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

            connection = (PGConnection)driver.connect(connectionString, props);
            jdbcConnection = (java.sql.Connection)connection;
        } catch (SQLException sqle) {
            throw context.runtime.newRuntimeError(sqle.getLocalizedMessage());
        }
        return context.nil;
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
          argumentsHash.put("port", args[last_index + 1].toJava(Integer.class));
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

    @JRubyMethod
    public IRubyObject connect_poll(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"finish", "close"})
    public IRubyObject finish(ThreadContext context) {
      try {
        jdbcConnection.close();
        return context.nil;
      } catch (SQLException e) {
        throw context.runtime.newRuntimeError(e.getLocalizedMessage());
      }
    }

  @JRubyMethod
  public IRubyObject status(ThreadContext context) {
    try {
      if (jdbcConnection.isClosed()) {
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
        DatabaseMetaData metaData = jdbcConnection.getMetaData();
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
    public IRubyObject exec(ThreadContext context, IRubyObject[] args) {
        String query = args[0].convertToString().toString();
        ResultSet set = null;

        try {
            Statement statement = jdbcConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            statement.execute(query);

            set = statement.getResultSet();

            if (set == null)
              return context.nil;
        } catch (SQLException sqle) {
            throw context.runtime.newRuntimeError(sqle.getLocalizedMessage());
        }

        return new Result(context.runtime, (RubyClass)context.runtime.getClassFromPath("PG::Result"), set);
    }

    @JRubyMethod(rest = true)
    public IRubyObject prepare(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(rest = true)
    public IRubyObject exec_prepared(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject describe_prepared(ThreadContext context, IRubyObject arg0) {
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

    @JRubyMethod(name = {"escape_string", "escape"})
    public IRubyObject escape_string(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject escape_literal(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject escape_identifier(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject escape_bytea(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject unescape_bytea(ThreadContext context, IRubyObject arg0) {
        return context.nil;
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

    @JRubyMethod
    public IRubyObject get_client_encoding(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = {"set_client_encoding", "client_encoding="})
    public IRubyObject set_client_encoding(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod()
    public IRubyObject transaction(ThreadContext context, Block block) {
      if (!block.isGiven())
        throw context.runtime.newArgumentError("Must supply block for PG::Connection#transaction");

      try {
        try {
          jdbcConnection.setAutoCommit(false);
          block.call(context);
          jdbcConnection.commit();
        } catch (RuntimeException ex) {
          jdbcConnection.rollback();
          throw ex;
        } finally {
          jdbcConnection.setAutoCommit(true);
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
        throw newPgError(context, "lo_create failed");
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
        throw newPgError(context, e.getLocalizedMessage());
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
        throw newPgError(context, e.getLocalizedMessage());
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
        throw newPgError(context, e.getLocalizedMessage());
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
        throw newPgError(context, e.getLocalizedMessage());
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

    @JRubyMethod
    public IRubyObject internal_encoding(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod(name = "internal_encoding=")
    public IRubyObject internal_encoding_set(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject external_encoding(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject set_default_encoding(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    private static ObjectAllocator CONNECTION_ALLOCATOR = new ObjectAllocator() {
        @Override
        public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
            return new Connection(ruby, rubyClass);
        }
    };

    private RaiseException newPgError(ThreadContext context, String message) {
      RubyClass klass = context.runtime.getModule("PG").getClass("Error");
      IRubyObject exception = klass.newInstance(context, context.runtime.newString(message), null);
      return new RaiseException((RubyException) exception);
    }

    java.sql.Connection jdbcConnection;
    PGConnection connection;
}
