package com.headius.jruby.pg_ext;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.postgresql.PGConnection;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

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

    @JRubyMethod(rest = true)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        String host = "localhost";
        String dbname = null;
        int port = 5740;
        String user = "root";
        String password = "";

        if (args.length >= 1 && args[0] instanceof RubyHash) {
            RubyHash hash = (RubyHash)args[0];

            host = (String)hash.get("host");
            dbname = (String)hash.get("dbname");
            user = (String)hash.get("user");
            password = (String)hash.get("password");
            Object portObj = hash.get("port");
            if (portObj != null) port = (int)(long)(Long)portObj;
        }

        try {
            Driver driver = DriverManager.getDriver("jdbc:postgresql");

            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);

            connection = (PGConnection)driver.connect("jdbc:postgresl://" + host + ":" + port + "/" + dbname, props);
//            System.out.println(Arrays.toString(connection.getNotifications()));
            System.out.println(connection);
            jdbcConnection = (java.sql.Connection)connection;
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
        return context.nil;
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
    public IRubyObject status(ThreadContext context) {
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
        return context.nil;
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
            Statement statement = jdbcConnection.createStatement();
            statement.execute(query);

            set = statement.getResultSet();
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

    @JRubyMethod
    public IRubyObject transaction(ThreadContext context) {
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

    @JRubyMethod(name = {"lo_creat", "locreat"}, rest = true)
    public IRubyObject lo_creat(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_create", "locreate"})
    public IRubyObject lo_create(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_import", "loimport"})
    public IRubyObject lo_import(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_export", "loexport"})
    public IRubyObject lo_export(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_open", "loopen"}, rest = true)
    public IRubyObject lo_open(ThreadContext context, IRubyObject[] args) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_write", "lowrite"})
    public IRubyObject lo_write(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_read", "loread"})
    public IRubyObject lo_read(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
    }

    @JRubyMethod(name = {"lo_lseek", "lolseek", "lo_seek", "loseek"})
    public IRubyObject lo_lseek(ThreadContext context, IRubyObject arg0, IRubyObject arg1, IRubyObject arg2) {
        return context.nil;
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

    java.sql.Connection jdbcConnection;
    PGConnection connection;
}
