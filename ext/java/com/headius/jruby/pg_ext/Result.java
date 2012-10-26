package com.headius.jruby.pg_ext;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Result extends RubyObject {
    public Result(Ruby ruby, RubyClass rubyClass, ResultSet resultSet) {
        super(ruby, rubyClass);

        this.jdbcResultSet = resultSet;
    }

    public static void define(Ruby ruby, RubyModule pg, RubyModule constants) {
        RubyClass result = pg.defineClassUnder("Result", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);

        result.includeModule(ruby.getEnumerable());
        result.includeModule(constants);

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
        return context.nil;
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

    @JRubyMethod
    public IRubyObject getvalue(ThreadContext context, IRubyObject arg0, IRubyObject arg1) {
        return context.nil;
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

    @JRubyMethod(name = "[]")
    public IRubyObject op_aref(ThreadContext context, IRubyObject arg0) {
        Ruby runtime = context.runtime;
        int index = (int)arg0.convertToInteger().getLongValue();

        try {
            jdbcResultSet.absolute(index);
            ResultSetMetaData metaData = jdbcResultSet.getMetaData();
            return currentRowToHash(runtime, metaData, jdbcResultSet);
        } catch (Exception e) {
            throw context.runtime.newRuntimeError(e.getLocalizedMessage());
        }
    }

    private static RubyHash currentRowToHash(Ruby runtime, ResultSetMetaData metaData, ResultSet resultSet) throws SQLException {
        RubyHash hash = RubyHash.newHash(runtime);

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            hash.put(metaData.getColumnName(i), resultSet.getObject(i));
        }

        return hash;
    }

    @JRubyMethod
    public IRubyObject each(ThreadContext context, Block block) {
        Ruby runtime = context.runtime;
        try {
            ResultSetMetaData metaData = jdbcResultSet.getMetaData();
            while (jdbcResultSet.next()) {
                block.yieldSpecific(context, currentRowToHash(runtime, metaData, jdbcResultSet));
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

    @JRubyMethod
    public IRubyObject values(ThreadContext context) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject column_values(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    @JRubyMethod
    public IRubyObject field_values(ThreadContext context, IRubyObject arg0) {
        return context.nil;
    }

    ResultSet jdbcResultSet;
}
