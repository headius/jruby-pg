# -*- coding: utf-8 -*-

require 'rspec'
require 'spec/lib/helpers'
require 'pg'

describe PG::Result do
  before(:all) do
    @conn = setup_testing_db( "PG_Connection" )
  end

  before( :each ) do
    @conn.exec( 'BEGIN' ) unless example.metadata[:without_transaction]
  end

  after( :each ) do
    @conn.exec( 'ROLLBACK' ) unless example.metadata[:without_transaction]
  end

  after(:all) do
    teardown_testing_db( @conn )
  end

  it 'returns the number of rows affected by INSERT and DELETE' do
    @conn.exec 'CREATE TABLE FOO (BAR INT)'
    res = @conn.exec 'INSERT INTO FOO VALUES (1)'
    res.cmd_tuples.should == 1
    @conn.exec 'INSERT INTO FOO VALUES (1)'
    res = @conn.exec 'DELETE FROM FOO'
    res.cmd_tuples.should == 2
  end

  it 'nfields return the correct number of columns' do
    res = @conn.exec 'SELECT 1 as n'
    res.nfields.should== 1
  end

  it 'ftype should return the type oid of the column' do
    res = @conn.exec "SELECT 123::money as n"
    res.ftype(0).should== PG::OID_MONEY
  end

  it 'ftype should return the type oid of the column' do
    res = @conn.exec "SELECT 'foo'::bytea as n"
    res.ftype(0).should== PG::OID_BYTEA
  end

  it 'returns the names of the fields in the result set' do
    res = @conn.exec "Select 1 as n"
    res.fields.should== ['n']
  end

  it 'returns newlines in text fields properly' do
    value = "foo\nbar"
    res = @conn.exec "VALUES ('#{@conn.escape value}')"
    res.getvalue(0, 0).should== value
  end

  it 'escapes multibyte strings properly' do
    value = 'いただきます！'
    res = @conn.exec "VALUES ('#{@conn.escape value}')"
    res.getvalue(0, 0).should== value
  end
end
