require 'rspec'
require 'spec/lib/helpers'
require 'pg'

describe PG::Connection do
  it 'assumes standard conforming strings is off before any connection is created' do
    foo = "\x00"
    PG::Connection.escape_bytea(foo).should== "\\\\000"
    @conn = setup_testing_db( "PG_Connection" )
    @conn.exec 'SET standard_conforming_strings = on'
    PG::Connection.escape_bytea(foo).should== "\\000"
    @conn.exec 'SET standard_conforming_strings = off'
    PG::Connection.escape_bytea(foo).should== "\\\\000"
		teardown_testing_db( @conn )
  end

  describe 'prepared statements' do
    before(:all) do
      @conn = setup_testing_db( "PG_Connection" )
    end

    after(:all) do
      teardown_testing_db( @conn )
    end

    it 'execute successfully' do
      @conn.prepare 'query', 'SELECT 1 AS n'
      res = @conn.exec_prepared 'query'
      res[0]['n'].should== '1'
    end

    it 'execute successfully with parameters' do
      @conn.prepare 'query', 'SELECT $1 AS n'
      res = @conn.exec_prepared 'query', ['foo']
      res[0]['n'].should== 'foo'
    end
  end
end
