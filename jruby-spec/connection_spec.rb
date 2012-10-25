require 'rspec'
require 'spec/lib/helpers'
require 'pg'

module PG::TestingHelpers
  alias :old_log_and_run :log_and_run

  def certs_directory
    File.expand_path "#{__FILE__}/../../../certs"
  end

	def log_and_run( logpath, *cmd )
    return old_log_and_run logpath, cmd unless cmd.first =~ /initdb/

    tmp = old_log_and_run logpath, cmd
    File.open("#{@test_pgdata}/pg_hba.conf", "w") do |f|
      f.puts "# TYPE    DATABASE        USER            ADDRESS         METHOD"
      f.puts "  hostssl all             ssl             127.0.0.1/32    password"
      f.puts "  host    all             ssl             127.0.0.1/32    reject"
      f.puts "  host    all             password        127.0.0.1/32    password"
      f.puts "  host    all             encrypt         127.0.0.1/32    md5"
      f.puts "  host    all             all             127.0.0.1/32    trust"
    end
    File.open("#{@test_pgdata}/postgresql.conf", "a") do |f|
      f.puts "ssl = yes"
    end
    FileUtils.cp "#{certs_directory}/server.key", @test_pgdata
    FileUtils.cp "#{certs_directory}/server.crt", @test_pgdata
    tmp
  end
end

describe PG::Connection do
  it 'assumes standard conforming strings is off before any connection is created' do
    # make sure that there are no last connections cached
    PG::Connection.reset_last_conn
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

    before( :each ) do
      @conn.exec( 'BEGIN' ) unless example.metadata[:without_transaction]
    end

    after( :each ) do
      @conn.exec( 'ROLLBACK' ) unless example.metadata[:without_transaction]
    end

    after(:all) do
      teardown_testing_db( @conn )
    end

    it 'execute successfully' do
      @conn.prepare '', 'SELECT 1 AS n'
      res = @conn.exec_prepared ''
      res[0]['n'].should== '1'
    end

    it 'execute successfully with parameters' do
      @conn.prepare '', 'SELECT $1::text AS n'
      res = @conn.exec_prepared '', ['foo']
      res[0]['n'].should== 'foo'
    end

    it 'should return an error if a prepared statement is used more than once' do
      expect {
        @conn.prepare 'foo', 'SELECT $1::text AS n'
        @conn.prepare 'foo', 'SELECT $1::text AS n'
      }.to raise_error(PGError, /already exists/i)
    end

    it 'return an error if a parameter is not bound to a type' do
      expect {
        @conn.prepare 'bar', 'SELECT $1 AS n'
      }.to raise_error(PGError, /could not determine/i)
    end

    it 'return an error if a prepared statement does not exist' do
      expect {
        @conn.exec_prepared 'foobar'
      }.to raise_error(PGError, /does not exist/i)
    end

    it 'should maintain a correct state after an error' do
      @conn.exec 'ROLLBACK'

      expect {
        res = @conn.exec 'select * from foo'
      }.to raise_error(PGError, /does not exist/)

      expect {
        res = @conn.exec 'SELECT 1 / 0 AS n'
      }.to raise_error(PGError, /by zero/)
    end

    it 'should correctly accept queries after a query is cancelled' do
      @conn.exec 'ROLLBACK'
      @conn.send_query 'SELECT pg_sleep(1000)'
      @conn.cancel
      res = @conn.get_result
      @conn.exec 'select pg_sleep(1)'
    end

    it 'exec should clear results from previous queries' do
      @conn.exec 'ROLLBACK'
      @conn.send_query 'SELECT pg_sleep(1000)'
      @conn.cancel
      @conn.block
      @conn.exec 'ROLLBACK'
    end

    # FIXME: how does this spec pass in ruby-pg without the last get_last_result
    # not calling get_last_reuslt will leave the connection in a state that
    # doesn't accept new queries and ROLLBACK will fail
    it "described_class#block should allow a timeout" do
      @conn.send_query( "select pg_sleep(3)" )

      start = Time.now
      @conn.block( 0.1 )
      finish = Time.now

      (finish - start).should be_within( 0.05 ).of( 0.1 )
    end

    it 'correctly translates the server version' do
      @conn.server_version.should >=(80200)
    end

    it 'quotes identifier correctly' do
      table_name = @conn.quote_ident('foo')
      column_name = @conn.quote_ident('bar')
      @conn.exec "CREATE TABLE #{table_name} (#{column_name} text)"
    end

    it 'quotes identifier correctly when the static quote_ident is called' do
      table_name = PG::Connection.quote_ident('foo')
      column_name = PG::Connection.quote_ident('bar')
      @conn.exec "CREATE TABLE #{table_name} (#{column_name} text)"
    end

    it 'returns an empty result set when an INSERT is executed' do
      res = @conn.exec 'CREATE TABLE foo (bar INT)'
      res.should_not be_nil
      res = @conn.exec 'INSERT INTO foo VALUES (1234)'
      res.should_not be_nil
      res.nfields.should ==(0)
    end

    it 'delete does not fail' do
      @conn.exec 'CREATE TABLE foo (bar INT)'
      @conn.exec 'INSERT INTO foo VALUES (1234)'
      res = @conn.exec 'DELETE FROM foo WHERE bar = 1234'
      res.should_not be_nil
      res.nfields.should ==(0)
    end

    it 'returns id when a new row is inserted' do
      @conn.exec 'CREATE TABLE foo (id SERIAL UNIQUE, bar INT)'
      @conn.prepare 'query', 'INSERT INTO foo(bar) VALUES ($1) returning id'
      @conn.send_query_prepared 'query', ['1234']
      @conn.block
      res = @conn.get_last_result
      res.should_not be_nil
      res.nfields.should ==(1)
    end

    it 'can authenticate clients using the clear password' do
      @conn.exec 'ROLLBACK'
      begin
        @conn.exec "CREATE USER password WITH PASSWORD 'secret'"
      rescue
        # ignore
      end
      @conn2 = PG.connect "#{@conninfo} user=password password=secret"
    end

    it 'fails if no password was given and a password is required' do
      expect {
        @conn.exec 'ROLLBACK'
        begin
          @conn.exec "CREATE USER password WITH PASSWORD 'secret'"
        rescue
          # ignore
        end
        @conn2 = PG.connect "#{@conninfo} user=password"
      }.to raise_error(RuntimeError, /authentication failed/)
    end

    it 'connects to the server using ssl' do
      @conn.exec 'ROLLBACK'
      begin
        @conn.exec "CREATE USER ssl WITH PASSWORD 'secret'"
      rescue
        # ignore
      end
      @conn2 = PG.connect "#{@conninfo} user=ssl password=secret ssl=require"
    end

    it 'can authenticate clients using the md5 hash' do
      @conn.exec 'ROLLBACK'
      begin
        @conn.exec "CREATE USER encrypt WITH PASSWORD 'md5'"
      rescue
        # ignore
      end
      @conn2 = PG.connect "#{@conninfo} user=encrypt password=md5"
    end

    it 'fails if the user does not exist' do
      expect {
        @conn2 = PG.connect "#{@conninfo} user=nonexistentuser"
      }.to raise_error(RuntimeError, /does not exist/)
    end

    it "handles large object methods properly" do
      fd = oid = 0
      @conn.transaction do
        oid = @conn.lo_create( 0 )
        fd = @conn.lo_open( oid, PG::INV_READ|PG::INV_WRITE )
        count = @conn.lo_write( fd, "foobar" )
        @conn.lo_read( fd, 10 ).should be_nil()
        @conn.lo_tell(fd).should ==(6)
        @conn.lo_lseek( fd, 0, PG::SEEK_SET )
        @conn.lo_tell(fd).should ==(0)
        @conn.lo_read( fd, 10 ).should == 'foobar'
      end

    end
    it "closes large objects properly" do
      @conn.transaction do
        oid = @conn.lo_create( 0 )
        fd = @conn.lo_open( oid, PG::INV_READ|PG::INV_WRITE )
        @conn.lo_close(fd)
        expect {
          @conn.lo_write(fd, 'foo')
        }.to raise_error(PGError)
      end
    end

    it "unlinks large objects properly" do
      @conn.transaction do
        oid = @conn.lo_create( 0 )
        fd = @conn.lo_open( oid, PG::INV_READ|PG::INV_WRITE )
        @conn.lo_unlink(oid)
        expect {
          @conn.lo_open(oid)
        }.to raise_error(PGError)
      end
    end

    it "truncates large objects properly" do
      @conn.transaction do
        oid = @conn.lo_create( 0 )
        fd = @conn.lo_open( oid, PG::INV_READ|PG::INV_WRITE )
        @conn.lo_write(fd, 'foobar')
        @conn.lo_seek(fd, 0, PG::SEEK_SET )
        @conn.lo_read(fd, 10).should ==('foobar')
        @conn.lo_truncate(fd, 3)
        @conn.lo_seek(fd, 0, PG::SEEK_SET )
        @conn.lo_read(fd, 10).should ==('foo')
      end
    end

    it 'can copy data in and out correctly' do
      @conn.exec %{ CREATE TABLE ALTERNATE_PARKING_NYC (
                    Subject text,
                    "Start Date" date,
                    "Start Time" time,
                    "End Date" date,
                    "End Time" time,
                    "All day event" boolean,
                    "Reminder on/off" boolean,
                    "Reminder Date" date,
                    "Reminder Time" time)
                  }
      res = @conn.exec %{ Copy ALTERNATE_PARKING_NYC FROM STDIN WITH CSV HEADER QUOTE AS '"'}
      res.result_status.should == PG::PGRES_COPY_IN
      File.readlines('spec/jruby/data/sample.csv').each do |line|
        @conn.put_copy_data(line)
      end
      @conn.put_copy_end
      @conn.get_last_result
      res = @conn.exec 'select * from ALTERNATE_PARKING_NYC'
      res.ntuples.should == 40
    end
  end
end
