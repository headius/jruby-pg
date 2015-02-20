package org.jruby.pg.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.jruby.pg.internal.ResultSet.ResultStatus;
import org.jruby.pg.io.FlushableByteChannel;
import org.jruby.pg.io.HandshakeStatus;
import org.jruby.pg.io.SecureByteChannel;
import org.jruby.pg.io.SocketByteChannel;
import org.jruby.pg.messages.*;
import org.jruby.pg.messages.Close.StatementType;
import org.jruby.pg.messages.ErrorResponse.ErrorField;
import org.jruby.pg.messages.ProtocolMessage.MessageType;

/**
 * postgresql connection
 */
public class PostgresqlConnection implements ProtocolReader, ProtocolWriter {
  // this hashmap holds the static status of the parameters, the last
  // connection wins
  private static final Map<String, String> staticParameters =
    new ConcurrentHashMap<String, String>();

  // status fields
  private ConnectionStatus cStatus;

  // socket channel and optionally a SecureByteBufferStream
  private SocketChannel socket;
  private FlushableByteChannel channel;

  // enable or disable ssl on this connection, this isn't a final var
  // since the server could refuse SSL in which case the connection is
  // not secure if the user didn't set `sslmode' to `required'
  private boolean trySSL;

  // socket, optional SslEngine and buffers. outBuffer and inBuffer
  // are always ready to do put() operations on
  private ByteBuffer outBuffer, inBuffer;

  // the BackendKeyData, i.e. the pid of the server and the secret
  // used to cancel queries
  private BackendKeyData bkd;

  // store the original properties of the connection
  private final Properties props;

  // sotre the parameter status
  private final Map<String, String> parameters = new HashMap<String, String>();

  // store the next result that's being sent
  private ResultSet result;

  // store the nextResult, this is used in SingleRowMode
  private ResultSet nextResult;

  // store the notifications that are received
  private LinkedList<NotificationResponse> notifications =
    new LinkedList<NotificationResponse>();

  // true if singleRowMode is on
  private boolean singleRowMode;

  // the last query that was executed, this can
  // be used when reporting errors, but is currently
  // not used
  @SuppressWarnings("unused")
  private PostgresqlString lastQuery;

  // the class of the query currently being executed
  private QueryClass qClass;

  // the status of the connection
  private AsyncStatus aStatus;

  // the transaction status
  private TransactionStatus xStatus;

  // holds error messages from exceptions that aren't propagated back
  // to the caller
  private String errorMessage;

  // this is set if we opened this connection to cancel a request
  private boolean forCancel;

  // used in ping() to determine if the connection failed due to
  // invalid credentials
  private boolean authReqReceived;

  // used in ping() to determine the error that the server returned
  private String lastSqlState;

  // used in ping() to determine the reason of the failure if it's
  // caused by invalid input
  private boolean invalidInput;

  // store the non blocking mode of the connection
  private boolean nonBlocking;

  // holds the length's position of the current message being sent,
  // and the position of the first byte. this is used by writeMsgEnd
  // to write the final size
  private int lengthPosition = -1, firstPosition = -1;

  // used to trace the activity of the connection
  private PrintWriter tracer;

  // the current notice receiver, defaults to printing the error
  // message to standard error
  private NoticeReceiver receiver = new NoticeReceiver() {
    public void receive(ResultSet result) {
      System.err.println(result.getError());
    }
  };

  /**
   * Create a new connection asynchronously using the specified
   * parameters
   *
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static PostgresqlConnection connectStart(Properties props)
  throws IOException, GeneralSecurityException {
    PostgresqlConnection conn = new PostgresqlConnection(props);
    conn.connect();
    return conn;
  }

  /**
   * Create a new connection synchronously using the specified
   * parameters
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws KeyManagementException
   */
  public static PostgresqlConnection connectDb(Properties props)
  throws IOException {
    return connectDbCommon(props, false);
  }

  /**
   * Try to connect to the database, return if the connection is in bad state,
   * will block and the connection is non blocking or the connection is
   * established.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public PollingStatus connectPoll() {
    try {
      return connectPollInternal();
    } catch(IOException ex) {
      errorMessage = ex.getLocalizedMessage();
      cStatus = ConnectionStatus.CONNECTION_BAD;
      return PollingStatus.PGRES_POLLING_FAILED;
    } catch(GeneralSecurityException ex) {
      errorMessage = ex.getLocalizedMessage();
      cStatus = ConnectionStatus.CONNECTION_BAD;
      return PollingStatus.PGRES_POLLING_FAILED;
    } catch(NumberFormatException ex) {
      invalidInput = true;
      errorMessage = ex.getLocalizedMessage();
      cStatus = ConnectionStatus.CONNECTION_BAD;
      return PollingStatus.PGRES_POLLING_FAILED;
    }
  }

  public static PingStatus ping(Properties props) throws IOException {
    PostgresqlConnection conn = null;
    try {
      conn = PostgresqlConnection.connectDb(props);
      if(conn.invalidInput) {
        return PingStatus.PQPING_NO_ATTEMPT;
      }

      if(conn.cStatus != ConnectionStatus.CONNECTION_BAD) {
        return PingStatus.PQPING_OK;
      }

      if(conn.authReqReceived) {
        return PingStatus.PQPING_OK;
      }

      if(conn.lastSqlState == null || conn.lastSqlState.length() != 5) {
        return PingStatus.PQPING_NO_RESPONSE;
      }

      if(conn.lastSqlState.equals(ErrorResponse.ERRCODE_CANNOT_CONNECT_NOW)) {
        return PingStatus.PQPING_REJECT;
      }

      return PingStatus.PQPING_OK;
    } catch(IOException ex) {
      return PingStatus.PQPING_NO_RESPONSE;
    }
    finally {
      if(conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Close the connection and release all its resources
   */
  public void close() throws IOException {
    if(closed()) {
      return;
    }

    // configure blocking since we have to send the Terminate message
    // anyway
    cStatus = ConnectionStatus.CONNECTION_BAD;
    socket.configureBlocking(true);
    sendMessage(new Terminate());
    flush();
    channel.close();
  }

  /**
   * Return true if the connection is closed
   */
  public boolean closed() {
    return cStatus == ConnectionStatus.CONNECTION_BAD;
  }

  /**
   * Return the connection status, {@link ConnectionStatus}
   */
  public ConnectionStatus getStatus() {
    return cStatus;
  }

  /**
   * Return the current transaction status
   */
  public TransactionStatus getTransactionStatus() {
    if(cStatus != ConnectionStatus.CONNECTION_OK) {
      return TransactionStatus.PQTRANS_UNKNOWN;
    }

    if(aStatus != AsyncStatus.Idle) {
      return TransactionStatus.PQTRANS_ACTIVE;
    }

    return xStatus;
  }

  /**
   * Return the value of the given parameters
   */
  public String getParameterStatus(String name) {
    return parameters.get(name);
  }

  /**
   * Return the socket backing up this connection
   */
  public SelectableChannel getSocket() {
    return socket;
  }

  /**
   * Return the pid received from the server on startup
   */
  public int getBackendPid() {
    return bkd.getPid();
  }

  /**
   * Equivalent to getParameterStatus("server_version")
   */
  public int getServerVersion() {
    return getServerVersion(parameters);
  }

  /**
   * Execute a query synchronously. The query string can contain more
   * than one statement. This method will return the ResultSet of the
   * last statement.
   */
  public ResultSet exec(PostgresqlString query) throws IOException {
    execStart();
    sendQuery(query);
    return execFinish();
  }

  /**
   * Execute a query synchronously. The query string must have one
   * statement only.
   */
  public ResultSet execQueryParams(PostgresqlString query, Value[] values,
                                   Format format, int[] oids) throws IOException {
    execStart();
    sendQueryParams(query, values, format, oids);
    return execFinish();
  }

  /**
   * Create a prepared statement with the given query and parameters
   *
   * @throws IOException
   */
  public ResultSet prepare(PostgresqlString name, PostgresqlString query,
                           int[] oids) throws IOException {
    execStart();
    sendPrepare(name, query, oids);
    return execFinish();
  }

  /**
   * Obtain information about the given prepared statement
   *
   * @throws IOException
   */
  public ResultSet describePrepared(PostgresqlString queryName) throws IOException {
    execStart();
    sendDescribe(queryName, StatementType.Prepared);
    return execFinish();
  }

  /**
   * Obtain information about the given portal
   *
   * @throws IOException
   */
  public ResultSet describePortal(PostgresqlString name) throws IOException {
    execStart();
    sendDescribe(name, StatementType.Portal);
    return execFinish();
  }

  /**
   * Execute the given prepared statement using the given values
   *
   * @throws IOException
   */
  public ResultSet execPrepared(PostgresqlString queryName, Value[] values,
                                Format format) throws IOException {
    execStart();
    sendQueryPrepared(queryName, values, format);
    return execFinish();
  }

  /**
   * Execute a query asynchronously
   *
   * @return true if the query was successfully submitted, false
   *         otherwise
   * @throws IOException
   */
  public boolean sendQuery(PostgresqlString query) throws IOException {
    sendQueryStart();

    // add the query message to the output buffer
    sendMessage(new Query(query));

    // remember the query and the state we're in
    qClass = QueryClass.Simple;
    aStatus = AsyncStatus.Busy;
    lastQuery = query;

    // try to flush the output buffer
    flush();

    return true;
  }

  /**
   * Asynchronous version of {@link execQueryParams}
   *
   * @return true if the query was successfully submitted, false
   *         otherwise
   * @throws IOException
   */
  public boolean sendQueryParams(PostgresqlString query, Value[] values,
                                 Format resultFormat, int[] oids) throws IOException {
    sendQueryStart();

    PostgresqlString empty = new PostgresqlString("");

    // send parse, bind, execute and sync
    sendMessage(new Parse(empty, query, oids));
    sendMessage(new Bind(empty, empty, values, resultFormat));
    sendMessage(new Describe(empty, StatementType.Portal));
    sendMessage(new Execute(empty));
    sendMessage(new Sync());

    // remember the state and the query that we are running
    qClass = QueryClass.Extended;
    aStatus = AsyncStatus.Busy;
    lastQuery = query;

    // try to flush
    flush();
    return true;
  }

  /**
   * Asynchronous version of {@link prepare()}
   *
   * @throws IOException
   */
  public boolean sendPrepare(PostgresqlString name, PostgresqlString query,
                             int[] oids) throws IOException {
    sendQueryStart();

    sendMessage(new Parse(name, query, oids));
    sendMessage(new Sync());

    qClass = QueryClass.Prepare;
    aStatus = AsyncStatus.Busy;
    lastQuery = query;

    flush();

    return true;
  }

  /**
   * Asynchronous version of {@link #describePrepared()}
   *
   * @throws IOException
   */
  public void sendDescribePrepared(PostgresqlString stmtName) throws IOException {
    sendDescribe(stmtName, StatementType.Prepared);
  }

  /**
   * Asynchronous version of {@link #describePortal()}
   *
   * @throws IOException
   */
  public void sendDescribePortal(PostgresqlString portal) throws IOException {
    sendDescribe(portal, StatementType.Portal);
  }

  /**
   * Asynchronous version of {@link #execPrepared()}
   *
   * @throws IOException
   */
  public boolean sendQueryPrepared(PostgresqlString name, Value[] values,
                                   Format format) throws IOException {
    PostgresqlString empty = new PostgresqlString("");
    sendMessage(new Bind(empty, name, values, format));
    sendMessage(new Describe(empty, StatementType.Portal));
    sendMessage(new Execute(empty));
    sendMessage(new Sync());

    qClass = QueryClass.Extended;
    aStatus = AsyncStatus.Busy;
    lastQuery = null;

    // try to flush
    flush();
    return true;
  }

  /**
   * Return the next ResultSet. This method will block until the
   * ResultSet is ready
   */
  public ResultSet getResult() throws IOException {
    Selector selector = Selector.open();
    SelectionKey key = socket.register(selector, 0);

    // while we are busy keep reading more data
    while(isBusy()) {
      // wait until we send all the data in the output buffer
      while(!flush()) {
        key.interestOps(SelectionKey.OP_WRITE);
        selector.select();
      }

      key.interestOps(SelectionKey.OP_READ);
      selector.select();
      consumeInput();
    }

    selector.close();

    switch(aStatus) {
    case Idle:
      return null;

    case Ready:
      ResultSet res = prepareAsyncResult();
      aStatus = AsyncStatus.Busy;
      return res;

    case CopyIn:
      if(result != null && result.getStatus() == ResultStatus.PGRES_COPY_IN) {
        return prepareAsyncResult();
      }
      return makeEmptyResult(ResultStatus.PGRES_COPY_IN);

    case CopyOut:
      if(result != null && result.getStatus() == ResultStatus.PGRES_COPY_OUT) {
        return prepareAsyncResult();
      }
      return makeEmptyResult(ResultStatus.PGRES_COPY_OUT);

    case CopyBoth:
      if(result != null && result.getStatus() == ResultStatus.PGRES_COPY_BOTH) {
        return prepareAsyncResult();
      }
      return makeEmptyResult(ResultStatus.PGRES_COPY_BOTH);

    default:
      aStatus = AsyncStatus.Ready;
      return makeEmptyResult(ResultStatus.PGRES_FATAL_ERROR).
             appendErrorMessage("unexpected aStatus: " + aStatus);
    }
  }

  /**
   * Consume more data from the socket until it has no more data to be
   * read. After a call to this method the socket shouldn't be ready
   * for read. This method will only buffer the data and will not
   * attempt to parse it. Parsing happen in isBusy() or getResult().
   *
   * @throws IOException
   */
  public void consumeInput() throws IOException {
    // make sure we are connected before attempting to read from the
    // socket, we can't do `cStatus != CONNECTION_OK` here since this
    // method is used during startup
    if(channel == null) {
      throw new IOException("not connected");
    }

    // flush the output buffer, otherwise we might be waiting for a
    // response to something that the server didn't receive yet
    flush();

    do {
      // make the inBuffer ready for reading from the channel
      if(!inBuffer.hasRemaining()) {
        // if the inBuffer doesn't have enough data, double the capacity
        ByteBuffer newInBuffer = ByteBuffer.allocate(inBuffer.capacity() * 2);
        inBuffer.flip();
        newInBuffer.put(inBuffer);
        inBuffer = newInBuffer;
      }
      // as long as the channel is returning data keep going,
      // otherwise return
    } while(channel.read(inBuffer) > 0);
  }

  /**
   * Return true if a call to {@link #getResult} will block, false
   * otherwise.
   *
   * @throws IOException
   */
  public boolean isBusy() throws IOException {
    parseInput();
    return aStatus == AsyncStatus.Busy;
  }

  /**
   * Set single row mode to true. This method has to be called after
   * the starting a query asynchronously and before any results are
   * retrieved using {@link #getResult}
   */
  public void setSingleRowMode() throws IOException {
    if(aStatus != AsyncStatus.Busy) {
      throw new IOException("connection not busy");
    }

    if(qClass != QueryClass.Simple && qClass != QueryClass.Extended) {
      throw new IOException("connection isn't executing a query");
    }

    if(result != null) {
      throw new IOException("result set already received");
    }

    singleRowMode = true;
  }

  /**
   * Set non blocking send mode, by default sending messages to the
   * backend are sent synchronously. In non blocking mode the messages
   * are queued in a buffer and the user has to make sure they are
   * sent using {@link #flush}
   *
   * Note: by default a connection is blocking unless this method is
   * called with `true' argument
   */
  public void setNonBlocking(boolean nonBlocking) {
    this.nonBlocking = nonBlocking;
  }

  /**
   * Return true if the connection is in non blocking mode
   */
  public boolean isNonBlocking() {
    return nonBlocking;
  }

  /**
   * Cancel the current request. This method will open
   * a new connection to the backend and send a Cancel
   * request
   *
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public void cancel() throws IOException {
    PostgresqlConnection conn = null;
    try {
      conn = PostgresqlConnection.connectDbCommon(props, true);
      // we should do this synchronously
      conn.socket.configureBlocking(true);
      conn.sendMessage(new CancelRequest(bkd.getPid(), bkd.getSecret()));
      while(!conn.flush()) ;
      // try to close the connection
      // wait for the connection to be closed by the
      // server. That's the only way we have to tell that the
      // server has processed the CancelRequest. Otherwise, we
      // might issue a new query too fast before the previous
      // CancelRequest is processed which will cause the new query
      // to be cancelled.
      conn.channel.read(ByteBuffer.allocate(1));
    } catch(IOException ex) {
      // ignore the exception
    } finally {
      if(conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Flush the output buffer to the socket. If {@link #isNonBlocking}
   * returns true then this method can return before all data is
   * flushed. The return value of this method should
   *
   * @return true if all the data has been written to the socket,
   * false otherwise.
   */
  public boolean flush() throws IOException {
    boolean done;
    do {
      outBuffer.flip();
      if(outBuffer.hasRemaining()) {
        channel.write(outBuffer);
      }
      outBuffer.compact();
      done = outBuffer.position() == 0 && channel.flush();
    } while(!done && !nonBlocking) ;
    return done;
  }

  /**
   * Return pending notifications on this connection. This method
   * doesn't read any data from the connection, use consumeInput() to
   * actually read data from the connection if the connection is idle.
   *
   * @throws IOException
   */
  public NotificationResponse notifies() throws IOException {
    parseInput();

    if(notifications.isEmpty()) {
      return null;
    }
    return notifications.pop();
  }

  /**
   * Send the copy data to the server during CopyIn state
   *
   * @param data
   *
   * @throws IOException
   */
  public void putCopyData(byte[] data) throws IOException {
    if(aStatus != AsyncStatus.CopyIn && aStatus != AsyncStatus.CopyBoth) {
      throw new UnsupportedOperationException("not in copy in/both state");
    }

    // make sure we read all the notice or notify messages that could
    // be accumulating during the copy
    parseInput();

    sendMessage(new CopyData(data));

    // try to flush the data
    flush();
  }

  /**
   * End the copy in mode, by sending either a CopyDone or CopyFail
   * depending on whether err is null or not (respectively).
   *
   * @throws IOException
   */
  public void putCopyEnd(String err) throws IOException {
    if(aStatus != AsyncStatus.CopyIn && aStatus != AsyncStatus.CopyBoth) {
      throw new UnsupportedOperationException("not in copy in/both state");
    }

    if(err != null) {
      sendMessage(new CopyFail(err));
    } else {
      sendMessage(new CopyDone());
    }

    // if we're not in simple mode we must resend the Sync since the
    // first one was ignored during the CopyIn mode
    if(qClass != QueryClass.Simple) {
      sendMessage(new Sync());
    }

    // prepare the result for the next result
    result = null;

    // if we are in CopyBoth then remove the CopyIn part
    if(aStatus == AsyncStatus.CopyBoth) {
      aStatus = AsyncStatus.CopyOut;
    } else {
      aStatus = AsyncStatus.Busy;
    }

    flush();
  }

  /**
   * Return CopyData messages from the connection. This is used during
   * CopyOut mode to receive data from the server
   *
   * @return the row data as a byte array. a zero byte array will be
   *         returned if the data isn't ready and async is set to true
   *         or null if all data has been received.
   */
  public byte[] getCopyData(boolean async) throws IOException {
    if(aStatus != AsyncStatus.CopyBoth &&
    aStatus != AsyncStatus.CopyOut) {
      throw new IOException("not in copy out mode");
    }

    for(;;) {
      // try to read some data
      consumeInput();

      if(!hasAsynchronousMessage() && !hasCopyMessage()) {
        // any other message should terminate the CopyOut and/or
        // CopyBoth modes
        aStatus = AsyncStatus.Busy;
        return null;
      }

      ProtocolMessage msg = getMessage();
      if(msg == null) {
        if(async) {
          return new byte[0];
        }

        // if we're waiting synchronously, then block until the socket
        // is ready for read
        Selector selector = Selector.open();
        socket.register(selector, SelectionKey.OP_READ);
        selector.select();
        selector.close();
        continue;
      }

      switch(msg.getType()) {

      case ParameterStatus:
      case NotificationResponse:
      case NoticeResponse:
        handleAsyncMessage(msg);
        break;

      case CopyData:
        return ((CopyData)msg).getValue();

      case CopyDone:
        if(aStatus == AsyncStatus.CopyBoth) {
          aStatus = AsyncStatus.CopyIn;
        } else {
          aStatus = AsyncStatus.Busy;
        }
        result = null;
        return null;

      default:
        throw new IOException("unexpected meessage received");
      }
    }
  }

  /**
   * Create a {@link LargeObjectAPI} that uses this connection
   */
  public LargeObjectAPI getLargeObjectAPI() {
    return new LargeObjectAPI(this);
  }

  public String getClientEncoding() {
    return parameters.get("client_encoding");
  }

  public void setClientEncoding(String encoding) throws IOException {
    String query = "SET client_encoding TO '" + encoding + "'";
    ResultSet res = exec(new PostgresqlString(query));
    if(res.getStatus() != ResultStatus.PGRES_COMMAND_OK) {
      throw new IOException("changing encoding failed");
    }
  }

  /**
   * MD5 encrypt the password using the given salt
   *
   * @param password
   * @param salt
   * @return
   * @throws NoSuchAlgorithmException
   */
  public static byte[] encrypt(byte[] password, byte[] salt)
  throws NoSuchAlgorithmException {
    return encrypt(password, 0, salt);
  }

  public static String escapeStringStatic(String string) {
    return escapeStringInternal(string, staticParameters);
  }

  public String escapeString(String string) {
    return escapeStringInternal(string, parameters);
  }

  public static byte[] escapeBytesStatic(byte[] bytes) {
    boolean conformingStrings = getStandardConformingStrings(staticParameters);
    return escapeBytesInternal(bytes, conformingStrings, false);
  }

  public byte[] escapeBytes(byte[] bytes) {
    boolean conformingStrings = getStandardConformingStrings(parameters);
    return escapeBytesInternal(bytes, conformingStrings, getServerVersion() >= 9000);
  }

  public static String escapeIdentifier(String ident) {
    return escapeInternal(ident, true);
  }

  public static String escapeLiteral(String literal) {
    return escapeInternal(literal, false);
  }

  /**
   * Get the last error on this connection
   * @return
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Create a new empty result with the given status
   */
  public ResultSet makeEmptyResult(ResultStatus status) {
    return new ResultSet().setStatus(status);
  }

  /**
   * Set the given writer as the tracer of the connection. The writer
   * will receive strings describing messages as they are
   * sent/received to/from the backend
   */
  public void trace(Writer tracer) {
    untrace();
    this.tracer = new PrintWriter(tracer, true);
  }

  /**
   * Remove any previously added tracer
   */
  public void untrace() {
    if(this.tracer != null) {
      this.tracer.flush();
      this.tracer = null;
    }
  }

  /**
   * Set the notice receiver
   */
  public NoticeReceiver setNoticeReceiver(NoticeReceiver receiver) {
    NoticeReceiver oldReceiver = this.receiver;
    if(receiver != null) {
      this.receiver = receiver;
    }
    return oldReceiver;
  }

  // ProtocolReader methods

  // Note for all ProtocolReader methods, the buffer is assumed to be
  // ready for read here, i.e. it's flipped.

  public boolean hasCompleteMessage() {
    if(inBuffer.remaining() < 5) {
      return false;
    }

    try {
      inBuffer.mark();
      // get the type of the message
      inBuffer.get();
      int len = inBuffer.getInt();
      if(inBuffer.remaining() < len - 4) {
        // there isn't enough data, return for now
        return false;
      }
      return true;
    }
    finally {
      inBuffer.reset();
    }
  }

  public byte getByte() {
    byte b = inBuffer.get();
    if(tracer != null) {
      tracer.printf("From backend> %c\n", b);
    }
    return b;
  }

  public int getInt() {
    int i = inBuffer.getInt();
    if(tracer != null) {
      tracer.printf("From backend (#%d)> %d\n", 4, i);
    }
    return i;
  }

  public short getShort() {
    short s = inBuffer.getShort();
    if(tracer != null) {
      tracer.printf("From backend (#%d)> %d\n", 2, s);
    }
    return s;
  }

  public String getString() {
    ByteBuffer slice = inBuffer.slice();
    while(slice.get() != '\0')
      ;
    slice.flip();
    int len = slice.remaining();
    // copy the entire string without the null byte
    String s = new String(slice.array(), slice.arrayOffset(), len - 1);
    inBuffer.position(inBuffer.position() + len);
    if(tracer != null) {
      tracer.printf("From backend> \"%s\"\n", s);
    }
    return s;
  }

  public byte[] getNChar(int len) {
    byte[] bytes = new byte[len];
    inBuffer.get(bytes);
    if(tracer != null) {
      tracer.printf("From backend (%d)> %s\n", bytes.length, new String(bytes));
    }
    return bytes;
  }

  // ProtocolWriter methods

  public void writeMsgStart(byte b) {
    if(lengthPosition >= 0) {
      throw new RuntimeException("lengthPosition should be negative");
    }
    int requiredLength = 4;
    // if b is 0 then this message type doesn't have the initial byte
    // that identifies its type, e.g. SSLRequest and StartupRequest.
    if(b != 0) {
      requiredLength += 1;
    }
    expandOutputBuffer(requiredLength);
    firstPosition = outBuffer.position();
    if(b != 0) {
      outBuffer.put(b);
    }
    lengthPosition = outBuffer.position();
    // temporary set the length to 0
    outBuffer.putInt(0);
    if(tracer != null) {
      tracer.printf("To backend> Msg %c\n", b);
    }
  }

  public void writeByte(char c) {
    outBuffer.put((byte) c);
    if(tracer != null) {
      tracer.printf("To backend> %c\n", c);
    }
  }

  public void writeInt(int n) {
    expandOutputBuffer(4);
    outBuffer.putInt(n);
    if(tracer != null) {
      tracer.printf("To backend (%d#)> %d\n", 4, n);
    }
  }

  public void writeShort(int s) {
    expandOutputBuffer(2);
    outBuffer.putShort((short) s);
    if(tracer != null) {
      tracer.printf("To backend (%d#)> %d\n", 2, s);
    }
  }

  public void writeNChar(byte[] bytes) {
    expandOutputBuffer(bytes.length);
    outBuffer.put(bytes);
    if(tracer != null) {
      tracer.printf("To backend> %s\n", new String(bytes));
    }
  }

  public void writeString(byte[] bytes) {
    int requiredLength = bytes.length;
    // make sure the last byte is null otherwise, add our own
    if(requiredLength == 0 || bytes[requiredLength - 1] != 0) {
      requiredLength++;
    }
    expandOutputBuffer(requiredLength);
    outBuffer.put(bytes);
    if(requiredLength > bytes.length) {
      outBuffer.put((byte) 0);
    }
    if(tracer != null) {
      tracer.printf("To backend> \"%s\"\n", new String(bytes));
    }
  }

  public void writeString(String s) {
    writeString(s.getBytes());
  }

  public void writeString(PostgresqlString s) {
    writeString(s.getBytes());
  }

  public void writeMsgEnd() {
    if(lengthPosition < 0) {
      throw new RuntimeException("lengthPosition shouldn't be negative");
    }

    int msgLen = outBuffer.position() - lengthPosition;
    int actualLen = outBuffer.position() - firstPosition;
    outBuffer.putInt(lengthPosition, msgLen);
    lengthPosition = -1;
    if(tracer != null) {
      tracer.printf("To backend> Msg complete, length %d\n", actualLen);
    }
  }

  // private methods

  /**
   * Expand the output buffer to guarantee that at least required bytes
   * are available in the output buffer
   */
  private void expandOutputBuffer(int required) {
    int oldCapacity = outBuffer.capacity();
    int oldRemaining = outBuffer.remaining();
    if(oldRemaining < required) {
      // try to double the buffer if this adds up enough space,
      // otherwise add (required - oldRemaining)
      int newCapacity = oldCapacity + Math.max(oldCapacity, required - oldRemaining);
      ByteBuffer newOutBuffer = ByteBuffer.allocate(newCapacity);
      outBuffer.flip();
      newOutBuffer.put(outBuffer);
      outBuffer = newOutBuffer;
    }
  }

  private PollingStatus connectPollInternal()
  throws IOException, GeneralSecurityException {
    for(;;) {
      switch(cStatus) {

      case CONNECTION_NEEDED:
        // start the connection
        String host = Utils.host(props);
        int port = Utils.port(props);
        socket.connect(new InetSocketAddress(host, port));
        cStatus = ConnectionStatus.CONNECTION_STARTED;
        return PollingStatus.PGRES_POLLING_WRITING;

      case CONNECTION_STARTED:
        // waiting for the connection to be established if the
        // connection is established, change the state and try again
        if(socket.finishConnect()) {
          cStatus = ConnectionStatus.CONNECTION_MADE;
          continue;
        }

        // if the connection isn't established wait for the connection
        // to be ready
        return PollingStatus.PGRES_POLLING_WRITING;

      case CONNECTION_MADE:
        // the connection was made and the connection is ready for
        // write. send SSLRequest or StartupRequest depending on the
        // ssl preference
        if(trySSL && channel == null) {
          // if we should try ssl and the channel wasn't created yet,
          // see if the server is willing to use ssl

          new SSLRequest().write(this);
          outBuffer.flip();
          socket.write(outBuffer);
          if(outBuffer.hasRemaining()) {
            // throw an error if we can't send all the data
            throw new IOException("Cannot send SSL negotiation package");
          }
          outBuffer.compact();
          cStatus = ConnectionStatus.CONNECTION_SSL_STARTUP;
          return PollingStatus.PGRES_POLLING_READING;
        } else if(!trySSL && channel == null) {
          // if we shouldn't use ssl and channel isn't created then
          // create a NonSecureByteChannel to wrap our socket
          channel = new SocketByteChannel(socket);
        }

        // if we opened this connection to send a cancel request, stop
        // here and let the caller send the CancelRequest
        if(forCancel) {
          // just return something, to stop the loop in connectDbCommon
          return PollingStatus.PGRES_POLLING_OK;
        }

        // send StartupRequest, usage of encryption has been
        // established at this point
        String user = Utils.user(props);
        String dbname = Utils.dbname(props);
        String options = Utils.options(props);
        sendMessage(new Startup(user, dbname, options));
        // we should be able to send the entire message, otherwise
        // just fail
        if(!flush()) {
          cStatus = ConnectionStatus.CONNECTION_BAD;
          return PollingStatus.PGRES_POLLING_FAILED;
        }
        cStatus = ConnectionStatus.CONNECTION_AWAITING_RESPONSE;
        return PollingStatus.PGRES_POLLING_READING;

      case CONNECTION_SSL_STARTUP:
        if(channel != null) {
          // the SSL handshake has started, keep calling doHandshake
          // until the handshake is done
          return finishHandshake();
        }

        // otherwise, the SSLRequest has been sent, read the server
        // response
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        if(socket.read(oneByte) != 1) {
          throw new IOException("Cannot read server response");
        }

        switch(oneByte.get(0)) {
        case 'S':
          // server ready for SSL handshake, initialize the SSLEngine
          // and start the handshake
          boolean verify = false;
          String sslmode = Utils.ssl(props);
          if(sslmode.equals("verify-ca") || sslmode.equals("verify-full")) {
            verify = true;
          }
          channel = new SecureByteChannel(socket, verify);
          return finishHandshake();

        case 'N':
          // the server doesn't support SSL, use regular connection
          // unless the user required ssl
          if(Utils.ssl(props).equals("required")) {
            throw new IOException("Cannot initiate a SSL connection with server");
          }
          // disable ssl, and continue as if the SSL negotiation
          // never happened
          trySSL = false;
          cStatus = ConnectionStatus.CONNECTION_MADE;
          continue;

        default:
          throw new IOException("Unexpected response to SSLRequest");
        }

      case CONNECTION_AWAITING_RESPONSE:
      case CONNECTION_AUTH_OK:
        // waiting for response to StartupRequest
        return finishConnecting();

      case CONNECTION_OK:
        return PollingStatus.PGRES_POLLING_OK;

      case CONNECTION_BAD:
        return PollingStatus.PGRES_POLLING_FAILED;
      }
    }
  }

  private static int getServerVersion(Map<String, String> params) {
    String value = params.get("server_version");
    if(value == null) {
      return 0;
    }

    String[] parts = value.split("\\.");
    int version = 0;
    for(int i = 0; i < parts.length; i++) {
      version = version * 100 + Integer.parseInt(parts[i]);
    }
    return version;
  }

  private static String escapeInternal(String str, boolean isIdentifier) {
    byte[] bytes = str.getBytes();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    char quoteChar = isIdentifier ? '\"' : '\'';

    out.write(quoteChar);

    for(int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];

      if(b == quoteChar || (!isIdentifier && b == '\\')) {
        // if b is a quote character or a backslack in an identifier
        // then repeat the character twice to escape it
        out.write(b);
      }

      if(b == '\0') {
        break;
      }

      // todo: handle multibyte

      out.write(b);
    }

    out.write(quoteChar);
    try {
      out.close();
    } catch(IOException ex) {
      // todo: set the error message in the connection object
      return null;
    }
    return out.toString();
  }


  private static String escapeStringInternal(String string, Map<String, String> params) {
    StringBuffer out = new StringBuffer();
    char[] chars = string.toCharArray();
    boolean conformingStrings = getStandardConformingStrings(params);
    for(int i = 0 ; i < chars.length; i++) {
      char b = chars[i];
      if(b == '\0') {
        break;
      }
      if(b == '\'' || (b == '\\' && !conformingStrings)) {
        out.append(b);
      }
      out.append(b);
    }

    // todo: handle multibytes

    return out.toString();
  }

  private static byte[] escapeBytesInternal(byte[] bytes, boolean conformingStrings,
      boolean useHex) {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(out);

    if(useHex) {
      if(!conformingStrings) {
        writer.append('\\');
      }
      writer.append("\\x");
    }

    for(int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      // we use c to test if b > 0x7e below, we can't do this test
      // using b, otherwise it will be converted to a negative int if
      // it's > 0x7e and the test will always by false
      char c = (char)(b & 0xFF);

      if(useHex) {
        writer.printf("%02x", b);
      } else if(c < 0x20 || c > 0x7e) {
        if(!conformingStrings) {
          writer.append("\\");
        }
        writer.printf("\\%03o", b);
      } else if(b == '\'') {
        // escape the single quote
        writer.append("\'\'");
      } else if(b == '\\') {
        // escape the backslash
        if(!conformingStrings) {
          // append two backslashes
          writer.append("\\\\");
        }
        // append two backslashes
        writer.append("\\\\");
      } else {
        // all other characters, print as themselves
        writer.write(b);
      }
    }

    writer.close();
    return out.toByteArray();
  }

  /**
   * Return true if the standard_conforming_strings parameter is set
   * to "on", false otherwise. If this method returns true, then the
   * server treats a backslack in string literals literally, e.g.
   * 'foo\0' has five characters, 'foo' plus `\' plus a single quote.
   */
  private static boolean getStandardConformingStrings(Map<String, String> params) {
    String value = params != null ? params.get("standard_conforming_strings") : null;
    if(value != null && value.equals("on")) {
      return true;
    }
    return false;
  }

  /**
   * This method will finish establishing the connection by calling
   * {@link #connectionPoll} until the connection fails or is
   * established.
   *
   * @param forCancel if true then the connection is used to send a
   *        CancelRequest and no StartupRequest will be sent
   */
  private static PostgresqlConnection connectDbCommon(Properties props, boolean forCancel)
  throws IOException {
    // start a connection asynchronously
    PostgresqlConnection conn = new PostgresqlConnection(props);
    conn.forCancel = forCancel;
    conn.connect();

    // while the connection isn't established keep calling connectPoll
    // and wait for the socket to be read/write ready depending on
    // the return value of connectPoll
    Selector selector = Selector.open();
    try {
      SelectionKey key = conn.getSocket().register(selector, 0);
      for(;;) {
        PollingStatus status = conn.connectPoll();
        switch(status) {
        case PGRES_POLLING_WRITING:
          key.interestOps(SelectionKey.OP_WRITE);
          selector.select();
          continue;
        case PGRES_POLLING_READING:
          key.interestOps(SelectionKey.OP_READ);
          selector.select();
          continue;
        case PGRES_POLLING_OK:
        case PGRES_POLLING_FAILED:
          return conn;
        }
      }
    } finally {
      selector.close();
    }
  }

  /**
   * Common code for {@link #sendDescribePortal} and {@link
   * #sendDescribePrepared}
   *
   * @param name the name of the prepared statement or portal
   * @param type whether it's a portal or prepared statement
   * @throws IOException
   */
  private void sendDescribe(PostgresqlString name, StatementType type)
  throws IOException {
    sendMessage(new Describe(name, type));
    sendMessage(new Sync());
    aStatus = AsyncStatus.Busy;
    qClass = QueryClass.Describe;
    lastQuery = null;
    flush();
  }

  /**
   * Called at the beginning of any synchronous exec or prepare method
   * to clear results from previous queries
   */
  private void execStart() throws IOException {
    if(cStatus != ConnectionStatus.CONNECTION_OK) {
      throw new IOException("no connection");
    }

    while(getResult() != null) {
      // todo: make we sure we exit from CopyIn and CopyOut modes
      // properly
    }
  }

  /**
   * Called at the end of a synchronous exec or prepare to wait for
   * all the data to be parsed and for the connection to be in Idle
   * status
   */
  private ResultSet execFinish() throws IOException {
    ResultSet result, lastResult;
    lastResult = null;
    while((result = getResult()) != null) {
      // assign result to lastResult only if lastResult is null or
      // doesn't have an error
      if(lastResult == null || lastResult.getStatus() != ResultStatus.PGRES_FATAL_ERROR) {
        lastResult = result;
      }

      if(lastResult.getStatus() == ResultStatus.PGRES_FATAL_ERROR) {
        // drop the rest of the ResultSets and return the error
        continue;
      }

      if(cStatus == ConnectionStatus.CONNECTION_BAD) {
        break;
      }

      switch(result.getStatus()) {
      case PGRES_COPY_OUT:
      case PGRES_COPY_IN:
      case PGRES_COPY_BOTH:
        // as soon as we see a copy start result we should return it to the user
        return result;

      default:
        // continue getting more results
        break;
      }
    }

    return lastResult;
  }

  /**
   * Called at the beginning of all asynchronous exec or prepare
   * methods to make sure the connection is ok and not busy
   */
  private void sendQueryStart() {
    // if the connection isn't good, return an error
    if(cStatus != ConnectionStatus.CONNECTION_OK) {
      throw new UnsupportedOperationException("Bad connection");
    }

    // if we're not idle, throw an exception
    if(aStatus != AsyncStatus.Idle) {
      throw new UnsupportedOperationException("Busy");
    }

    // clear the state from previous queries
    result = null;
    singleRowMode = false;
  }

  /**
   * Construct a new postgresql connection using the given properties
   */
  private PostgresqlConnection(Properties props) {
    this.props = props;
    inBuffer = ByteBuffer.allocate(4096);
    outBuffer = ByteBuffer.allocate(4096);
    // by default the connection should be blocking on send
    nonBlocking = false;
    cStatus = ConnectionStatus.CONNECTION_NEEDED;
  }

  /**
   * Finishes the SSL handshake
   */
  private PollingStatus finishHandshake() throws IOException {
    HandshakeStatus status = ((SecureByteChannel) channel).doHandshake();
    switch(status) {
    case FINISHED:
      cStatus = ConnectionStatus.CONNECTION_MADE;
      // the CONNECTION_MADE will send a StartupRequest, so we are
      // blocking waiting for a write
      return PollingStatus.PGRES_POLLING_WRITING;
    case WRITING:
      return PollingStatus.PGRES_POLLING_WRITING;
    case READING:
      return PollingStatus.PGRES_POLLING_READING;
    default:
      throw new IOException("Invalid HandshakeStatus: " + status);
    }
  }

  /**
   * Handle message flow after the StartupRequest has been sent
   * @throws IOException
   */
  private PollingStatus finishConnecting() throws IOException {
    // make sure we flush everything in the outBuffer, or block until
    // we are ready
    if(!flush()) {
      return PollingStatus.PGRES_POLLING_WRITING;
    }

    for(;;) {
      ProtocolMessage msg = getMessage();

      if(msg == null) {
        // if there are no messages ready for reading, consume more
        // input
        consumeInput();
        msg = getMessage();
      }

      if(msg == null) {
        // if there are no messages ready, wait for the socket to be
        // ready
        return PollingStatus.PGRES_POLLING_READING;
      }

      switch(msg.getType()) {
      case AuthenticationCleartextPassword:
      case AuthenticationMD5Password:
        // send password
        sendMessage(createAuthenticationMessage(msg));
        if(!flush()) {
          // if not all data was sent then wait for the socket to be
          // write ready
          return PollingStatus.PGRES_POLLING_WRITING;
        }
        // read more messages from the buffer
        continue;

      case AuthenticationOk:
        // authentication succeeded
        cStatus = ConnectionStatus.CONNECTION_AUTH_OK;
        // read more messages from the buffer
        continue;

      case ParameterStatus:
        ParameterStatus ps = (ParameterStatus) msg;
        parameters.put(ps.getName(), ps.getValue());
        staticParameters.put(ps.getName(), ps.getValue());
        continue;

      case BackendKeyData:
        bkd = (BackendKeyData) msg;
        continue;

      case ReadyForQuery:
        // we are now ready for query and the connection has been established
        cStatus = ConnectionStatus.CONNECTION_OK;
        aStatus = AsyncStatus.Idle;
        xStatus = ((ReadyForQuery)msg).getTransactionStatus();
        return PollingStatus.PGRES_POLLING_OK;

      case ErrorResponse:
        // failed for some reason
        cStatus = ConnectionStatus.CONNECTION_BAD;
        lastSqlState = ((ErrorResponse)msg).getErrorField(ErrorField.PG_DIAG_SQLSTATE);
        errorMessage = ((ErrorResponse)msg).getErrorMessage();
        return PollingStatus.PGRES_POLLING_FAILED;

      default:
        // invalid message
        throw new IllegalArgumentException("Unexpected message " + msg.getType());
      }
    }
  }

  /**
   * Create an authentication message based on the Authentication
   * response received from the backend.
   */
  private FrontendMessage createAuthenticationMessage(ProtocolMessage msg)
  throws IOException {
    String password = Utils.password(props);
    String user = Utils.user(props);

    switch(msg.getType()) {
    case AuthenticationCleartextPassword:
      if(password == null) {
        // throw an exception if a password was request and the user
        // didn't provide one
        throw new IOException("no password supplied");
      }
      return new PasswordMessage(password.getBytes());

    case AuthenticationMD5Password:
      if(password == null) {
        // throw an exception if a password was requested and the user
        // didn't provide one
        throw new IOException("no password supplied");
      }

      try {
        AuthenticationMD5Password auth = (AuthenticationMD5Password) msg;
        byte[] firstmd5 = encrypt(password.getBytes(), user.getBytes());
        byte[] finalmd5 = encrypt(firstmd5, 3, auth.getSalt());
        return new PasswordMessage(finalmd5);
      } catch(Exception e) {
        // if I know what I'm doing then we shouldn't be here
        return null;
      }
    default:
      throw new IllegalArgumentException("Unsupported authentication type: " +
                                         msg.getType().name());
    }
  }

  /**
   * Append given message to the outBuffer, this method will not
   * attempt to send the data on the socket. Call {@link flush} to
   * flush the output buffer to the socket
   */
  private void sendMessage(FrontendMessage msg) {
    msg.write(this);
  }

  /**
   * Process messages in the input buffer
   *
   * @throws IOException
   */
  private void parseInput() throws IOException {
    for(;;) {
      // check the first character to see if it's an asynchronous
      // message
      if(hasAsynchronousMessage()) {
        ProtocolMessage msg = getMessage();
        if(msg == null) {
          return;
        }

        // handle async messages regardless of the status of the connection
        handleAsyncMessage(msg);
        continue;
      }

      if(aStatus == AsyncStatus.Idle) {
        // todo: handle ErrorResponse as a notice when we're idle
        return;
      }

      if(aStatus != AsyncStatus.Busy) {
        // don't parse anything unless we're Busy
        return;
      }

      ProtocolMessage msg = getMessage();
      if(msg == null) {
        // if there are no messages in the queue return
        return;
      }

      switch(msg.getType()) {

      case CommandComplete:
        if(result == null) {
          result = makeEmptyResult(ResultStatus.PGRES_COMMAND_OK);
        }
        result.setCmdStatus((CommandComplete) msg);
        aStatus = AsyncStatus.Ready;
        break;

      case ErrorResponse:
        result = makeEmptyResult(ResultStatus.PGRES_FATAL_ERROR);
        result.setErrorResponse((ErrorResponse) msg);
        lastSqlState = ((ErrorResponse)msg).getErrorField(ErrorField.PG_DIAG_SQLSTATE);
        errorMessage = ((ErrorResponse)msg).getErrorMessage();
        aStatus = AsyncStatus.Ready;
        break;

      case ReadyForQuery:
        xStatus = ((ReadyForQuery)msg).getTransactionStatus();
        aStatus = AsyncStatus.Idle;
        break;

      case ParseComplete:
        // if we're just doing a prepare, then we're done. Otherwise,
        // just ignore
        if(qClass == QueryClass.Prepare) {
          if(result == null) {
            result = makeEmptyResult(ResultStatus.PGRES_COMMAND_OK);
          }
          aStatus = AsyncStatus.Ready;
        }
        break;

      case BindComplete:
      case CloseComplete:
        // we can safely ignore these messages
        break;

      case ParameterDescription:
        result = makeEmptyResult(ResultStatus.PGRES_COMMAND_OK);
        result.setParameterDescription((ParameterDescription) msg);
        break;

      case RowDescription:
        // if this isn't a describe query and we already have a result
        // then assume that the new RowDescription without the
        // previous CommandComplete being delivered. This shouldn't
        // happen
        if(result != null && qClass != QueryClass.Describe) {
          throw new IOException("invalid state");
        }

        // if result is null, then we create a new result and set it's
        // row descrption
        if(result == null) {
          // set the proper status, if this is a
          ResultStatus status = qClass == QueryClass.Describe ?
                                ResultStatus.PGRES_COMMAND_OK :
                                ResultStatus.PGRES_TUPLES_OK;
          result = makeEmptyResult(status);
        }

        // get the row descrption
        result.setDescription((RowDescription)msg);

        // if this is a Describe query then we're done
        if(qClass == QueryClass.Describe) {
          aStatus = AsyncStatus.Ready;
        }

        break;

      case NoData:
        // NoData means the query won't return data, thus we won't get
        // a RowDescription
        if(qClass == QueryClass.Describe) {
          if(result == null) {
            result = makeEmptyResult(ResultStatus.PGRES_COMMAND_OK);
          }
          aStatus = AsyncStatus.Ready;
        }
        break;

      case DataRow:
        if(result == null) {
          throw new IOException("invalid result set");
        }

        if(result.getStatus() == ResultStatus.PGRES_FATAL_ERROR) {
          // skip if we reached a fatal error
          continue;
        } else if(result.getStatus() != ResultStatus.PGRES_TUPLES_OK) {
          throw new IOException("received DataRow without RowDescription");
        }

        // in single row mode
        if(singleRowMode) {
          ResultSet res = result.copy();
          nextResult = result;
          result = res;
          res.setStatus(ResultStatus.PGRES_SINGLE_TUPLE);
          aStatus = AsyncStatus.Ready;
        }

        result.appendRow((DataRow) msg);
        break;

      case CopyInResponse:
        getCopyStart((CopyResponse)msg, ResultStatus.PGRES_COPY_IN);
        aStatus = AsyncStatus.CopyIn;
        break;

      case CopyOutResponse:
        getCopyStart((CopyResponse)msg, ResultStatus.PGRES_COPY_OUT);
        aStatus = AsyncStatus.CopyOut;
        break;

        // todo: support copy both response (low priority since it's
        // only used in sreaming data between servers)

      case CopyData:
        // just ignore it, the user must have exited the copy mode too
        // early
        break;

      case CopyDone:
        // this is normally sent at the end of the copy command, we
        // can ignore it since it's followed by CommandComplete:
        break;

      default:
        if(result == null || result.getStatus() != ResultStatus.PGRES_FATAL_ERROR) {
          result = makeEmptyResult(ResultStatus.PGRES_FATAL_ERROR);
        }
        result.appendErrorMessage("unexpected response");
        aStatus = AsyncStatus.Ready;
        break;
      }
    }
  }

  /**
   * Returns true if the type of the next message is of an
   * Asynchronous type, i.e. ParameterStatus, NoticeResponse, or
   * NotificationResponse.
   */
  private boolean hasAsynchronousMessage() {
    ByteBuffer buf = (ByteBuffer) inBuffer.duplicate().flip();
    if(!buf.hasRemaining()) {
      return false;
    }
    byte type = buf.get();
    if(type == MessageType.NoticeResponse.firstByte ||
        type == MessageType.NotificationResponse.firstByte ||
        type == MessageType.ParameterStatus.firstByte) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if the next message on the buffer is of type
   * CopyData or CopyDone
   */
  private boolean hasCopyMessage() {
    ByteBuffer buf = (ByteBuffer) inBuffer.duplicate().flip();
    if(!buf.hasRemaining()) {
      return false;
    }
    byte type = buf.get();
    if(type == MessageType.CopyData.firstByte ||
        type == MessageType.CopyDone.firstByte) {
      return true;
    }
    return false;
  }

  /**
   * Handle asynchoronous messages, i.e. ParameterStatus,
   * NotificationResponse, NoticeResponse
   */
  private void handleAsyncMessage(ProtocolMessage msg) {
    switch(msg.getType()) {
    case ParameterStatus:
      ParameterStatus ps = (ParameterStatus) msg;
      parameters.put(ps.getName(), ps.getValue());
      staticParameters.put(ps.getName(), ps.getValue());
      break;

    case NotificationResponse:
      notifications.add((NotificationResponse)msg);
      break;

    case NoticeResponse:
      ResultSet result = makeEmptyResult(ResultStatus.PGRES_NONFATAL_ERROR);
      result.setErrorResponse((ErrorResponse) msg);
      receiver.receive(result);
      break;

    default:
      break;
    }
  }

  /**
   * Create a new result with the proper row description from the
   * given CopyResponse and the status set to the given status
   */
  private void getCopyStart(CopyResponse resp, ResultStatus status) {
    result = makeEmptyResult(status);
    Format[] formats = resp.getColumnFormats();
    Column[] columns = new Column[formats.length];
    for(int i = 0; i < formats.length; i++) {
      columns[i] = new Column(null, Oid.UNSPECIFIED, 0,
                              Oid.UNSPECIFIED, 0, 0,
                              formats[i].getValue());
    }
    RowDescription description = new RowDescription(columns, columns.length);
    result.setDescription(description);
  }

  /**
   * Return the current result and clear it. If there are no result
   * currently in progress then a new result is created with status
   * {@link ResultStatus.PGRES_FATAL_ERROR}
   */
  private ResultSet prepareAsyncResult() {
    ResultSet res = result;
    if(res == null) {
      res = makeEmptyResult(ResultStatus.PGRES_FATAL_ERROR);
    }

    result = nextResult;
    nextResult = null;
    return res;
  }

  /**
   * Return the next message from the inBuffer or null if there are no
   * messages ready for parsing
   */
  private ProtocolMessage getMessage() {
    inBuffer.flip();
    ProtocolMessage msg = ProtocolMessageParser.parseMessage(this);
    inBuffer.compact();
    return msg;
  }

  /**
   * Start a connection asynchronously, this method shouldn't block.
   * {@link connectPoll} should be used to complete the connection.
   *
   * @throws IOException
   */
  private void connect() throws IOException {
    // if this connection was not created to cancel another request
    // and SSL wasn't explicitly disabled, then try to establish a
    // secure connection
    if(!forCancel && !Utils.ssl(props).equals("disable")) {
      trySSL = true;
    }

    socket = SocketChannel.open();
    socket.configureBlocking(false);
  }

  /**
   * MD5 encrypt the given password using the username as the salt
   */
  private static byte[] encrypt(byte[] password, int offset, byte[] salt)
  throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("MD5");
    digest.update(password, offset, password.length - offset);
    digest.update(salt);
    byte[] md5 = digest.digest();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(out);
    writer.write("md5");
    for(byte b : md5) {
      writer.printf("%02x", b);
    }
    writer.flush();
    return out.toByteArray();
  }
}
