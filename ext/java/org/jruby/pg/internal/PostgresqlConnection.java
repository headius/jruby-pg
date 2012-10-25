package org.jruby.pg.internal;

import static org.jruby.pg.internal.PostgresqlConnectionUtils.dbname;
import static org.jruby.pg.internal.PostgresqlConnectionUtils.host;
import static org.jruby.pg.internal.PostgresqlConnectionUtils.password;
import static org.jruby.pg.internal.PostgresqlConnectionUtils.port;
import static org.jruby.pg.internal.PostgresqlConnectionUtils.ssl;
import static org.jruby.pg.internal.PostgresqlConnectionUtils.user;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jruby.pg.internal.ResultSet.ResultStatus;
import org.jruby.pg.internal.io.SecureSocketWrapper;
import org.jruby.pg.internal.io.SocketWrapper;
import org.jruby.pg.internal.messages.AuthenticationMD5Password;
import org.jruby.pg.internal.messages.BackendKeyData;
import org.jruby.pg.internal.messages.Bind;
import org.jruby.pg.internal.messages.CancelRequest;
import org.jruby.pg.internal.messages.Close.StatementType;
import org.jruby.pg.internal.messages.CopyData;
import org.jruby.pg.internal.messages.CopyDone;
import org.jruby.pg.internal.messages.DataRow;
import org.jruby.pg.internal.messages.Describe;
import org.jruby.pg.internal.messages.ErrorResponse;
import org.jruby.pg.internal.messages.Format;
import org.jruby.pg.internal.messages.NotificationResponse;
import org.jruby.pg.internal.messages.ParameterDescription;
import org.jruby.pg.internal.messages.ParameterStatus;
import org.jruby.pg.internal.messages.Parse;
import org.jruby.pg.internal.messages.PasswordMessage;
import org.jruby.pg.internal.messages.ProtocolMessage;
import org.jruby.pg.internal.messages.ProtocolMessage.MessageType;
import org.jruby.pg.internal.messages.ProtocolMessageBuffer;
import org.jruby.pg.internal.messages.Query;
import org.jruby.pg.internal.messages.ReadyForQuery;
import org.jruby.pg.internal.messages.RowDescription;
import org.jruby.pg.internal.messages.SSLRequest;
import org.jruby.pg.internal.messages.Startup;
import org.jruby.pg.internal.messages.Sync;
import org.jruby.pg.internal.messages.Terminate;
import org.jruby.pg.internal.messages.TransactionStatus;

public class PostgresqlConnection {
  public static final CopyData COPY_DATA_NOT_READY = new CopyData(null);

  /** Static API (create a new connection) **/
  public static PostgresqlConnection connectDb(Properties props) throws Exception {
    PostgresqlConnection connection = new PostgresqlConnection(props);
    connection.connect();
    return connection;
  }

  public static PostgresqlConnection connectStart(Properties props) throws IOException, PostgresqlException,
      NoSuchAlgorithmException {
    PostgresqlConnection connection = new PostgresqlConnection(props);
    connection.connectAsync();
    return connection;
  }

  public static byte[] encrypt(byte[] password, int offset, byte[] salt) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("MD5");
    digest.update(password, offset, password.length - offset);
    digest.update(salt);
    byte[] md5 = digest.digest();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(out);
    writer.write("md5");
    for (byte b : md5)
      writer.printf("%02x", b);
    writer.flush();
    return out.toByteArray();
  }

  public static byte[] encrypt(byte[] password, byte[] salt) throws NoSuchAlgorithmException {
    return encrypt(password, 0, salt);
  }

  /** Public API (execute, close, etc.) **/

  public ConnectionState status() {
    return state.simpleState();
  }

  public void consumeInput() throws IOException {
    try {
      if (state.isWrite()) {
        flush();
      } else if (state.isRead()) {
        int read = socket.read(currentInMessage.getBuffer());
        if (read == -1) {
          // connection is closed
          throw new ClosedChannelException();
        }
      } else if (!state.isIdle()) {
        throw new IllegalStateException("Cannot consume input while connection is in " + state.name() + " state");
      }
    } catch (IOException e) {
      state = ConnectionState.Failed;
      throw e;
    }
    changeState();

    if (state.isRead() && !socket.shouldWaitForData()) {
      // we cannot return until the buffer is ready or
      // we're not in the read state anymore, otherwise
      // the user can block using select waiting for data
      // to read while the data is in buffered by out socket
      consumeInput();
    }
  }

  public ConnectionState connectPoll() throws IOException {
    consumeInput();
    return state.pollingState();
  }

  public ResultSet exec(String query) throws IOException, PostgresqlException {
    // ignore any prior results
    getLastResult();
    sendQuery(query);
    return getLastResultThrowError();
  }

  public ResultSet execQueryParams(String query, Value[] values, Format format, int[] oids) throws IOException,
      PostgresqlException {
    // ignore any prior results
    getLastResult();
    sendQueryParams(query, values, format, oids);
    return getLastResultThrowError();
  }

  public ResultSet execPrepared(String name, Value[] values, Format format) throws IOException, PostgresqlException {
    // ignore any prior results
    getLastResult();
    sendExecPrepared(name, values, format);
    return getLastResultThrowError();
  }

  public ResultSet prepare(String name, String query, int[] oids) throws IOException, PostgresqlException {
    // ignore any prior results
    getLastResult();
    sendPrepareQuery(name, query, oids);
    return getLastResultThrowError();
  }

  public ResultSet describePrepared(String name) throws IOException, PostgresqlException {
    sendDescribePrepared(name);
    return getLastResultThrowError();
  }

  public ResultSet describePortal(String name) throws IOException, PostgresqlException {
    sendDescribePortal(name);
    return getLastResultThrowError();
  }

  public void sendQueryParams(String query, Value[] values, Format format, int[] oids) throws IOException {
    sendPrepareQuery("", query, oids);
    shouldBind = true;
    // storing the binding info for later
    this.values = values;
    this.format = format;
  }

  public void sendExecPrepared(String name, Value[] values, Format format) throws IOException {
    checkIsReady();
    currentOutBuffer = new Bind("", name, values, format).toBytes();
    shouldDescribe = true;
    shouldExecute = true;
    state = ConnectionState.SendingBind;
    if (!nonBlocking)
      socket.configureBlocking(true);
    socket.write(currentOutBuffer);
    if (!nonBlocking)
      socket.configureBlocking(false);
    changeState();
  }

  public void sendExecutePortal(String name) throws IOException {
    checkIsReady();
    currentOutBuffer = new Execute(name).toBytes();
    state = ConnectionState.SendingExecute;
    if (!nonBlocking)
      socket.configureBlocking(true);
    socket.write(currentOutBuffer);
    if (!nonBlocking)
      socket.configureBlocking(false);
    changeState();
  }

  public void sendDescribePortal(String name) throws IOException {
    checkIsReady();
    Describe message = new Describe(name, StatementType.Portal);
    sendDescribeCommon(message);
  }

  public void sendDescribePrepared(String name) throws IOException {
    checkIsReady();
    Describe message = new Describe(name, StatementType.Prepared);
    sendDescribeCommon(message);
  }

  public void sendQuery(String query) throws IOException {
    checkIsReady();
    currentOutBuffer = new Query(query).toBytes();
    state = ConnectionState.SendingQuery;
    if (!nonBlocking)
      socket.configureBlocking(true);
    socket.write(currentOutBuffer);
    if (!nonBlocking)
      socket.configureBlocking(false);
    changeState();
  }

  public void sendPrepareQuery(String name, String query, int[] oids) throws IOException {
    checkIsReady();
    currentOutBuffer = new Parse(name, query, oids).toBytes();
    state = ConnectionState.SendingParse;
    if (!nonBlocking)
      socket.configureBlocking(true);
    socket.write(currentOutBuffer);
    if (!nonBlocking)
      socket.configureBlocking(false);
    changeState();
  }

  public SelectableChannel getSocket() {
    return socket.getSocket();
  }

  public void close() throws IOException {
    socket.configureBlocking(true);
    currentOutBuffer = new Terminate().toBytes();
    socket.write(currentOutBuffer);
    socket.close();
    state = ConnectionState.Closed;
  }

  public void cancel() throws IOException {
    SocketChannel closeChannel = SocketChannel.open();
    closeChannel.configureBlocking(true);
    closeChannel.connect(new InetSocketAddress(host, port));
    ByteBuffer bytes = new CancelRequest(backendKeyData.getPid(), backendKeyData.getSecret()).toBytes();
    closeChannel.write(bytes);
    closeChannel.close();
  }

  public boolean closed() {
    return state == ConnectionState.Closed || state == ConnectionState.Failed;
  }

  public void setNonBlocking(boolean nonBlocking) {
    this.nonBlocking = nonBlocking;
  }

  public boolean isNonBlocking() {
    return nonBlocking;
  }

  public boolean isBusy() {
    if (state.shouldBlock() || state.isFlush() || state.isSync()) {
      return lastResultSet.isEmpty();
    } else if (state.isCopyOut()) {
      return copyData.isEmpty();
    } else if (state.isCopyIn()) {
      return currentOutBuffer.remaining() != 0;
    }
    return false;
  }

  public NotificationResponse notifications() {
    return notifications.isEmpty() ? null : notifications.remove(0);
  }

  public NotificationResponse waitForNotify() throws IOException {
    checkIsReady();
    state = ConnectionState.ReadingNotifications;
    consumeInput();
    while (notifications.isEmpty()) {
      Selector selector = Selector.open();
      socket.register(selector, SelectionKey.OP_READ);
      selector.select();
      selector.close();
      consumeInput();
    }
    state = ConnectionState.ReadyForQuery;
    return notifications.remove(0);
  }

  public ResultSet getLastResult() throws IOException {
    ResultSet prevResult, result;
    prevResult = result = null;
    while ((result = getResult()) != null) {
      prevResult = result;

      // break out of the loop if a copy command has started
      if (result.getStatus() == ResultStatus.PGRES_COPY_IN || result.getStatus() == ResultStatus.PGRES_COPY_OUT)
        break;
    }
    return prevResult;
  }

  public ResultSet getResult() throws IOException {
    block();
    ResultSet temp = null;
    if (!lastResultSet.isEmpty())
      temp = lastResultSet.remove(0);
    return temp;
  }

  public boolean block() throws IOException {
    return block(0);
  }

  public boolean block(int timeout) throws IOException {
    long startTime = System.currentTimeMillis();
    long timeLeft = timeout;
    while (isBusy() && (timeout == 0 || timeLeft > 0)) {
      if (state.isRead() && !socket.shouldWaitForData()) {
        // we have data that we can use
      } else {
        Selector selector = Selector.open();
        int op = state.isRead() ? SelectionKey.OP_READ : SelectionKey.OP_WRITE;
        socket.register(selector, op);
        selector.select(timeLeft);
        selector.close();
      }
      consumeInput();
      timeLeft = timeout == 0 ? 0 : timeout - (System.currentTimeMillis() - startTime);
    }

    return !isBusy();
  }

  public LargeObjectAPI getLargeObjectAPI() {
    if (largeObjectAPI == null)
      largeObjectAPI = new LargeObjectAPI(this);
    return largeObjectAPI;
  }

  public boolean flush() throws IOException {
    if (!state.isWrite() || currentOutBuffer.remaining() == 0)
      return true;
    socket.write(currentOutBuffer);
    return currentOutBuffer.remaining() == 0;
  }

  public int getServerVersion() {
    String value = parameterValues.get("server_version");
    if (value == null)
      return 0;
    String[] parts = value.split("\\.");
    int version = 0;
    for (int i = 0; i < parts.length; i++)
      version = version * 100 + Integer.parseInt(parts[i]);
    return version;
  }

  public String getParameterStatus(String name) {
    return parameterValues.get(name);
  }

  public int getBackendPid() {
    return backendKeyData.getPid();
  }

  public TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }

  public boolean getStandardConformingStrings() {
    String value = parameterValues.get("standard_conforming_strings");
    if (value == null || !value.equals("on"))
      return false;
    return true;
  }

  public boolean putCopyDone() throws IOException {
    try {
      if (!state.isCopyIn()) {
        throw new UnsupportedOperationException("Cannot put copy data");
      }
      if (!nonBlocking)
        socket.configureBlocking(true);

      if (currentOutBuffer.remaining() != 0)
        flush();

      if (currentOutBuffer.remaining() == 0) {
        currentOutBuffer = new CopyDone().toBytes();
        if (state == ConnectionState.NewCopyInState)
          state = ConnectionState.NewCopyDone;
        if (state == ConnectionState.NewExtendedCopyInState)
          state = ConnectionState.NewExtendedCopyDone;
        consumeInput();	// call consume input to change the state if possible
        return true;
      } else {
        return false;
      }
    } finally {
      if (!nonBlocking)
        socket.configureBlocking(false);
    }
  }

  public boolean putCopyData(ByteBuffer data) throws IOException {
    try {
      if (!state.isCopyIn()) {
        throw new UnsupportedOperationException("Cannot put copy data");
      }

      if (!nonBlocking)
        socket.configureBlocking(true);

      if (currentOutBuffer.remaining() != 0)
        flush();

      if (currentOutBuffer.remaining() == 0) {
        currentOutBuffer = new CopyData(data).toBytes();
        flush();
        return true;
      } else {
        return false;
      }
    } finally {
      if (!nonBlocking)
        socket.configureBlocking(false);
    }
  }

  public CopyData getCopyData(boolean async) throws IOException {
    if (!copyData.isEmpty())
      return copyData.remove(0);

    try {
      if (!state.isCopyOut())
        return null;

      if (!async)
        socket.configureBlocking(true);

      consumeInput();

      if (!copyData.isEmpty())
        return copyData.remove(0);

      return COPY_DATA_NOT_READY;
    } finally {
      if (!async)
        socket.configureBlocking(false);
    }
  }

  /** the shitty implementation the makes the connection ticks **/

  private void sendDescribeCommon(Describe describeMessage) throws IOException {
    currentOutBuffer = describeMessage.toBytes();
    state = ConnectionState.SendingDescribe;
    if (!nonBlocking)
      socket.configureBlocking(true);
    socket.write(currentOutBuffer);
    if (!nonBlocking)
      socket.configureBlocking(false);
    changeState();
  }

  private void checkIsReady() {
    if (state != ConnectionState.ReadyForQuery && state != ConnectionState.ExtendedReadyForQuery)
      throw new IllegalStateException("Connection isn't ready for a new query. State: " + state.name());

    // reset the result from previous queries
    lastResultSet.clear();
  }

  private ResultSet getLastResultThrowError() throws IOException, PostgresqlException {
    ResultSet result = getLastResult();
    if (result != null && result.hasError()) {
      throw new PostgresqlException(result.getError().getErrorMesssage(), result);
    }
    return result;
  }

  private PostgresqlConnection(Properties props) {
    this.host = host(props);
    this.port = port(props);
    this.user = user(props);
    this.dbname = dbname(props);
    this.password = password(props);
    this.ssl = ssl(props);
  }

  private void connect() throws Exception {
    connectAsync();
    while (state.pollingState() != ConnectionState.PGRES_POLLING_OK
        && state.pollingState() != ConnectionState.PGRES_POLLING_FAILED) {
      Selector selector = Selector.open();
      // do connection poll
      ConnectionState pollState = connectPoll();
      if (pollState == ConnectionState.PGRES_POLLING_WRITING) {
        socket.register(selector, SelectionKey.OP_WRITE);
        selector.select();
      } else if (pollState == ConnectionState.PGRES_POLLING_READING) {
        socket.register(selector, SelectionKey.OP_READ);
        selector.select();
      }
      selector.close();
    }

    if (state.pollingState() == ConnectionState.PGRES_POLLING_FAILED) {
      ResultSet result = lastResultSet.get(0);
      throw new PostgresqlException(result.getError().getErrorMesssage(), result);
    }
  }

  private void connectAsync() throws IOException, PostgresqlException, NoSuchAlgorithmException {
    try {
      socket = new NonSecureSocketWrapper(SocketChannel.open());
      socket.configureBlocking(true);
      socket.connect(new InetSocketAddress(host, port));

      // do the SSL negotiation
      if (!ssl.equals("disable")) {
        socket.configureBlocking(true);
        currentOutBuffer = new SSLRequest().toBytes();
        socket.write(currentOutBuffer);
        ByteBuffer tempBuffer = ByteBuffer.allocate(1);
        socket.read(tempBuffer);
        tempBuffer.flip();
        switch (tempBuffer.get()) {
        case 'E':
          socket.close();
          socket = new NonSecureSocketWrapper(SocketChannel.open());
          socket.configureBlocking(true);
          socket.connect(new InetSocketAddress(host, port));
        case 'N':
          if (ssl.equals("require")) {
            socket.close();
            throw new PostgresqlException("Cannot establish ssl socket with server", null);
          }
          break;
        case 'S':
          socket = new SecureSocketWrapper(socket);
          socket.doHandshake();
          break;
        }
      }

      // send startup message
      socket.configureBlocking(false);
      state = ConnectionState.SendingStartup;
      currentOutBuffer = new Startup(user, dbname, new Properties()).toBytes();
      currentInMessage = new ProtocolMessageBuffer();
    } catch (IOException e) {
      state = ConnectionState.Failed;
      throw e;
    }
  }

  private void changeState() throws IOException {
    if (state.isWrite()) {
      if (currentOutBuffer.remaining() == 0 && socket.outBufferRemaining() == 0)
        state = state.nextState();
    } else if (state.isRead()) {
      if (currentInMessage.remaining() == 0) {
        processMessage();
        state = state.nextState(currentInMessage.getMessage().getType());

        if (state == ConnectionState.SendingAuthentication) {
          PasswordMessage message = createPasswordMessage(currentInMessage.getMessage());
          currentOutBuffer = message.toBytes();
        }
        currentInMessage = new ProtocolMessageBuffer();
      }
    }

    if (state.isFlush()) {
      currentOutBuffer = new Flush().toBytes();
    }

    if (inProgress != null && inProgress.hasError()) {
      shouldBind = shouldDescribe = shouldExecute = false;
    }

    if (state == ConnectionState.ExtendedReadyForQuery) {
      if (extendedQueryIsOver()) {
        currentOutBuffer = new Sync().toBytes();
        state = ConnectionState.SendingSync;
      } else {
        if (shouldBind) {
          shouldBind = false;
          sendExecPrepared("", values, format);
        } else if (shouldDescribe) {
          shouldDescribe = false;
          sendDescribePortal("");
        } else if (shouldExecute) {
          shouldExecute = false;
          sendExecutePortal("");
        }
      }
    }
  }

  private PasswordMessage createPasswordMessage(ProtocolMessage message) {
    switch (message.getType()) {
    case AuthenticationCleartextPassword:
      if (password == null)
        return new PasswordMessage(new byte[0]);
      return new PasswordMessage(password.getBytes());
    case AuthenticationMD5Password:
      try {
        AuthenticationMD5Password auth = (AuthenticationMD5Password) message;
        byte[] firstmd5 = encrypt(password.getBytes(), user.getBytes());
        byte[] finalmd5 = encrypt(firstmd5, 3, auth.getSalt());
        return new PasswordMessage(finalmd5);
      } catch (Exception e) {
        // if I know what I'm doing then we shouldn't be here
        return null;
      }
    default:
      throw new IllegalArgumentException("Unsupported authentication type: " + message.getType().name());
    }
  }

  private void processMessage() {
    if (currentInMessage.getMessage().getType() == MessageType.NoticeResponse) {
    }

    if (currentInMessage.getMessage().getType() == MessageType.NotificationResponse) {
      notifications.add((NotificationResponse) currentInMessage.getMessage());
      return;
    }
    if (currentInMessage.getMessage().getType() == MessageType.ParameterStatus) {
      processParameterStatusAndBackend();
      return;
    }

    switch (state) {
    case ReadingAuthentication:
    case ReadingAuthenticationResponse:
      if (currentInMessage.getMessage().getType() == MessageType.ErrorResponse) {
        if (inProgress == null)
          inProgress = new ResultSet();
        inProgress.setErrorResponse((ErrorResponse) currentInMessage.getMessage());
        lastResultSet.add(inProgress);
        inProgress = null;
      }
      break;
    case ReadingBackendData:
    case ReadingParameterStatus:
      processParameterStatusAndBackend();
      break;
    case NewCopyOutState:
    case NewExtendedCopyOutState:
      processCopyOutResponse();
      break;
    case ReadingParseResponse:
      processParseResponse();
      break;
    case ReadingBindResponse:
      processBindResponse();
      break;
    case ReadingDescribeResponse:
      processDescribeResponse();
      break;
    case ReadingQueryResponse:
    case ReadingExecuteResponse:
      processQueryResponse();
      break;
    case ReadingReadyForQuery:
      processSyncReadyForQuery();
      break;
    }
  }

  private void processCopyOutResponse() {
    switch (currentInMessage.getMessage().getType()) {
    case CopyData:
      copyData.add((CopyData) currentInMessage.getMessage());
      break;
    case CopyDone:
      break;
    default:
      throw new IllegalArgumentException("Unknown message type received while in copy in mode: "
          + currentInMessage.getMessage().getType());
    }
  }

  private void processSyncReadyForQuery() {
    if (extendedQueryIsOver()) {
      lastResultSet.add(inProgress);
      inProgress = null;
    }
  }

  private void processDescribeResponse() {
    ProtocolMessage message = currentInMessage.getMessage();
    if (inProgress == null)
      inProgress = new ResultSet();
    switch (message.getType()) {
    case ErrorResponse:
      inProgress.setErrorResponse((ErrorResponse) message);
      break;
    case RowDescription:
      inProgress.setDescription((RowDescription) message);
      break;
    case ParameterDescription:
      inProgress.setParameterDescription((ParameterDescription) message);
      break;
    }
  }

  private void processParseResponse() {
    ProtocolMessage message = currentInMessage.getMessage();
    if (inProgress == null)
      inProgress = new ResultSet();
    switch (message.getType()) {
    case ErrorResponse:
      inProgress.setErrorResponse((ErrorResponse) message);
      break;
    }
  }

  private void processBindResponse() {
    processParseResponse(); // same logic
  }

  private void processQueryResponse() {
    ProtocolMessage message = currentInMessage.getMessage();

    switch (message.getType()) {
    case CopyInResponse:
      if (inProgress == null)
        inProgress = new ResultSet();
      inProgress.setStatus(ResultStatus.PGRES_COPY_IN);
      lastResultSet.add(inProgress);
      inProgress = null;
      break;
    case CopyOutResponse:
      if (inProgress == null)
        inProgress = new ResultSet();
      inProgress.setStatus(ResultStatus.PGRES_COPY_OUT);
      lastResultSet.add(inProgress);
      inProgress = null;
      break;
    case CommandComplete:
      if (inProgress == null)
        inProgress = new ResultSet();
      lastResultSet.add(inProgress);
      inProgress = null;
      break;
    case EmptyQueryResponse:
      if (inProgress == null)
        inProgress = new ResultSet();
      break;
    case RowDescription:
      if (inProgress == null)
        inProgress = new ResultSet();
      // fetch the row description
      RowDescription description = (RowDescription) message;
      inProgress.setDescription(description);
      break;
    case DataRow:
      DataRow dataRow = (DataRow) message;
      inProgress.appendRow(dataRow);
      break;
    case ErrorResponse:
      if (inProgress == null)
        inProgress = new ResultSet();
      ErrorResponse error = (ErrorResponse) message;
      inProgress.setErrorResponse(error);
      break;
    case ReadyForQuery:
      lastResultSet.add(inProgress);
      inProgress = null;
      transactionStatus = ((ReadyForQuery) message).getTransactionStatus();
      break;
    }
  }

  private void processParameterStatusAndBackend() {
    ProtocolMessage message = currentInMessage.getMessage();
    switch (message.getType()) {
    case ParameterStatus:
      ParameterStatus parameterStatus = (ParameterStatus) message;
      parameterValues.put(parameterStatus.getName(), parameterStatus.getValue());
      break;
    case BackendKeyData:
      backendKeyData = (BackendKeyData) message;
      break;
    case ReadyForQuery:
      transactionStatus = ((ReadyForQuery) message).getTransactionStatus();
      break;
    case ErrorResponse:
      if (inProgress == null)
        inProgress = new ResultSet();
      inProgress.setErrorResponse((ErrorResponse) message);
      lastResultSet.add(inProgress);
      inProgress = null;
      break;
    }
  }

  private boolean extendedQueryIsOver() {
    return !shouldBind && !shouldDescribe && !shouldExecute;
  }

  private final String                     host;
  private final String                     dbname;
  private final int                        port;
  private final String                     user;
  private final String                     password;
  private final String                     ssl;
  private boolean                          nonBlocking     = false;

  private boolean                          shouldBind      = false;
  private boolean                          shouldDescribe  = false;
  private boolean                          shouldExecute   = false;

  private Value[]                          values;
  private Format                           format;

  private LargeObjectAPI                   largeObjectAPI;
  private TransactionStatus                transactionStatus;
  private ResultSet                        inProgress;
  private final List<ResultSet>            lastResultSet   = new LinkedList<ResultSet>();
  private final List<CopyData>             copyData        = new LinkedList<CopyData>();
  private final List<NotificationResponse> notifications   = new LinkedList<NotificationResponse>();
  private SocketWrapper                    socket;
  private ConnectionState                  state           = ConnectionState.CONNECTION_BAD;
  private ByteBuffer                       currentOutBuffer;
  private ProtocolMessageBuffer            currentInMessage;
  private final Map<String, String>        parameterValues = new HashMap<String, String>();
  private BackendKeyData                   backendKeyData;
}
