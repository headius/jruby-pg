package org.jruby.pg.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.*;

/**
 * A wrapper for any ByteChannel that will encrypt and decrypt the
 * data before sending/receiving it over the channel
 */
public class SecureByteChannel implements FlushableByteChannel {
  // an empty buffer used when there is no data expected to be read or
  // written, e.g. when the SSLEngine is doing handhsake
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  // A dummy trust manager that trusts all certificates
  private static final TrustManager[] DUMMY_TRUST_MANAGER = new TrustManager[] {
  new X509TrustManager() {
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
    @Override
    public void checkClientTrusted(X509Certificate[] chain,
                                   String authType) throws CertificateException {
    }
    @Override
    public void checkServerTrusted(X509Certificate[] chain,
                                   String authType) throws CertificateException {
    }
  }
  };

  // the ssl engine where all the encryption/decryption happen
  private final SSLEngine sslEngine;
  // The underlying channel that we read/write to/from
  private final ByteChannel channel;
  // and decrypted input buffer, always ready for a get()
  private final ByteBuffer decryptedBuffer;
  // and encrypted input buffer, always ready for a get()
  private final ByteBuffer inBuffer;
  // and output buffer, always ready for a put()
  private final ByteBuffer outBuffer;

  /**
   * Create a new SecureByteChannel that will wrap the given channel
   *
   * @param channel the underlying byte channel
   * @param verify verify the certificate returned by the server
   *
   * @throws NoSuchAlgorithmException if we can't create an SSLEngine
   * @throws KeyManagementException
   */
  public SecureByteChannel(ByteChannel channel, boolean verify)
  throws NoSuchAlgorithmException, KeyManagementException, SSLException {
    this.channel = channel;
    SSLContext context;
    if(!verify) {
      // if we don't have to verify the cert, use the
      // DUMMY_TRUST_MANAGER which will accept any cert
      context = SSLContext.getInstance("SSL");
      context.init(null, DUMMY_TRUST_MANAGER, null);
    } else {
      // otherwise, use the default which will only accept certs
      // signed by a trusted CA
      context = SSLContext.getDefault();
    }
    sslEngine = context.createSSLEngine();
    sslEngine.setUseClientMode(true);
    SSLSession session = sslEngine.getSession();

    // inBuffer should always be in a get() state
    inBuffer = ByteBuffer.allocate(session.getPacketBufferSize());
    inBuffer.flip();
    // decryptedBuffer should always be in a get() state
    decryptedBuffer = ByteBuffer.allocate(session.getApplicationBufferSize());
    decryptedBuffer.flip();
    outBuffer = ByteBuffer.allocate(session.getPacketBufferSize());

    // begin the handshake, if we are here then the server has
    // accepted to encrypt the connection
    sslEngine.beginHandshake();
  }

  /**
   * Read from the underlying channel and decrypt the data into buf
   *
   * @return the number of bytes inserted into buf
   */
  public int read(ByteBuffer buf) throws IOException {
    if(!decryptedBuffer.hasRemaining()) {
      // read from the underlying socket and decrypt if the
      // decryptedBuffer is empty
      decryptedBuffer.clear();
      inBuffer.compact();
      channel.read(inBuffer);
      inBuffer.flip();
      // as long as we are consuming data from the inBuffer or
      // generating data in the decryptedBuffer keep going. This is
      // essential, otherwise there are cases when the SSLEngine will
      // consume part of the buffer without generating any data and
      // the caller has no way to know whether there are more data in
      // the inBuffer or not.
      SSLEngineResult res;
      do {
        res = sslEngine.unwrap(inBuffer, decryptedBuffer);
      } while(res.bytesConsumed() > 0 || res.bytesProduced() > 0);
      decryptedBuffer.flip();
    }

    int remaining = Math.min(buf.remaining(), decryptedBuffer.remaining());
    // create a new buffer with limit equal to the max number of bytes
    // that we can transfer from decryptedBuffer to buf. Note: we
    // can't simply do buf.put(decryptedBuffer) since it can throw a
    // BufferOverflowException
    ByteBuffer slice = (ByteBuffer) decryptedBuffer.slice().limit(remaining);
    buf.put(slice);
    // set the position of decryptedBuffer
    decryptedBuffer.position(decryptedBuffer.position() + remaining);
    return remaining;
  }

  /**
   * Encrypt and write the data in the given buffer to the underlying
   * channel
   *
   * @return the number of bytes consumed from the buffer
   */
  public int write(ByteBuffer buf) throws IOException {
    if(!outBuffer.hasRemaining()) {
      outBuffer.flip();
      channel.write(outBuffer);
      outBuffer.compact();
    }

    return sslEngine.wrap(buf, outBuffer).bytesConsumed();
  }

  /**
   * Flush all buffered data to the underlying channel
   *
   * @return true if the internal encrypted bufferd was flushed to the channel
   * @throws IOException
   */
  public boolean flush() throws IOException {
    outBuffer.flip();
    try {
      if(outBuffer.hasRemaining()) {
        channel.write(outBuffer);
      }
      return !outBuffer.hasRemaining();
    } finally {
      outBuffer.compact();
    }
  }

  /**
   * Tries to finish the handshake with the other end of the
   * connection
   *
   * @return the status of the handshake
   */
  public HandshakeStatus doHandshake() throws IOException {
    for(;;) {
      // make sure we send all the data
      if(!flush()) {
        return HandshakeStatus.WRITING;
      }

      switch(sslEngine.getHandshakeStatus()) {
      case NEED_UNWRAP:
        read(EMPTY_BUFFER);
        javax.net.ssl.SSLEngineResult.HandshakeStatus status =
          sslEngine.getHandshakeStatus();
        // if we still need to unwrap, then return READING
        if(status == javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
          return HandshakeStatus.READING;
        }
        // otherwise, go back to the beginning of the loop
        continue;

      case NEED_WRAP:
        // wrap and return to the beginning of the loop to make sure
        // we flush all the data
        write(EMPTY_BUFFER);
        continue;

      case NEED_TASK:
        Runnable runnable = sslEngine.getDelegatedTask();
        runnable.run();
        continue;

      case NOT_HANDSHAKING:
      case FINISHED:
        return HandshakeStatus.FINISHED;

      default:
        throw new IOException("Unknown handshake status");
      }
    }
  }

  @Override
  public boolean isOpen() {
    return channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    // signal to the SSLEngine that we're closing the connection
    sslEngine.closeOutbound();

    // give the engine a chance to start the closure handshake
    while(!sslEngine.isOutboundDone()) {
      sslEngine.wrap(EMPTY_BUFFER, outBuffer);
      outBuffer.flip();

      while(outBuffer.hasRemaining()) {
        channel.write(outBuffer);
      }

      outBuffer.clear();
    }

    // we can just close the connection since we don't care about
    // Truncation attacks
    channel.close();
  }
}
