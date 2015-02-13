package org.jruby.pg.messages;

public class ReadyForQuery extends BackendMessage {
  private final TransactionStatus transactionStatus;

  public ReadyForQuery(TransactionStatus transactionStatus, int length) {
    this.transactionStatus = transactionStatus;
  }

  @Override
  public MessageType getType() {
    return MessageType.ReadyForQuery;
  }

  public TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }
}
