package org.jruby.pg.internal.messages;

public class CopyOutRespone extends CopyResponse {
  public CopyOutRespone(Format overallFormat, Format[] columnFormats) {
    super(overallFormat, columnFormats);
  }
  @Override
  public MessageType getType() {
    return MessageType.CopyOutResponse;
  }
}
