package org.jruby.pg.messages;

import java.util.Map;

public class NoticeResponse extends ErrorResponse {

  public NoticeResponse(Map<Byte, String> fields, int length) {
    super(fields, length);
  }

  @Override
  public MessageType getType() {
    return MessageType.NoticeResponse;
  }
}
