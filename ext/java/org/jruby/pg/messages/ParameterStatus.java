package org.jruby.pg.messages;

public class ParameterStatus extends BackendMessage {
  private final String name;
  private final String value;

  public ParameterStatus(String name, String value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public MessageType getType() {
    return MessageType.ParameterStatus;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }
}
