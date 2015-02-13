package org.jruby.pg.messages;

public class ParameterDescription extends BackendMessage {
  private final int[] oids;

  public ParameterDescription(int [] oids, int length) {
    this.oids = oids;
  }

  @Override
  public MessageType getType() {
    return MessageType.ParameterDescription;
  }

  public int[] getOids() {
    return oids;
  }
}
