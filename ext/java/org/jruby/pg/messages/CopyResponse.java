package org.jruby.pg.messages;

public abstract class CopyResponse extends BackendMessage {
  private final Format overallFormat;
  private final Format[] columnFormats;

  public CopyResponse(Format overallFormat, Format[] columnFormats) {
    this.overallFormat = overallFormat;
    this.columnFormats = columnFormats;
  }

  public Format getOverallFormat() {
    return overallFormat;
  }

  public Format[] getColumnFormats() {
    return columnFormats;
  }
}
