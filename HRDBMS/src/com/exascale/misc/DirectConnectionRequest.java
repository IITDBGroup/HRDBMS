package com.exascale.misc;

import java.io.Serializable;

public final class DirectConnectionRequest implements Serializable
{
  private int from;
  private int to;
  private int opId;

  public DirectConnectionRequest(final int from, final int to, final int opId) {
    this.from = from;
    this.to = to;
    this.opId = opId;
  }

  public int getFrom() {
    return this.from;
  }

  public int getTo() {
    return this.to;
  }

  public int getOpId() {
    return this.opId;
  }

  @Override
	public String toString()
	{
    return "DIRECT CONNECTION REQUEST REQUIRED FROM " + this.from + " TO " + this.to + " WITH OP ID " + this.opId;
  }
}
