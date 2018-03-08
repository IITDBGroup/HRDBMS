package com.exascale.misc;

import java.io.Serializable;

public final class DirectConnectionRequest implements Serializable
{
  private int from;
  private int to;

  public DirectConnectionRequest(final int from, final int to) {
    this.from = from;
    this.to = to;
  }

  public int getFrom() {
    return from;
  }

  public int getTo() {
    return to;
  }

  @Override
	public String toString()
	{
    return "DIRECT CONNECTION REQUEST REQUIRED FROM " + this.from + " TO " + this.to;
  }
}
