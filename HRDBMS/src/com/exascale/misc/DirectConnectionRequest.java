package com.exascale.misc;

import java.io.Serializable;

public final class DirectConnectionRequest implements Serializable
{
  private byte from;
  private byte to;

  public DirectConnectionRequest(final byte from, final byte to) {
    this.from = from;
    this.to = to;
  }

  public byte getFrom() {
    return from;
  }

  public byte getTo() {
    return to;
  }

  @Override
	public String toString()
	{
    int from = (int) this.from;
    int to = (int) this.to;
    return "DIRECT CONNECTION REQUEST REQUIRED FROM " + from + " TO " + to;
  }
}
