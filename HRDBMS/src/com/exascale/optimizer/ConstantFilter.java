package com.exascale.optimizer;

public final class ConstantFilter extends Filter
{
	private final double likelihood;

	public ConstantFilter(double likelihood)
	{
		this.likelihood = likelihood;
	}

	public double getLikelihood()
	{
		return likelihood;
	}
}
