package com.exascale.optimizer;

public class ExistsPredicate extends Predicate
{
	private final SubSelect select;

	public ExistsPredicate(final SubSelect select)
	{
		this.select = select;
	}

	@Override
	public Predicate clone()
	{
		return new ExistsPredicate(select.clone());
	}

	@Override
	public boolean equals(final Object o)
	{
		return false;
	}

	public SubSelect getSelect()
	{
		return select;
	}
}
