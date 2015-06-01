package com.exascale.optimizer;

public class ExistsPredicate extends Predicate
{
	private final SubSelect select;

	public ExistsPredicate(SubSelect select)
	{
		this.select = select;
	}

	@Override
	public Predicate clone()
	{
		return new ExistsPredicate(select.clone());
	}

	@Override
	public boolean equals(Object o)
	{
		return false;
	}

	public SubSelect getSelect()
	{
		return select;
	}
}
