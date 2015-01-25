package com.exascale.optimizer;

public class ExistsPredicate extends Predicate
{
	private SubSelect select;
	
	public ExistsPredicate(SubSelect select)
	{
		this.select = select;
	}
	
	public SubSelect getSelect()
	{
		return select;
	}
	
	public Predicate clone()
	{
		return new ExistsPredicate(select.clone());
	}
	
	public boolean equals(Object o)
	{
		return false;
	}
}
