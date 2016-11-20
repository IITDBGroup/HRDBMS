package com.exascale.optimizer;

import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;

public class BailErrorStrategy extends DefaultErrorStrategy
{
	@Override
	public void recover(final Parser recognizer, final RecognitionException e)
	{
		final String string = "The parser failed at token " + e.getOffendingToken().getText() + " of type " + SelectLexer.tokenNames[e.getOffendingToken().getType()];
		throw new RuntimeException(string);
	}

	@Override
	public Token recoverInline(final Parser recognizer) throws RecognitionException
	{
		throw new RuntimeException("Input mismatch at " + recognizer.getCurrentToken() + " of type " + SelectLexer.tokenNames[recognizer.getCurrentToken().getType()]);
	}

	@Override
	public void sync(final Parser recognizer)
	{
	}
}
