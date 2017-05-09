package com.exascale.optimizer;

import static org.junit.Assert.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import org.junit.Test;
import com.exascale.exceptions.ParseException;

public class ExpressionCompilationTest
{

	@Test		
	public void testPostOrderTraversalIterative2() throws IOException
	{
		System.out.println("Testing");
		Literal l1= new Literal(7);
	
		Expression e1  = new Expression(l1);
		Literal l2= new Literal(1);
		
		//Function f2= new Function("Max", args)
		Expression e2  = new Expression(l2);
		String op="+";
		Expression e3 = new Expression( e1,op, e2);
		Column c1= new Column("Left","B");
		Expression e4=new Expression(c1);
		Expression e5 = new Expression( e3,"+", e4);
		HashMap<String, Integer> lcols2Pos = new HashMap<String, Integer>();
		HashMap<Integer, String> lcols2Type = new HashMap<Integer, String>();
		lcols2Pos.put("A",1);
		lcols2Pos.put("B",2);
		lcols2Type.put(1, "int");
		lcols2Type.put(2, "double");
		ArrayList<Expression> args=new ArrayList<Expression>();
		ArrayList<Expression> args1=new ArrayList<Expression>();
		Literal l6=new Literal(3);
		Expression e6=new Expression(l6);
		HashMap<String, Integer> rcols2Pos = new HashMap<String, Integer>();
		HashMap<Integer, String> rcols2Type = new HashMap<Integer, String>();
		rcols2Pos.put("A",1);
		rcols2Pos.put("B",2);
		rcols2Type.put(1, "int");
		rcols2Type.put(2, "double");
		Column c7=new Column("Right","A");
		Expression e7=new Expression(c7);
		Expression e8=new Expression(e6,"*",e7);
		//Expression e9=new Expression(new Literal(1));
		Expression e9=new Expression(new Literal(new String("This is a large String Pun")));
		//args.add(e8);
		args.add(e9);
		Expression e12 = new Expression(new Literal(new Double(3.62)));
		Expression e13 = new Expression(new Literal(new Double(4.62)));
		Expression e15 = new Expression(new Literal(new Double(11.23)));
		Expression e16= new Expression(e13,"/",e15);
		//Expression e16= new Expression(new Literal(5));
		args1.add(e12);
		args1.add(e16);
		Expression e24 = new Expression(new Function("Min", args1));
		Predicate p1= new Predicate(e3, ">",e24);
		 Predicate p2= new Predicate(e6, ">=",e7);
		SearchClause s1 = new SearchClause(p1, false);
		SearchClause s2 = new SearchClause(p2, false);
		SearchCondition sc1= new SearchCondition(s1, null);
		SearchCondition sc2= new SearchCondition(s2, null);
		Case cs1 = new Case(sc1, e16);
		Case cs2 = new Case(sc2,e8);
		ArrayList<Case> css = new ArrayList<Case>();
		css.add(cs1);
		css.add(cs2);
		Expression e17 = new Expression(css, e5);
		
		/* Expression e14 = new Expression(new Literal(1));
		//args.add(e14);
		//args.add(e16);
		/* System.out.println(args.size());
		Expression e10 = new Expression(new Function("Substring", args));
		Expression e11= new Expression(e5,"+",e10);
		/* Literal l3= new Literal(4);
		Expression e4 = new Expression(l3);

		Expression e5 = new Expression( e3,"/", e4);
		Literal l4= new Literal(3);
		Expression e6 = new Expression(l4);


		Expression e7 = new Expression(e6,"*", e5);*/
				//Expression e24 = new Expression(e5,"*",e23);
		ExpressionCompilation ec = new ExpressionCompilation();
		ec.expressionEval(e17,lcols2Pos,lcols2Type,rcols2Pos,rcols2Type);
		
		// Uncomment Lines  96-104 to Run Year Operator
		/*  ArrayList<Expression> args2= new ArrayList<Expression>();
		
		Literal l5= new Literal(new String("Fri, June 7 2013"));
		Expression e18 = new Expression(l5);
		args2.add(e18);
		Expression e20 = new Expression(new Function("Year", args2));
		Expression e25= new Expression(e5,"+",e20);
		ec.expressionEval(e25,lcols2Pos,lcols2Type,rcols2Pos,rcols2Type);
		*/
		
		//UnComment line 107-112 to see Optimization Expression Caching
		/*
		Expression e21 = new Expression(e3,"+", e4);
		Expression e22 = new Expression(e4,"+",e3);
		Expression e23 = new Expression(e22,"*",e21);
		ec.expressionEval(e23,lcols2Pos,lcols2Type,rcols2Pos,rcols2Type);
		*/
		// Uncomment Lines 114-127 to run Substring Function
		 /* Expression e31= new Expression(new Literal(1));
		Expression e32= new Expression(new Literal(1.23));
		Expression e33= new Expression(e31,"/",e32);
		Expression e34= new Expression(new Literal(3));
		Expression e35= new Expression(new Literal(10));
		ArrayList<Expression> max_args=new ArrayList<Expression>();
		max_args.add(e33);
		max_args.add(e34);
		max_args.add(e35);
		Expression e36 = new Expression(new Function("Substring", max_args));
		Expression e37 = new Expression(new Literal(3));
		Expression e38= new Expression(e36,"+",e37); 
				ec.expressionEval(e38,lcols2Pos,lcols2Type,rcols2Pos,rcols2Type);*/
	}

}
