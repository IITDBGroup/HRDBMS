package com.exascale.optimizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Stack;
import org.apache.commons.io.FileUtils;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;



public class ExpressionCompilation
{
	// org.apache.log4j.Logger
	String strTempVarName = "temp";
	String strSbVarName = "sb";
	String CaseFinalVar;
	String fileNameString;
	static int intTempVarCount = 0;
	static int intSbVarCount = 0;
	static int FileHashCount = 0;
	int left_type, right_type;
	boolean left_literal, right_literal;
	HashMap<Integer, String> types;
	HashMap<String, String> vars = new HashMap<String, String>();
	static StringBuilder imports = new StringBuilder();
	public static Logger logger; 
	private static FileAppender fa;
	HashMap<String, String> cache = new HashMap<String, String>();// Lucy add a
																	// string
																	// string
																	// hashmap
	
	/* Constructor which appends variables used for type management */
	public ExpressionCompilation()
	{
		types = new HashMap<Integer, String>();
		types.put(1, "int");
		types.put(2, "float");
		types.put(3, "double");
		types.put(4, "String");
		imports.append("import java.util.ArrayList;");
		
	}
	/* Create temp final  variable which stores result of Operations */ 
	String getTempVarName()
	{
		return strTempVarName + Integer.toString(intTempVarCount++);
	}
	/* Create StringBuilder Variables which are used for  Functions like Substring*/ 
	String getSbVarName()
	{
		return strSbVarName + Integer.toString(intSbVarCount++);
	}
	/* Manage File names should be uniques across multiple Objects */ 
	String getFileName()
	{

		return "MyExprEval_" + Integer.toString(FileHashCount++);
	}
	
	/* Post Order Traversal of the Expression Tree which returns a stack which can be further evaluated */
	Stack<Expression> postOrderTraversalIterative2(Expression root, HashMap<String, Integer> cols2Pos, HashMap<Integer, String> cols2Types)
	{
	
		if (this == null)
			return null;

		Stack<Expression> s = new Stack<Expression>();
		Stack<Expression> s2 = new Stack<Expression>();
		s.push(root);
		Expression prev = null;
		
		while (!s.empty())
		{
			Expression curr = s.lastElement();
			// we are traversing down the tree
			if ((prev == null) || (prev.getLHS() == curr) || (prev.getRHS() == curr))
			{
				// System.out.println("Yeh wale mai");
				Expression lhs = curr.getLHS();
				Expression rhs = curr.getRHS();
				if (lhs != null)
				{
					s.push(lhs);
				}
				else if (rhs != null)
				{
					s.push(rhs);
				}
				else
				{

					s2.push(curr);
					s.pop();
				}
			}
			// we are traversing up the tree from the left
			else if (curr.getLHS() == prev)
			{
				
				if (curr.getRHS() != null)
				{
					s.push(curr.getRHS());
				}
				else
				{
					s2.push(curr);
					s.pop();
				}
			}
			// we are traversing up the tree from the right
			else if (curr.getRHS() == prev)
			{
				
				s2.push(curr);
				s.pop();
			}
			prev = curr; // record previously traversed node


		}
			logger.info("Created to a stack from Post Order Traversal with size:"+s2.size());
		return s2;

	}
	
	// Return String Operands and Sets the type of them
	String ReturnOperand(Expression e, HashMap<String, Integer> lcols2Pos, HashMap<Integer, String> lcols2Type, HashMap<String, Integer> rcols2Pos, HashMap<Integer, String> rcols2Type, int operandSide)
	{
		if (e.isLiteral())
		{
			
			Literal Operand = e.getLiteral();
			Object value = Operand.getValue();
			String result = Operand.getValue().toString();
	
			boolean flag = false;
			// For Left Operand set left_literal true and Check the type and set left_type
			if (operandSide == 1)
			{
				this.left_literal = true;
				if (value instanceof Integer)
				{
					this.left_type = 1;
				}
				if (value instanceof Double)
				{
					this.left_type = 3;
				}
				if (value instanceof Float)
				{
					this.left_type = 2;
				}
				if (value instanceof String)
				{

					//System.out.println(value.toString());
					// Check if the Literal is a result(Temp Variable) of the String Type
					//If it is then w can find it in the hashmap vars and get its type
					
					if (vars.containsKey(result))
					{
						//System.out.println("Var contains key");
						String res = vars.get(result);
						for (int o : types.keySet())
						{
							// String s1="\""+types.get(o)+"\"";
							if (types.get(o).equals(res))
							{
								this.left_type = o;

								break;
							}
						}
					}
					else
					{
						// Else it is just a normal String Literal
						// value=new String("\""+value.toString()+"\"");
						flag = true;
						this.left_type = 4;
					}
				}
			}
			if (operandSide == 2)
			{
				// Repeat the same process for right
				this.right_literal = true;
				if (value instanceof Integer)
				{
					this.right_type = 1;
				}
				if (value instanceof Double)
				{
					this.right_type = 3;
				}
				if (value instanceof Float)
				{
					this.right_type = 2;
				}
				if (value instanceof String)
				{

					if (vars.containsKey(result))
					{
						String res = vars.get(result);
						for (int o : types.keySet())
						{
							if (types.get(o).equals(res))
							{
								this.right_type = o;
							}
						}
					}
					else
					{
						flag = true;
						this.right_type = 4;
					}
				}
			}
			if (flag == false)
			{

				return Operand.getValue().toString();
			}
			logger.info("Returned String Operand:"+new String("\"" + Operand.getValue().toString() + "\""));
			return new String("\"" + Operand.getValue().toString() + "\"");
			

		}
		if (e.isColumn())
		{
			// If it is a column,then get Table name check if Left or Right and then Return Variable 
			Column Operand = e.getColumn();
			// strLeftOperand = LeftOperand.getValue().toString();
			String col_name = Operand.getColumn();
			String Table_name = Operand.getTable();
			String name = "";
			if (Table_name == "Left")
			{
				int getPos = lcols2Pos.get(col_name);

				if (operandSide == 1)
				{

					name = lcols2Type.get(getPos);
					for (Entry<Integer, String> entry : types.entrySet())
					{
						if (name.equals(entry.getValue()))
						{
							this.left_type = entry.getKey();
							break;
						}
					}
				}
				// this.left_type=
				if (operandSide == 2)
				{
					name = lcols2Type.get(getPos);
					for (Entry<Integer, String> entry : types.entrySet())
					{
						if (name.equals(entry.getValue()))
						{
							this.right_type = entry.getKey();
							break;
						}
					}
				}
				logger.info("Column returned:"+new String("(" + name + ")rowl.get(" + getPos + ")"));
				return new String("(" + name + ")rowl.get(" + getPos + ")");
			}
			if (Table_name == "Right")
			{
				int getPos = rcols2Pos.get(col_name);

				if (operandSide == 1)
				{

					name = rcols2Type.get(getPos);
					for (Entry<Integer, String> entry : types.entrySet())
					{
						if (name.equals(entry.getValue()))
						{
							this.left_type = entry.getKey();
						}
					}
				}
				// this.left_type=
				if (operandSide == 2)
				{
					name = rcols2Type.get(getPos);
					for (Entry<Integer, String> entry : types.entrySet())
					{
						if (name.equals(entry.getValue()))
						{
							this.right_type = entry.getKey();
						}
					}
				}
				logger.info("Column returned:"+new String("(" + name + ")rowl.get(" + getPos + ")"));
				return new String("(" + name + ")rowr.get(" + getPos + ")");

			}
			

		}
		if (e.isFunction())
		{
			// If function then call Handle Function Method
			STGroup group = new STGroupFile("./src/templates/templates.stg");
			String functionOp = handleFunction(e, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type, group);
			String[] funcString = functionOp.split("=");
			String typeFunString = vars.get(strTempVarName + Integer.toString(intTempVarCount - 1));
			// Extract Function name with parameters. Just remove the final variable just keep RHS.
			int typeInt = 0;
			for (Entry<Integer, String> entry : types.entrySet())
			{
				if (typeFunString.equals(entry.getValue()))
				{
					typeInt = entry.getKey();
				}
			}
			if (operandSide == 1)
			{
				this.left_type = typeInt;
			}
			if (operandSide == 2)
			{
				this.right_type = typeInt;
			}
			logger.info("Returned Function:"+funcString[1].split(";")[0]);
			return funcString[1].split(";")[0];
		}
		return new String("");
	}
	
	// Return Final Datatype by checking Left and Right
	String getDataType()
	{
		int result = 0;
		if (this.left_type > this.right_type)
		{
			result = this.left_type;
		}
		else
		{
			result = this.right_type;
		}
		logger.info("Final DataType:"+this.types.get(result));
		return this.types.get(result);
	}

	// Return Type of Operator
	String getOpreatorType(String op)
	{
		if (op == "+" || op == "-" || op == "*" || op == "/")
		{
			return "ArithmeticOperator";
		}
		else
		{
			return "ConditionalOperator";
		}
	}

	// Code For Handing and Evaluating an Expression with both Operands as Literals i.e. Optimizing Constants  
	Expression handleConstants(Object LeftOperand, Object RightOperand, String strOperator, String DT)
	{
		// LeftOpretor
		int i_left = 0, i_right = 0;
		float f_left = 0, f_right = 0;
		double d_left = 0.00, d_right = 0.00;
		String s_left = "", s_right = "";
		// System.out.println("Optimizing stuff Final DataType:"+DT+"Operator:"+strOperator);
		// String DT=getDataType();
		logger.info("Optimizing for handling Constants");
		// Converting each literal to its Final type 
		if (DT.equals("int"))
		{
			i_left = (int)LeftOperand;
			i_right = (int)RightOperand;
		}
		else if (DT.equals("float"))
		{
			f_left = (float)LeftOperand;
			f_right = (float)RightOperand;
		}
		else if (DT.equals("double"))
		{
			if (left_type == 1)
			{
				d_left = (double)((Integer)LeftOperand).doubleValue();
			}
			else
			{
				d_left = (double)LeftOperand;
			}
			if (right_type == 1)
			{
				d_right = (double)((Integer)RightOperand).doubleValue();
			}
			else
			{
				d_right = (double)RightOperand;
			}

		}
		Object value = new Object();
		// Based on Operator do the operation
		switch (strOperator)
		{
			case "+":
				if (DT.equals("int"))
				{
					Integer result = new Integer(i_left + i_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("float"))
				{
					Float result = new Float(f_left + f_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("double"))
				{
					Double result = new Double(d_left + d_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("string"))
				{
					String result = new String(s_left + s_right);
					return new Expression(new Literal(result));
				}
				// Object result=left+RightOperand;

				// Object result = left.cast(LeftOperand) +
				// right.cast(RightOperand);
				break;
			case "-":
				if (DT.equals("int"))
				{
					Integer result = new Integer(i_left - i_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("float"))
				{
					Float result = new Float(f_left - f_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("double"))
				{
					Double result = new Double(d_left - d_right);
					return new Expression(new Literal(result));
				}
				/*
				 * if(DT.equals("string")){ String result=new
				 * String(s_left+s_right); value=result; }
				 */
				// Object result=left+RightOperand;

				// Object result = left.cast(LeftOperand) +
				// right.cast(RightOperand);
				break;
			case "/":
				if (DT.equals("int"))
				{
					Integer result = new Integer(i_left / i_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("float"))
				{
					Float result = new Float(f_left / f_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("double"))
				{
					Double result = new Double(d_left / d_right);
					return new Expression(new Literal(result));
				}
				/*
				 * if(DT.equals("string")){ String result=new
				 * String(s_left/s_right); value=result; }
				 */
				// Object result=left+RightOperand;

				// Object result = left.cast(LeftOperand) +
				// right.cast(RightOperand);
				break;
			case "*":
				if (DT.equals("int"))
				{
					Integer result = new Integer(i_left * i_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("float"))
				{
					Float result = new Float(f_left * f_right);
					return new Expression(new Literal(result));
				}
				if (DT.equals("double"))
				{
					Double result = new Double(d_left * d_right);
					return new Expression(new Literal(result));
				}
				/*
				 * if(DT.equals("string")){ String result=new
				 * String(s_left*s_right); value=result; }
				 */
				// Object result=left+RightOperand;

				// Object result = left.cast(LeftOperand) +
				// right.cast(RightOperand);
				break;

		}

		return null;
	}

	// The Stack after Post order Traversal is Traced and then checked.Finally the result is returned as String or Written to a file. 
	String EvaluateStack(Stack<Expression> s, HashMap<String, Integer> lcols2Pos, HashMap<Integer, String> lcols2Types, HashMap<String, Integer> rcols2Pos, HashMap<Integer, String> rcols2Types, boolean fileWriteFlag, String resultTemp)
	{
		
		Stack<String> stExpr = new Stack<String>();
		int i = 0;
		StringBuilder outputstring = new StringBuilder();
		
		STGroup group = new STGroupFile("./src/templates/templates.stg");
		
		// If Stack Size is one For now its assumed to be case can be skiped if both left and right operands exist for Equation 
		if (s.size() == 1)
		{
			Expression exp = s.elementAt(i);
			String var_final = "";
			this.CaseFinalVar = "";
			outputstring.append(CaseStatementEquivalance(exp.getCases(), lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, exp.getDefault(), var_final));
			Literal l1 = new Literal(this.CaseFinalVar);
			Expression e1 = new Expression(l1);
			s.set(i, e1);
		}
		while (s.size() > 1)
		{
			// If we find an Operator
			if (s.elementAt(i).getOp() != null)
			{

				
				String strOperator = s.elementAt(i).getOp();
				// System.out.println("String Operator"+strOperator);
				// rhs =2 lhs=1 operand side
				// Get operands both Left and Right
				String strRightOperand = ReturnOperand(s.elementAt(i - 1), lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, 2);
				String strLeftOperand = ReturnOperand(s.elementAt(i - 2), lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, 1);
				String OperatorType = getOpreatorType(s.elementAt(i).getOp());
				// Test if both operands are Literals
				// if true then substitute direct values
				String dataType;
				// If non condtional Operator we get the Final DataType else its just boolean
				if (OperatorType != "ConditionalOperator")
				{

					dataType = getDataType();
				}
				else
				{
					dataType = "boolean";
				}
				// LUCY'S CODE GOES HERE:

				boolean flag_temp = false;
				// Object left_val=s.elementAt(i-2).get
				if (strLeftOperand.lastIndexOf("temp") > -1 || strRightOperand.lastIndexOf("temp") > -1)
					flag_temp = true;
				
				// If both the operands are Literals we can Optimize and Evaluate them directly
				if (s.elementAt(i - 1).isLiteral() == true && s.elementAt(i - 2).isLiteral() == true && flag_temp == false)
				{
					if (OperatorType != "ConditionalOperator")
					{
						Object result;
						Expression e1 = handleConstants(s.elementAt(i - 2).getLiteral().getValue(), s.elementAt(i - 1).getLiteral().getValue(), strOperator, dataType);
						// Expression e1= new Expression(new Literal(result));
						logger.info("Optimization Result:"+e1.getLiteral().getValue());
						s.set(i, e1);
						s.removeElementAt(i - 2);
						s.removeElementAt(i - 2);
						i = i - 1;
					}
				}

				
				else
				{
					// Check if equation already Cached and use pre-existing computation.
					String cachesubexp = SubexprCache(strLeftOperand, strOperator, strRightOperand);
					if (cachesubexp != "")
					{
						Literal l1 = new Literal(cachesubexp);
						logger.info("Cached Expression Already Exists");
						// System.out.println("Left_type:"+this.left_type+"right_type:"+this.right_type+"Result_type="+dataType);
						s.set(i, new Expression(l1));
						// Update Stack with the final result found in cache
						s.removeElementAt(i - 2);
						s.removeElementAt(i - 2);
						i = i - 1;
					}
					else
					{
						// Generate Template for the equation
						ST stArithmeticOperator = group.getInstanceOf(OperatorType);
						stArithmeticOperator.add("dataType", dataType);
						stArithmeticOperator.add("varName", this.getTempVarName());
						stArithmeticOperator.add("leftOperand", strLeftOperand);
						stArithmeticOperator.add("operator", strOperator);
						stArithmeticOperator.add("rightOperand", strRightOperand);
						//System.out.println(stArithmeticOperator.render());
						logger.info("Rendered Equation"+stArithmeticOperator.render());
						// outputstring.append(System.lineSeparator());
						// outputstring.append
						outputstring.append(stArithmeticOperator.render());
						outputstring.append(System.lineSeparator());

						// stExpr.push(this.ConvertToExpression(strOperator,
						// strLeftOperand, strRightOperand));
						// New Literal is generated to put back on the stack to
						// be used on the next iterations
						resultTemp = new String(strTempVarName + Integer.toString(intTempVarCount - 1));
						//System.out.println("Result temp is:" + resultTemp);
						Literal l1 = new Literal(resultTemp);

						// Put the result temp variable with data type for
						// future mapping

						// LUCY'S CODE GOES HERE:
						
						vars.put(resultTemp, dataType);
						//System.out.println(resultTemp);
						//System.out.println("Left_type:" + this.left_type + "right_type:" + this.right_type + "Result_type=" + dataType);
						s.set(i, new Expression(l1));
						s.removeElementAt(i - 2);
						s.removeElementAt(i - 2);
						i = i - 1;
						insertSubExprCache(resultTemp, strLeftOperand, strOperator, strRightOperand);
						// insert into cache for optimization
					}
				}
			}
			else if (s.elementAt(i).isFunction())
			{
				// Manage the functions and Write the function output to
				// variable with the type
				String func = handleFunction(s.elementAt(i), lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, group);
				outputstring.append(func);
				Literal l1 = new Literal(strTempVarName + Integer.toString(intTempVarCount - 1));
				Expression e1 = new Expression(l1);
				s.set(i, e1);
			}
			else if (s.elementAt(i).isCase())
			{
				// Manage the case and No output is written back
				Expression exp = s.elementAt(i);
				String var_final = "";
				this.CaseFinalVar = "";
				outputstring.append(CaseStatementEquivalance(exp.getCases(), lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, exp.getDefault(), var_final));

				Literal l1 = new Literal(this.CaseFinalVar);
				Expression e1 = new Expression(l1);
				s.set(i, e1);
				// System.exit(0);
			}
			else
			{
				// Move forward on the stack
				i++;
			}
		}
		if (fileWriteFlag == true)
		{
			// Write to File
			writeToFile(group, outputstring.toString(), s);
			return null;
		}
		else
		{
			// just Return String
			return outputstring.toString();
		}
	}

	void writeToFile(STGroup group, String outputstring1, Stack<Expression> s)
	{
		File file = new File(fileNameString + ".java");
		try
		{

			if (file.createNewFile())
			{
				logger.info("Writing to File. File is created!");
			}
			else
			{
				logger.info("Writing to File. File already Exists!");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		// the whole code is stored in the output string and then written to the
		// code
		// Imports are stored in global variable import
		// Code takes the code that goes into the main function

		ST stClass = group.getInstanceOf("MyExpr");
		stClass.add("name", fileNameString);
		stClass.add("code", outputstring1);
		stClass.add("ReturnType", "Object");
		stClass.add("imports", imports.toString());
		if (s.lastElement() != null)
		{
			Literal ret = s.lastElement().getLiteral();

			stClass.add("returnObject", ret.getValue().toString());
		}
		else
		{
			stClass.add("returnObject", "null");
		}
		System.out.println(stClass.render());

		try
		{
			FileUtils.writeStringToFile(file, stClass.render());
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;

	}

	// The function returns type for temp variable in integer form used in the Hashmap Types.
	int getTypefromString(String input)
	{
		System.out.println();

		if (vars.containsKey(input))
		{

			for (Entry<Integer, String> entry : types.entrySet())
			{
				if (entry.getValue().equals(vars.get(input)))
				{
					return entry.getKey();
				}
			}
		}

		return -1;
	}

	// This Function handles all function independently, with imports and returns the String each function would generate 
	String functiontostring(String final_type, String final_var, String func_name, ArrayList<String> args, STGroup group)
	{
		StringBuilder output = new StringBuilder();
		if (func_name == "Min")
		{
			if (imports.indexOf("import java.lang.Math") == -1)
			{
				imports.append("\n");
				imports.append("import java.lang.Math;");
			}
			ST stClass = group.getInstanceOf("Function");
			stClass.add("dataType", final_type);
			stClass.add("varName", final_var);
			// stClass.add("funcName", "Math.min");
			stClass.add("code", generateFuncString(group, "Math.min", args));
			// stClass.add("imports",imports.toString());
			// Literal ret=s.lastElement().getLiteral();
			return stClass.render();

		}
		if (func_name == "Max")
		{
			if (imports.indexOf("import java.lang.Math") == -1)
			{
				imports.append("\n");
				imports.append("import java.lang.Math;");
			}
			ST stClass = group.getInstanceOf("Function");
			stClass.add("dataType", final_type);
			stClass.add("varName", final_var);
			// stClass.add("funcName", "Math.max");
			stClass.add("code", generateFuncString(group, "Math.max", args));
			// stClass.add("imports",imports.toString());
			// Literal ret=s.lastElement().getLiteral();
			return stClass.render();

		}
		if (func_name == "Concat")
		{
			if (imports.indexOf("import java.lang.StringBuilder") == -1)
			{
				imports.append("\n");
				imports.append("import java.lang.StringBuilder;");
			}
			// We use String builder for Concating String
			ST conc = group.getInstanceOf("ConcatFunc");
			StringBuilder sb = new StringBuilder();
			String varName = getSbVarName();
			conc.add("varname", varName);
			// sb.append("StringBuilder "+varName+"=new StringBuilder();");
			// sb.append("\n");
			for (int i = 0; i < args.size(); i++)
			{
				sb.append(varName + ".append(" + args.get(i) + ");");
				sb.append("\n");
			}

			conc.add("final_var", final_var);
			conc.add("code", sb.toString());
			// sb.append("String "+final_var+"="+varName+".toString()");
			logger.info("Renderd For Concat Function:"+sb.toString());
			return conc.render();

		}
		if (func_name == "Sum")
		{
			StringBuilder sb = new StringBuilder();
			sb.append(final_type + " " + final_var + "=");
			// add all arguments 
			for (int i = 0; i < args.size(); i++)
			{
				sb.append(args.get(i));
				if (i != args.size() - 1)
				{
					sb.append(" + ");
				}
				else
				{
					sb.append(";");
				}
			}
			return sb.toString();
		}
		if (func_name == "Avg")
		{
			StringBuilder sb = new StringBuilder();
			// similar to Sum, followed by divide by number of Args 
			sb.append(final_type + " " + final_var + "=(");
			for (int i = 0; i < args.size(); i++)
			{
				sb.append(args.get(i));
				if (i != args.size() - 1)
				{
					sb.append(" + ");
				}

			}
			sb.append(")/" + args.size() + ";");
			return sb.toString();
		}
		if (func_name == "Year")
		{
			logger.info("Year Operator found.");
			if (imports.indexOf("import java.text.SimpleDateFormat;") == -1)
			{
				imports.append("\n");
				imports.append("import java.text.SimpleDateFormat;\n");
				imports.append("import java.util.Calendar;\n");
				imports.append("import java.util.Date;\n");
				imports.append("import com.exascale.exceptions.ParseException;\n");
			}

			StringBuilder sb = new StringBuilder();
			sb.append("try{\n");
			sb.append("SimpleDateFormat formatter = new SimpleDateFormat(\"yyyy-MM-dd\");\n");
			sb.append("Date date = formatter.parse(" + args.get(0) + ");\n");
			sb.append("Calendar cal = Calendar.getInstance();\n");
			sb.append("cal.setTime(date);\n");
			sb.append(final_type + " " + final_var + "=cal.get(Calendar.YEAR);\n");
			sb.append("}\n");
			sb.append("catch(Exception e){\n");
			sb.append("e.printStackTrace();\n");
			sb.append("}\n");
			return sb.toString();

		}
		if (func_name == "Substring")
		{
			if (imports.indexOf("import java.lang.StringBuilder") == -1)
			{
				imports.append("\n");
				imports.append("import java.lang.StringBuilder;");
			}
			StringBuilder sb = new StringBuilder();
			String sbvar = getSbVarName();
			int start = 0, end = 0;
			if (args.size() > 2)
			{
				start = Integer.parseInt(args.get(1)) - 1;
				end = Integer.parseInt(args.get(2)) - 1;
			}
			else
			{
				start = 0;
				end = Integer.parseInt(args.get(1)) - 1;
			}
			ST subs = group.getInstanceOf("SubStringFunc");
			subs.add("varname", sbvar);
			subs.add("arg1", args.get(0));
			subs.add("start", start);
			subs.add("end", end);
			subs.add("final_var", final_var);
			/*
			 * sb.append("StringBuilder "+sbvar+"=new StringBuilder("+args.get(0)
			 * +");"); sb.append("\n");
			 * sb.append("String "+final_var+"="+sbvar+".substring("
			 * +start+","+end+");");
			 */
			vars.put(final_var, "String");
			return subs.render();
		}
		return "";
	}

	// Take arguments and Uses template to generate String for Function
	String generateFuncString(STGroup group, String name, ArrayList<String> args)
	{
		ST subfunc = group.getInstanceOf("SubFunction");
		StringBuilder sb = new StringBuilder();
		if (args.size() > 2)
		{
			subfunc.add("name", name);
			// sb.append("Math.min(");
			subfunc.add("arg1", args.get(args.size() - 1));
			// sb.append(args.get(args.size()-1));
			// sb.append(",");
			args.remove(args.size() - 1);
			subfunc.add("arg2", args.get(args.size() - 1));
			// sb.append(")");
			sb.append(subfunc.render());
			// subfunc.
			args.remove(args.size() - 1);
			for (int i = args.size() - 1; i >= 0; i--)
			{
				subfunc = group.getInstanceOf("SubFunction");
				subfunc.add("name", name);
				subfunc.add("arg1", args.get(args.size() - 1));
				subfunc.add("arg2", sb.toString());
				String str = subfunc.render();
				sb = new StringBuilder();
				sb.append(str);
				// sb.insert(0, str);
			}
		}
		else
		{
			subfunc.add("name", name);
			// sb.append("Math.min(");
			subfunc.add("arg1", args.get(args.size() - 1));
			// sb.append(args.get(args.size()-1));
			// sb.append(",");
			args.remove(args.size() - 1);
			subfunc.add("arg2", args.get(args.size() - 1));
			// sb.append(")");
			sb.append(subfunc.render());
			// subfunc.
			args.remove(args.size() - 1);

		}
		return sb.toString();

	}

	// This Function is called to manage Functions. Here we first generate string equivalent for each Argument for function and then manage each function Seprately. 	
	String handleFunction(Expression e, HashMap<String, Integer> lcols2Pos, HashMap<Integer, String> lcols2Types, HashMap<String, Integer> rcols2Pos, HashMap<Integer, String> rcols2Types, STGroup group)
	{
		StringBuilder functionOutput = new StringBuilder();
		// StringBuilder functionArgs=new StringBuilder();
		ArrayList<Expression> args = e.getFunction().getArgs();
		ArrayList<String> fargs = new ArrayList<String>();
		int type = -1;
		int curr_type = -1;
		// generate String for each Argument
		for (int i = 0; i < args.size(); i++)
		{
			Expression e1 = args.get(i);
			if (e1.getLHS() == null && e1.getRHS() == null && !e1.isFunction())
			{
				String op = ReturnOperand(e1, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, 1);
				// System.out.println(op);
				// functionArgs.append(op);
				fargs.add(op);
				if (curr_type == -1)
				{
					curr_type = this.left_type;
				}
				if (this.left_type > curr_type)
				{
					curr_type = this.left_type;
				}
				// functionOutput.append(",");
			}
			else if (e1.isFunction())
			{
				functionOutput.append(handleFunction(e1, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, group));
				// functionArgs.append(new
				// String(strTempVarName+Integer.toString(intTempVarCount-1)));
				fargs.add(new String(strTempVarName + Integer.toString(intTempVarCount - 1)));
				int type_value = getTypefromString(new String(strTempVarName + Integer.toString(intTempVarCount - 1)));
				if (curr_type < type_value)
				{
					curr_type = type_value;
				}
			}
			else
			{
				// This code is used when the argument to a function is itself and expression.
				Stack<Expression> s = postOrderTraversalIterative2(e1, lcols2Pos, lcols2Types);
				String res = null;
				//System.out.println("Yaha stack ka size hai:" + s.size());
				
				String op = EvaluateStack(s, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, false, res);
				Expression e5 = s.pop();
				//System.out.println("Pata chalega true hai va:" + (e5 != null));
				//System.out.println("Optimizarion stack::::" + s.size());
				// We just used Post Order Traversal and then add Evaluate Stack 

				functionOutput.append(op);
				String fres;

				if (e5 != null)
				{
					// System.out.println("KHUD EXTRACT KIYA:"+s.lastElement().getLiteral().getValue());
					String result = ReturnOperand(e5, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, 1);
					//System.out.println("Yaha ayya aur stack ka yeh hua:" + result);
					if (result.lastIndexOf("temp") > -1)
					{
						fres = new String(new String(strTempVarName + Integer.toString(intTempVarCount - 1)));
						//System.out.println("TEMP VARIABLE LIYA ");
						if (curr_type > getTypefromString(fres))
						{
							curr_type = getTypefromString(fres);
						}
						fargs.add(fres);
					}
					else
					{
						fres = result;
						//System.out.println("ORIGNAL VALUE VARIABLE LIYA: " + fres);
						if (curr_type > this.left_type)
						{
							curr_type = this.left_type;
						}
						fargs.add(fres);
					}
				}
				else
				{
					fres = new String(new String(strTempVarName + Integer.toString(intTempVarCount - 1)));
					//System.out.println("TEMP VARIABLE LIYA 2 ");
					if (curr_type > getTypefromString(fres))
					{
						curr_type = getTypefromString(fres);
					}
					fargs.add(fres);
				}
				// String fres= new String(new
				// String(strTempVarName+Integer.toString(intTempVarCount-1)));

				// functionArgs.append(fres);

			}
			// if(i!=args.size()-1){
			// functionArgs.append(",");
			// }

		}
		// Use template
		functionOutput.append("\n");
		// functionOutput.append(types.get(curr_type)+" "+this.getTempVarName()+"="+e.getFunction().getName()+"(");
		vars.put(new String(strTempVarName + Integer.toString(intTempVarCount - 1)), types.get(curr_type));

		// functionOutput.append(functionArgs.toString());
		// functionOutput.append(");");
		functionOutput.append(functiontostring(types.get(curr_type), this.getTempVarName(), e.getFunction().getName(), fargs, group));
		String final_var = new String(strTempVarName + Integer.toString(intTempVarCount - 1));
		// if final_var which function is assigned is not already in variables hashmap.
		if (!vars.containsKey(final_var))
			vars.put(new String(strTempVarName + Integer.toString(intTempVarCount - 1)), types.get(curr_type));
		functionOutput.append("\n");
		return functionOutput.toString();
	}

	static void decrement_temp()
	{
		intTempVarCount -= 1;

	}

	// This Function is for Handling Case Statement. We get Condition tree and then check each statement that is executed in that case. 
	public String CaseStatementEquivalance(ArrayList<Case> cases, HashMap<String, Integer> lcols2Pos, HashMap<Integer, String> lcols2Type, HashMap<String, Integer> rcols2Pos, HashMap<Integer, String> rcols2Type, Expression defaultResult, String varf)
	{
		StringBuilder returnResult = new StringBuilder();

		String temp1;
		String temp2;
		//System.out.println("Cases mei aya");
		String final_var = getTempVarName();
		returnResult.append("Object " + final_var + ";");
		returnResult.append("\n");
		vars.put(final_var, "Object");
		STGroup group = new STGroupFile("./src/templates/templates.stg");
		ST stArithmeticOperator;
		String condition;
		for (int i = 0; i <= cases.size(); i++)
		{
			StringBuilder result = new StringBuilder();
			if (i == 0)
			{
				// result.append("if");
				stArithmeticOperator = group.getInstanceOf("IfStatement");
			}
			else if (i == cases.size())
			{
				// result.append("else");
				stArithmeticOperator = group.getInstanceOf("ElseStatement");

				// decrement_temp();
			}
			else
			{
				// result.append("else if");
				stArithmeticOperator = group.getInstanceOf("ElseIfStatement");
				// decrement_temp();
			}

			if (i != cases.size())
			{
				// result=result.append("(");
				// get sql search statement
				SearchCondition p = cases.get(i).getCondition();
				// if (searchCondition) {cases.get(i).getResult() }
				//System.out.println("Chalp case chalo");
				// searchCondition =====> temp1 op constant
				Expression exprLeft = p.getClause().getPredicate().getLHS(); // column
				String op = p.getClause().getPredicate().getOp();
				Expression exprRight = p.getClause().getPredicate().getRHS();
				// Column Operand = e.getColumn();

				String strLeftOperand = caseExtractCondition(exprLeft, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type);
				String strRightOperand = caseExtractCondition(exprRight, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type);

				// according to column name to get the column position in a row

				temp2 = strLeftOperand + " " + op + " " + strRightOperand;
				condition = temp2;
				logger.info("Case Condition Generated is:"+temp2);
				//System.out.println("Condition is:" + temp2);
				// result.append(temp2);

				// result.append(")");
				// result=result.append("{");
				// result.append("\n");
				Stack<Expression> es = postOrderTraversalIterative2(cases.get(i).getResult(), lcols2Pos, lcols2Type);
				result.append(EvaluateStack(es, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type, false, null));
				if (es.size() > 0)
				{
					Expression e5 = es.pop();
					String computation_op = ReturnOperand(e5, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type, 1);
					result.append(final_var + "=" + computation_op + ";");
				}
				else
				{

					result.append(final_var + "=" + strTempVarName + Integer.toString(intTempVarCount - 1) + ";");
				}
				stArithmeticOperator.add("condition", condition);
				stArithmeticOperator.add("code", result.toString());
				returnResult.append(stArithmeticOperator.render());
				returnResult.append("\n");

			}

			if (i == cases.size())
			{
				// if case is the last one, result get defaultResult
				// result=result.append("{");
				// result.append("\n");
				Stack<Expression> es = postOrderTraversalIterative2(defaultResult, lcols2Pos, lcols2Type);
				result.append(EvaluateStack(es, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type, false, null));
				if (es.size() > 0)
				{
					Expression e5 = es.pop();
					String computation_op = ReturnOperand(e5, lcols2Pos, lcols2Type, rcols2Pos, rcols2Type, 1);
					result.append(final_var + "=" + computation_op + ";");
				}
				else
				{

					result.append(final_var + "=" + strTempVarName + Integer.toString(intTempVarCount - 1));
				}
				// stArithmeticOperator.add("condtion",condition);
				stArithmeticOperator.add("code", result.toString());
				returnResult.append(stArithmeticOperator.render());
				returnResult.append("\n");
			}
			result.append("\n");
			result = result.append("}");
			result.append("\n");
		}
		this.CaseFinalVar = final_var;
		return returnResult.toString();
	}

	void expressionEval(Expression root, HashMap<String, Integer> lcols2Pos, HashMap<Integer, String> lcols2Types, HashMap<String, Integer> rcols2Pos, HashMap<Integer, String> rcols2Types) throws IOException
	{
		// set up Logger
		logger = Logger.getLogger("LOG");
		BasicConfigurator.configure();
		fa = new RollingFileAppender(new PatternLayout("%d{ISO8601}\t%p\t%C{1}: %m%n"), "ExpressionCompilation.log", true);
		((RollingFileAppender)fa).setMaxBackupIndex(1);
		((RollingFileAppender)fa).setMaximumFileSize(2 * 1024L * 1024L * 1024L);
		fa.activateOptions();
		logger.addAppender(fa);
		logger.setLevel(Level.ALL);
		logger.info("Expression Compilation  Started.");
		Stack<Expression> s2 = postOrderTraversalIterative2(root, lcols2Pos, lcols2Types);
		this.fileNameString = getFileName();
		String s = EvaluateStack(s2, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, true, null);

	}
	
	// Used to Check if Expression  is already cached
	String SubexprCache(String strLeftOperand, String strOperator, String strRightOperand)
	{
		String result = "";
		// create a stringkey
		String strkey = strLeftOperand + strOperator + strRightOperand;

		// if we can find the key in the hashmap
		if (cache.containsKey(strkey))
		{
			return cache.get(strkey);
		}
		else
		{
			// if we can't find the key in the hashmap
			return result;
		}
	}

	//Insert into Cache , check if expression exists if not then insert.
	void insertSubExprCache(String resultTemp, String strLeftOperand, String strOperator, String strRightOperand)
	{
		String result = this.SubexprCache(strLeftOperand, strOperator, strRightOperand);
		String key;
		String value;
		String key1;

		// if we can't find the same value in the hashmap
		if (result.equals(""))
		{
			key = strLeftOperand + strOperator + strRightOperand;
			value = resultTemp;
			// when operator is + and *, we can get the same result even when we
			// interchange the leftoperand and rightoperand
			if (strOperator == "+" || strOperator == "*")
			{
				key1 = strRightOperand + strOperator + strLeftOperand;
				// store the key and value to hashmap
				cache.put(key, value);
				cache.put(key1, value);

			}
			else
			{
				cache.put(key, value);
			}
		}

	}
	
	// This Function is used to extract the case condition
	String caseExtractCondition(Expression root, HashMap<String, Integer> lcols2Pos, HashMap<Integer, String> lcols2Types, HashMap<String, Integer> rcols2Pos, HashMap<Integer, String> rcols2Types)
	{
		// For left Operand
		// System.out.println("Extracting Conditions");
		Stack<Expression> s = postOrderTraversalIterative2(root, lcols2Pos, lcols2Types);
		String res = "";
		//System.out.println("Stack Size:" + s.size());
		if (s.size() == 1)
		{
			return ReturnOperand(root, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, 1);

		}
		String traversalResult = EvaluateStack(s, lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, false, res);
		String[] splitTR = traversalResult.split("\n");
		int k = 0;
		//System.out.println("TraversalResult:" + traversalResult);
		if (traversalResult.length() < 1 && s.size() == 1)
		{
			return ReturnOperand(s.pop(), lcols2Pos, lcols2Types, rcols2Pos, rcols2Types, 1);
		}
		StringBuilder sb1 = new StringBuilder();
		while (k < splitTR.length)
		{
			String extract = splitTR[k].split("=")[1];
			//System.out.println("Extract:" + extract);
			extract = extract.split(";")[0];
			sb1.append(extract);
			sb1.append(" ");
			k++;
		}
		
		return sb1.toString();
	}
}
