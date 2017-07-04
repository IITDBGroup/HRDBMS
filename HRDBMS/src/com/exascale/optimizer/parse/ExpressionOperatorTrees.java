package com.exascale.optimizer.parse;

import com.exascale.exceptions.ParseException;
import com.exascale.optimizer.*;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.exascale.optimizer.parse.SQLParser.*;

/** SQL Parsing logic for expressions */
public class ExpressionOperatorTrees extends AbstractParseController {
    /** Initialize with information that's needed to build a plan for any configuration of operators */
    public ExpressionOperatorTrees(ConnectionWorker connection, Transaction tx, MetaData meta, SQLParser.Model model) {
        super(connection, tx, meta, model);
    }

    SQLParser.OperatorTypeAndName buildOperatorTreeFromExpression(final Expression exp, String name, final SubSelect sub) throws ParseException
    {
        if (exp.isLiteral())
        {
            final Literal l = exp.getLiteral();
            if (l.isNull())
            {
                // TODO
            }
            else
            {
                final Object literal = l.getValue();
                if (literal instanceof Double || literal instanceof Long)
                {
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new ExtendOperator(literal.toString(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "." + "_E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new ExtendOperator(literal.toString(), name, meta), TYPE_INLINE, name, -1);
                    }
                }
                else
                {
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new ExtendObjectOperator(literal, sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "." + "_E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new ExtendObjectOperator(literal, name, meta), TYPE_INLINE, name, -1);
                    }
                }
            }
        }
        else if (exp.isCountStar())
        {
            if (name != null)
            {
                String sgetName = name;
                if (!sgetName.contains("."))
                {
                    sgetName = "." + sgetName;
                }
                return new SQLParser.OperatorTypeAndName(new CountOperator(sgetName, meta), TYPE_GROUPBY, sgetName, -1);
            }
            else
            {
                name = "." + "_E" + model.getAndIncrementSuffix();
                return new SQLParser.OperatorTypeAndName(new CountOperator(name, meta), TYPE_GROUPBY, name, -1);
            }
        }
        else if (exp.isList())
        {
            throw new ParseException("A list is not supported in the context where it is used");
        }
        else if (exp.isSelect())
        {
            throw new ParseException("A SELECT statement is not supported in the context where it is used");
        }
        else if (exp.isCase())
        {
            final ArrayList<HashSet<HashMap<Filter, Filter>>> alhshm = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
            final ArrayList<String> results = new ArrayList<String>();
            int prereq = -1;
            String type = null;
            for (final Case c : exp.getCases())
            {
                // handle search condition later
                if (c.getResult().isColumn())
                {
                    final Column input = c.getResult().getColumn();
                    String inputColumn = "";
                    if (input.getTable() != null)
                    {
                        inputColumn += (input.getTable() + ".");
                    }
                    else
                    {
                        inputColumn += ".";
                    }
                    String end = input.getColumn();
                    if (end.contains("."))
                    {
                        end = end.substring(end.indexOf('.') + 1);
                    }
                    inputColumn += end;

                    results.add(inputColumn);
                    continue;
                }
                else if (c.getResult().isLiteral())
                {
                    if (c.getResult().getLiteral().isNull())
                    {
                        // TODO
                    }

                    final Object obj = c.getResult().getLiteral().getValue();
                    if (obj instanceof String)
                    {
                        results.add("'" + obj + "'");
                        if (type != null && !type.equals("CHAR"))
                        {
                            throw new ParseException("All posible results of a case expression must have the same type");
                        }
                        type = "CHAR";
                    }
                    else if (obj instanceof Double)
                    {
                        if (type == null)
                        {
                            type = "FLOAT";
                        }
                        else if (type.equals("LONG"))
                        {
                            // fix
                            int i = 0;
                            for (final String result : (ArrayList<String>)results.clone())
                            {
                                results.remove(i);
                                results.add(i, result + ".0");
                                i++;
                            }
                        }
                        else if (type.equals("FLOAT"))
                        {
                        }
                        else
                        {
                            throw new ParseException("All possible results of a case expression must have the same type");
                        }
                        results.add(obj.toString());
                    }
                    else if (obj instanceof Long)
                    {
                        if (type == null)
                        {
                            type = "LONG";
                            results.add(obj.toString());
                        }
                        else if (type.equals("LONG"))
                        {
                            results.add(obj.toString());
                        }
                        else if (type.equals("FLOAT"))
                        {
                            results.add(obj.toString() + ".0");
                        }
                        else
                        {
                            throw new ParseException("All possible results of a case expression must have the same type");
                        }
                    }

                    continue;
                }

                final SQLParser.OperatorTypeAndName otan = buildOperatorTreeFromExpression(c.getResult(), null, sub);
                if (otan.getType() == TYPE_DAYS)
                {
                    throw new ParseException("A case expression cannot return a result of type DAYS");
                }
                else if (otan.getType() == TYPE_MONTHS)
                {
                    throw new ParseException("A case expression cannot return a result of type MONTHS");
                }
                else if (otan.getType() == TYPE_YEARS)
                {
                    throw new ParseException("A case expression cannot return a result of type YEARS");
                }
                else if (otan.getType() == TYPE_GROUPBY)
                {
                    throw new ParseException("A case expression cannot perform an aggregation operation");
                }
                else if (otan.getType() == TYPE_DATE)
                {
                    final Date date = ((GregorianCalendar)otan.getOp()).getTime();
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    final String dateString = sdf.format(date);
                    results.add("DATE('" + dateString + "')");
                    if (type == null)
                    {
                        type = "DATE";
                    }
                    else if (!type.equalsIgnoreCase("DATE"))
                    {
                        throw new ParseException("All possible results of a case expression must have the same type");
                    }
                }
                else
                {
                    results.add(otan.getName());

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(otan.getName());
                    row.add(otan.getOp());
                    row.add(otan.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(c.getResult());
                    row.add(otan.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (prereq != -1)
                    {
                        // go all the way to the bottom and set the prereq to
                        // prereq
                        // then set prereq to the id of this one
                        final ArrayList<Object> bottom = getBottomRow(row);
                        bottom.remove(5);
                        bottom.add(5, prereq);
                        prereq = (Integer)row.get(3);
                    }
                    else
                    {
                        prereq = (Integer)row.get(3);
                    }
                }
            }

            if (exp.getDefault().isColumn())
            {
                final Column input = exp.getDefault().getColumn();
                String inputColumn = "";
                if (input.getTable() != null)
                {
                    inputColumn += (input.getTable() + ".");
                }
                else
                {
                    inputColumn += ".";
                }
                String end = input.getColumn();
                if (end.contains("."))
                {
                    end = end.substring(end.indexOf('.') + 1);
                }
                inputColumn += end;

                results.add(inputColumn);
            }
            else if (exp.getDefault().isLiteral())
            {
                if (exp.getDefault().getLiteral().isNull())
                {
                    // TODO
                }

                final Object obj = exp.getDefault().getLiteral().getValue();
                if (obj instanceof String)
                {
                    results.add("'" + obj + "'");
                    if (type != null && !type.equals("CHAR"))
                    {
                        throw new ParseException("All posible results of a case expression must have the same type");
                    }
                    type = "CHAR";
                }
                else if (obj instanceof Double)
                {
                    if (type == null)
                    {
                        type = "FLOAT";
                    }
                    else if (type.equals("LONG"))
                    {
                        // fix
                        int i = 0;
                        for (final String result : (ArrayList<String>)results.clone())
                        {
                            results.remove(i);
                            results.add(i, result + ".0");
                            i++;
                        }
                    }
                    else if (type.equals("FLOAT"))
                    {
                    }
                    else
                    {
                        throw new ParseException("All possible results of a case expression must have the same type");
                    }
                    results.add(obj.toString());
                }
                else if (obj instanceof Long)
                {
                    if (type == null)
                    {
                        type = "LONG";
                        results.add(obj.toString());
                    }
                    else if (type.equals("LONG"))
                    {
                        results.add(obj.toString());
                    }
                    else if (type.equals("FLOAT"))
                    {
                        results.add(obj.toString() + ".0");
                    }
                    else
                    {
                        throw new ParseException("All possible results of a case expression must have the same type");
                    }
                }
            }
            else
            {
                final SQLParser.OperatorTypeAndName otan = buildOperatorTreeFromExpression(exp.getDefault(), null, sub);
                if (otan.getType() == TYPE_DAYS)
                {
                    throw new ParseException("A case expression cannot return a result of type DAYS");
                }
                else if (otan.getType() == TYPE_MONTHS)
                {
                    throw new ParseException("A case expression cannot return a result of type MONTHS");
                }
                else if (otan.getType() == TYPE_YEARS)
                {
                    throw new ParseException("A case expression cannot return a result of type YEARS");
                }
                else if (otan.getType() == TYPE_GROUPBY)
                {
                    throw new ParseException("A case expression cannot perform an aggregation operation");
                }
                else if (otan.getType() == TYPE_DATE)
                {
                    final Date date = ((GregorianCalendar)otan.getOp()).getTime();
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    final String dateString = sdf.format(date);
                    results.add("DATE('" + dateString + "')");
                    if (type == null)
                    {
                        type = "DATE";
                    }
                    else if (!type.equalsIgnoreCase("DATE"))
                    {
                        throw new ParseException("All possible results of a case expression must have the same type");
                    }
                }
                else
                {
                    results.add(otan.getName());

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(otan.getName());
                    row.add(otan.getOp());
                    row.add(otan.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getDefault());
                    row.add(otan.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (prereq != -1)
                    {
                        // go all the way to the bottom and set the prereq to
                        // prereq
                        // then set prereq to the id of this one
                        final ArrayList<Object> bottom = getBottomRow(row);
                        bottom.remove(5);
                        bottom.add(5, prereq);
                        prereq = (Integer)row.get(3);
                    }
                    else
                    {
                        prereq = (Integer)row.get(3);
                    }
                }
            }

            if (name != null)
            {
                String sgetName = name;
                if (!sgetName.contains("."))
                {
                    sgetName = "." + sgetName;
                }
                return new SQLParser.OperatorTypeAndName(new CaseOperator(alhshm, results, sgetName, type, meta), TYPE_INLINE, sgetName, prereq);
            }
            else
            {
                name = "._E" + model.getAndIncrementSuffix();
                return new SQLParser.OperatorTypeAndName(new CaseOperator(alhshm, results, name, type, meta), TYPE_INLINE, name, prereq);
            }
        }
        else if (exp.isFunction())
        {
            final Function f = exp.getFunction();
            final String method = f.getName();
            if (method.equals("MAX"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("MAX() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (arg.isColumn())
                {
                    final Column input = arg.getColumn();
                    String inputColumn = "";
                    if (input.getTable() != null)
                    {
                        inputColumn += (input.getTable() + ".");
                    }
                    else
                    {
                        inputColumn += ".";
                    }
                    String end = input.getColumn();
                    if (end.contains("."))
                    {
                        end = end.substring(end.indexOf('.') + 1);
                    }
                    inputColumn += end;
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new MaxOperator(inputColumn, sgetName, meta, true), TYPE_GROUPBY, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new MaxOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
                    }
                }
                else if (arg.isExpression() || arg.isCase() || arg.isFunction())
                {
                    final SQLParser.OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
                    if (retval.getType() == TYPE_DATE)
                    {
                        throw new ParseException("The argument to MAX() cannot be a literal DATE");
                    }
                    else if (retval.getType() == TYPE_DAYS)
                    {
                        throw new ParseException("The argument to MAX() cannot be of type DAYS");
                    }
                    else if (retval.getType() == TYPE_MONTHS)
                    {
                        throw new ParseException("The argument to MAX() cannot be of type MONTHS");
                    }
                    else if (retval.getType() == TYPE_YEARS)
                    {
                        throw new ParseException("The argument to MAX() cannot be of type YEARS");
                    }
                    else if (retval.getType() == TYPE_GROUPBY)
                    {
                        throw new ParseException("The argument to MAX() cannot be an aggregation");
                    }

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(retval.getName());
                    row.add(retval.getOp());
                    row.add(retval.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(arg);
                    row.add(retval.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new MaxOperator(retval.getName(), sgetName, meta, true), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new MaxOperator(retval.getName(), name, meta, true), TYPE_GROUPBY, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    throw new ParseException("MAX() requires 1 argument that is a column or an expression");
                }
            }
            else if (method.equals("MIN"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("MIN() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (arg.isColumn())
                {
                    final Column input = arg.getColumn();
                    String inputColumn = "";
                    if (input.getTable() != null)
                    {
                        inputColumn += (input.getTable() + ".");
                    }
                    else
                    {
                        inputColumn += ".";
                    }

                    String end = input.getColumn();
                    if (end.contains("."))
                    {
                        end = end.substring(end.indexOf('.') + 1);
                    }
                    inputColumn += end;
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new MinOperator(inputColumn, sgetName, meta, true), TYPE_GROUPBY, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new MinOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
                    }
                }
                else if (arg.isExpression() || arg.isCase() || arg.isFunction())
                {
                    final SQLParser.OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
                    if (retval.getType() == TYPE_DATE)
                    {
                        throw new ParseException("The argument to MIN() cannot be a literal DATE");
                    }
                    else if (retval.getType() == TYPE_DAYS)
                    {
                        throw new ParseException("The argument to MIN() cannot be of type DAYS");
                    }
                    else if (retval.getType() == TYPE_MONTHS)
                    {
                        throw new ParseException("The argument to MIN() cannot be of type MONTHS");
                    }
                    else if (retval.getType() == TYPE_YEARS)
                    {
                        throw new ParseException("The argument to MIN() cannot be of type YEARS");
                    }
                    else if (retval.getType() == TYPE_GROUPBY)
                    {
                        throw new ParseException("The argument to MIN() cannot be an aggregation");
                    }

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(retval.getName());
                    row.add(retval.getOp());
                    row.add(retval.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(arg);
                    row.add(retval.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new MinOperator(retval.getName(), sgetName, meta, true), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new MinOperator(retval.getName(), name, meta, true), TYPE_GROUPBY, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    throw new ParseException("MIN() requires an argument that is a column or an expression");
                }
            }
            else if (method.equals("AVG"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("AVG() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (arg.isColumn())
                {
                    final Column input = arg.getColumn();
                    String inputColumn = "";
                    if (input.getTable() != null)
                    {
                        inputColumn += (input.getTable() + ".");
                    }
                    else
                    {
                        inputColumn += ".";
                    }
                    String end = input.getColumn();
                    if (end.contains("."))
                    {
                        end = end.substring(end.indexOf('.') + 1);
                    }
                    inputColumn += end;
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new AvgOperator(inputColumn, sgetName, meta), TYPE_GROUPBY, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new AvgOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
                    }
                }
                else if (arg.isExpression() || arg.isCase() || arg.isFunction())
                {
                    final SQLParser.OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
                    if (retval.getType() == TYPE_DATE)
                    {
                        throw new ParseException("The argument to AVG() cannot be a literal DATE");
                    }
                    else if (retval.getType() == TYPE_DAYS)
                    {
                        throw new ParseException("The argument to AVG() cannot be of type DAYS");
                    }
                    else if (retval.getType() == TYPE_MONTHS)
                    {
                        throw new ParseException("The argument to AVG() cannot be of type MONTHS");
                    }
                    else if (retval.getType() == TYPE_YEARS)
                    {
                        throw new ParseException("The argument to AVG() cannot be of type YEARS");
                    }
                    else if (retval.getType() == TYPE_GROUPBY)
                    {
                        throw new ParseException("The argument to AVG() cannot be an aggregation");
                    }

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(retval.getName());
                    row.add(retval.getOp());
                    row.add(retval.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(arg);
                    row.add(retval.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new AvgOperator(retval.getName(), sgetName, meta), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new AvgOperator(retval.getName(), name, meta), TYPE_GROUPBY, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    throw new ParseException("AVG() requires 1 argument that is a column or an expression");
                }
            }
            else if (method.equals("SUM"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("SUM() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (arg.isColumn())
                {
                    final Column input = arg.getColumn();
                    String inputColumn = "";
                    if (input.getTable() != null)
                    {
                        inputColumn += (input.getTable() + ".");
                    }
                    else
                    {
                        inputColumn += ".";
                    }
                    String end = input.getColumn();
                    if (end.contains("."))
                    {
                        end = end.substring(end.indexOf('.') + 1);
                    }
                    inputColumn += end;
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new SumOperator(inputColumn, sgetName, meta, true), TYPE_GROUPBY, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new SumOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
                    }
                }
                else if (arg.isExpression() || arg.isCase() || arg.isFunction())
                {
                    final SQLParser.OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
                    if (retval.getType() == TYPE_DATE)
                    {
                        throw new ParseException("The argument to SUM() cannot be a literal DATE");
                    }
                    else if (retval.getType() == TYPE_DAYS)
                    {
                        throw new ParseException("The argument to SUM() cannot be of type DAYS");
                    }
                    else if (retval.getType() == TYPE_MONTHS)
                    {
                        throw new ParseException("The argument to SUM() cannot be of type MONTHS");
                    }
                    else if (retval.getType() == TYPE_YEARS)
                    {
                        throw new ParseException("The argument to SUM() cannot be of type YEARS");
                    }
                    else if (retval.getType() == TYPE_GROUPBY)
                    {
                        throw new ParseException("The argument to SUM() cannot be an aggregation");
                    }

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(retval.getName());
                    row.add(retval.getOp());
                    row.add(retval.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(arg);
                    row.add(retval.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new SumOperator(retval.getName(), sgetName, meta, true), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new SumOperator(retval.getName(), name, meta, true), TYPE_GROUPBY, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    throw new ParseException("SUM() requires 1 argument that is a column or an expression");
                }
            }
            else if (method.equals("COUNT"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("COUNT() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (arg.isColumn())
                {
                    final Column input = arg.getColumn();
                    String inputColumn = "";
                    if (input.getTable() != null)
                    {
                        inputColumn += (input.getTable() + ".");
                    }
                    else
                    {
                        inputColumn += ".";
                    }
                    String end = input.getColumn();
                    if (end.contains("."))
                    {
                        end = end.substring(end.indexOf('.') + 1);
                    }
                    inputColumn += end;
                    if (name != null)
                    {
                        if (f.getDistinct())
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new CountDistinctOperator(inputColumn, sgetName, meta), TYPE_GROUPBY, sgetName, -1);
                        }
                        else
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new CountOperator(inputColumn, sgetName, meta), TYPE_GROUPBY, sgetName, -1);
                        }
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        if (f.getDistinct())
                        {
                            return new SQLParser.OperatorTypeAndName(new CountDistinctOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
                        }
                        else
                        {
                            return new SQLParser.OperatorTypeAndName(new CountOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
                        }
                    }
                }
                else
                {
                    throw new ParseException("COUNT() requires 1 argument that is a column");
                }
            }
            else if (method.equals("DATE"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("DATE() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (!arg.isLiteral())
                {
                    throw new ParseException("DATE() requires a literal string argument");
                }

                final Object literal = arg.getLiteral().getValue();
                if (!(literal instanceof String))
                {
                    throw new ParseException("DATE() requires a literal string argument");
                }
                final String dateString = (String)literal;
                final GregorianCalendar cal = new GregorianCalendar();
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try
                {
                    final Date date = sdf.parse(dateString);
                    cal.setTime(date);
                    return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                }
                catch (final Exception e)
                {
                    throw new ParseException("Date is not in the proper format");
                }
            }
            else if (method.equals("DAYS"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("DAYS() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (!arg.isLiteral())
                {
                    throw new ParseException("DAYS() requires a literal numeric argument");
                }

                final Object literal = arg.getLiteral().getValue();
                if (literal instanceof Integer)
                {
                    return new SQLParser.OperatorTypeAndName(literal, TYPE_DAYS, "", -1);
                }
                else if (literal instanceof Long)
                {
                    return new SQLParser.OperatorTypeAndName(new Integer(((Long)literal).intValue()), TYPE_DAYS, "", -1);
                }
                else
                {
                    throw new ParseException("DAYS() requires a literal numeric argument");
                }
            }
            else if (method.equals("MONTHS"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("MONTHS() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (!arg.isLiteral())
                {
                    throw new ParseException("MONTHS() requires a literal numeric argument");
                }

                final Object literal = arg.getLiteral().getValue();
                if (literal instanceof Integer)
                {
                    return new SQLParser.OperatorTypeAndName(literal, TYPE_MONTHS, "", -1);
                }
                else if (literal instanceof Long)
                {
                    return new SQLParser.OperatorTypeAndName(new Integer(((Long)literal).intValue()), TYPE_MONTHS, "", -1);
                }
                else
                {
                    throw new ParseException("MONTHS() requires a literal numeric argument");
                }
            }
            else if (method.equals("YEARS"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("YEARS() requires only 1 argument");
                }
                final Expression arg = args.get(0);
                if (!arg.isLiteral())
                {
                    throw new ParseException("YEARS() requires a literal numeric argument");
                }

                final Object literal = arg.getLiteral().getValue();
                if (literal instanceof Integer)
                {
                    return new SQLParser.OperatorTypeAndName(literal, TYPE_YEARS, "", -1);
                }
                else if (literal instanceof Long)
                {
                    return new SQLParser.OperatorTypeAndName(new Integer(((Long)literal).intValue()), TYPE_YEARS, "", -1);
                }
                else
                {
                    throw new ParseException("YEARS() requires a literal numeric argument");
                }
            }
            else if (method.equals("YEAR"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 1)
                {
                    throw new ParseException("YEAR() requires only 1 argument");
                }
                final Expression arg = args.get(0);

                if (arg.isColumn())
                {
                    String colName = "";
                    final Column col = arg.getColumn();
                    if (col.getTable() != null)
                    {
                        colName += col.getTable();
                    }

                    colName += ("." + col.getColumn());

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new YearOperator(colName, sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new YearOperator(colName, name, meta), TYPE_INLINE, name, -1);
                    }
                }
                else if (arg.isFunction() || arg.isExpression() || arg.isCase())
                {
                    final SQLParser.OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
                    if (retval.getType() == TYPE_DATE)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)retval.getOp();
                        final int literal = cal.get(Calendar.YEAR);

                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new ExtendOperator(Integer.toString(literal), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new ExtendOperator(Integer.toString(literal), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else if (retval.getType() == TYPE_DAYS)
                    {
                        throw new ParseException("YEAR() cannot be called with an argument of type DAYS");
                    }
                    else if (retval.getType() == TYPE_MONTHS)
                    {
                        throw new ParseException("YEAR() cannot be called with an argument of type MONTHS");
                    }
                    else if (retval.getType() == TYPE_YEARS)
                    {
                        throw new ParseException("YEAR() cannot be called with an argument of type YEARS");
                    }
                    else if (retval.getType() == TYPE_GROUPBY)
                    {
                        throw new ParseException("YEAR() cannot be called with an agrument that is an aggregation");
                    }

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(retval.getName());
                    row.add(retval.getOp());
                    row.add(retval.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(arg);
                    row.add(retval.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new YearOperator(retval.getName(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new YearOperator(retval.getName(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    throw new ParseException("YEAR() requires an argument that is a column, a function, or an expression");
                }
            }
            else if (method.equals("SUBSTRING"))
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args.size() != 3 && args.size() != 2)
                {
                    throw new ParseException("SUBSTRING() requires 2 or 3 arguments");
                }
                Expression arg = args.get(1);
                int start;
                int end = -1;
                if (!arg.isLiteral())
                {
                    throw new ParseException("Argument 2 of SUBSTRING() must be a numeric literal");
                }

                Object val = arg.getLiteral().getValue();
                if (val instanceof Integer)
                {
                    start = (Integer)val - 1;
                    if (start < 0)
                    {
                        throw new ParseException("Argument 2 of SUBSTRING() cannot be less than 1");
                    }
                }
                else if (val instanceof Long)
                {
                    start = ((Long)val).intValue() - 1;
                    if (start < 0)
                    {
                        throw new ParseException("Argument 2 of SUBSTRING() cannot be less than 1");
                    }
                }
                else
                {
                    throw new ParseException("Argument 2 of SUBSTRING() must be a numeric literal");
                }

                if (args.size() == 3)
                {
                    int length;
                    arg = args.get(2);
                    if (!arg.isLiteral())
                    {
                        throw new ParseException("Argument 3 of SUBSTRING() must be a numeric literal");
                    }

                    val = arg.getLiteral().getValue();
                    if (val instanceof Integer)
                    {
                        length = (Integer)val;
                        if (length < 0)
                        {
                            throw new ParseException("Argument 3 of SUBSTRING() cannot be less than 0");
                        }

                        end = start + length;
                    }
                    else if (val instanceof Long)
                    {
                        length = ((Long)val).intValue();
                        if (length < 0)
                        {
                            throw new ParseException("Argument 3 of SUBSTRING() cannot be less than 0");
                        }

                        end = start + length;
                    }
                    else
                    {
                        throw new ParseException("Argument 3 of SUBSTRING() must be a numeric literal");
                    }
                }

                arg = args.get(0);
                if (arg.isLiteral() || arg.isFunction() || arg.isExpression() || arg.isCase())
                {
                    final SQLParser.OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
                    if (retval.getType() == TYPE_DATE || retval.getType() == TYPE_DAYS || retval.getType() == TYPE_MONTHS || retval.getType() == TYPE_YEARS || retval.getType() == TYPE_GROUPBY)
                    {
                        throw new ParseException("Argument 1 to SUBSTRING must be a string");
                    }

                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(retval.getName());
                    row.add(retval.getOp());
                    row.add(retval.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(arg);
                    row.add(retval.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new SubstringOperator(retval.getName(), start, end, sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new SubstringOperator(retval.getName(), start, end, name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                    }
                }
                else if (arg.isColumn())
                {
                    String colName = "";
                    final Column col = arg.getColumn();
                    if (col.getTable() != null)
                    {
                        colName += col.getTable();
                    }

                    colName += ("." + col.getColumn());

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new SubstringOperator(colName, start, end, sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new SubstringOperator(colName, start, end, name, meta), TYPE_INLINE, name, -1);
                    }
                }
                else
                {
                    throw new ParseException("The first argument to SUBSTRING() must be a literal, a column, a function, or an expression");
                }
            }
        }
        else if (exp.isExpression())
        {
            final SQLParser.OperatorTypeAndName exp1 = buildOperatorTreeFromExpression(exp.getLHS(), null, sub);
            final SQLParser.OperatorTypeAndName exp2 = buildOperatorTreeFromExpression(exp.getRHS(), null, sub);
            Column col1 = null;
            Column col2 = null;
            if (exp1 == null)
            {
                col1 = exp.getLHS().getColumn();
            }

            if (exp2 == null)
            {
                col2 = exp.getRHS().getColumn();
            }

            if (exp1 != null && exp1.getType() == TYPE_DATE)
            {
                if (exp.getOp().equals("+"))
                {
                    if (exp2 != null && exp2.getType() == TYPE_DAYS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
                        cal.add(Calendar.DATE, (Integer)exp2.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else if (exp2 != null && exp2.getType() == TYPE_MONTHS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
                        cal.add(Calendar.MONTH, (Integer)exp2.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else if (exp2 != null && exp2.getType() == TYPE_YEARS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
                        cal.add(Calendar.YEAR, (Integer)exp2.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else
                    {
                        throw new ParseException("Only types DAYS/MONTHS/YEARS can be added to a DATE");
                    }
                }
                else if (exp.getOp().equals("-"))
                {
                    if (exp2 != null && exp2.getType() == TYPE_DAYS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
                        cal.add(Calendar.DATE, -1 * (Integer)exp2.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else if (exp2 != null && exp2.getType() == TYPE_MONTHS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
                        cal.add(Calendar.MONTH, -1 * (Integer)exp2.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else if (exp2 != null && exp2.getType() == TYPE_YEARS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
                        cal.add(Calendar.YEAR, -1 * (Integer)exp2.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else
                    {
                        throw new ParseException("Only type DAYS/MONTHS/YEARS can be substracted from a DATE");
                    }
                }
                else
                {
                    throw new ParseException("Only the + and - operators are valid with type DATE");
                }
            }
            else if (exp2 != null && exp2.getType() == TYPE_DATE)
            {
                if (exp.getOp().equals("+"))
                {
                    if (exp1 != null && exp1.getType() == TYPE_DAYS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
                        cal.add(Calendar.DATE, (Integer)exp1.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else if (exp1 != null && exp1.getType() == TYPE_MONTHS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
                        cal.add(Calendar.MONTH, (Integer)exp1.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else if (exp1 != null && exp1.getType() == TYPE_YEARS)
                    {
                        final GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
                        cal.add(Calendar.YEAR, (Integer)exp1.getOp());
                        return new SQLParser.OperatorTypeAndName(cal, TYPE_DATE, "", -1);
                    }
                    else
                    {
                        throw new ParseException("Only type DAYS/MONTHS/YEARS can be added to a DATE");
                    }
                }
                else
                {
                    throw new ParseException("Only a DATE can be added to type DAYS/MONTHS/YEARS");
                }
            }
            else if (exp1 != null && exp1.getType() == TYPE_DAYS)
            {
                // already handled DAYS + DATE
                // so need to handle DAYS + col(DATE)
                // and DAYS +/- DAYS
                if (exp2 != null && exp2.getType() == TYPE_DAYS)
                {
                    if (exp.getOp().equals("+"))
                    {
                        return new SQLParser.OperatorTypeAndName((Integer)exp1.getOp() + (Integer)exp2.getOp(), TYPE_DAYS, "", -1);
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        return new SQLParser.OperatorTypeAndName((Integer)exp1.getOp() - (Integer)exp2.getOp(), TYPE_DAYS, "", -1);
                    }
                }

                // externalize exp2 if not col
                if (exp2 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp2.getName());
                    row.add(exp2.getOp());
                    row.add(exp2.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getRHS());
                    row.add(exp2.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_DAYS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_DAYS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    String colString2 = "";
                    if (col2.getTable() != null)
                    {
                        colString2 = col2.getTable();
                    }
                    colString2 += ("." + col2.getColumn());
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString2, TYPE_DAYS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString2, TYPE_DAYS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, -1);
                    }
                }
            }
            else if (exp1 != null && exp1.getType() == TYPE_MONTHS)
            {
                if (exp2 != null && exp2.getType() == TYPE_MONTHS)
                {
                    if (exp.getOp().equals("+"))
                    {
                        return new SQLParser.OperatorTypeAndName((Integer)exp1.getOp() + (Integer)exp2.getOp(), TYPE_MONTHS, "", -1);
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        return new SQLParser.OperatorTypeAndName((Integer)exp1.getOp() - (Integer)exp2.getOp(), TYPE_MONTHS, "", -1);
                    }
                }

                // externalize exp2 if not col
                if (exp2 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp2.getName());
                    row.add(exp2.getOp());
                    row.add(exp2.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getRHS());
                    row.add(exp2.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_MONTHS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_MONTHS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    String colString2 = "";
                    if (col2.getTable() != null)
                    {
                        colString2 = col2.getTable();
                    }
                    colString2 += ("." + col2.getColumn());
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString2, TYPE_MONTHS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString2, TYPE_MONTHS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, -1);
                    }
                }
            }
            else if (exp1 != null && exp1.getType() == TYPE_YEARS)
            {
                if (exp2 != null && exp2.getType() == TYPE_YEARS)
                {
                    if (exp.getOp().equals("+"))
                    {
                        return new SQLParser.OperatorTypeAndName((Integer)exp1.getOp() + (Integer)exp2.getOp(), TYPE_YEARS, "", -1);
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        return new SQLParser.OperatorTypeAndName((Integer)exp1.getOp() - (Integer)exp2.getOp(), TYPE_YEARS, "", -1);
                    }
                }

                // externalize exp2 if not col
                if (exp2 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp2.getName());
                    row.add(exp2.getOp());
                    row.add(exp2.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getRHS());
                    row.add(exp2.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_YEARS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_YEARS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                    }
                }
                else
                {
                    String colString2 = "";
                    if (col2.getTable() != null)
                    {
                        colString2 = col2.getTable();
                    }
                    colString2 += ("." + col2.getColumn());
                    if (name != null)
                    {
                        String sgetName = name;
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString2, TYPE_YEARS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                    }
                    else
                    {
                        name = "._E" + model.getAndIncrementSuffix();
                        return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString2, TYPE_YEARS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, -1);
                    }
                }
            }
            else if (exp2 != null && exp2.getType() == TYPE_DAYS)
            {
                // already handled DATE +/- DAYS and DAYS +/- DAYS
                // so need to handle col(DATE) +/- DAYS
                // externalize exp1 if not col

                if (exp1 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp1.getName());
                    row.add(exp1.getOp());
                    row.add(exp1.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getLHS());
                    row.add(exp1.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (exp.getOp().equals("+"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                        }
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                        }
                    }
                    else
                    {
                        throw new ParseException("Only the + and - operators are allowed with type DAYS");
                    }
                }
                else
                {
                    String colString1 = "";
                    if (col1.getTable() != null)
                    {
                        colString1 = col1.getTable();
                    }
                    colString1 += ("." + col1.getColumn());
                    if (exp.getOp().equals("+"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else
                    {
                        throw new ParseException("Only the + and - operators are allowed with type DAYS");
                    }
                }
            }
            else if (exp2 != null && exp2.getType() == TYPE_MONTHS)
            {
                if (exp1 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp1.getName());
                    row.add(exp1.getOp());
                    row.add(exp1.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getLHS());
                    row.add(exp1.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (exp.getOp().equals("+"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                        }
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                        }
                    }
                    else
                    {
                        throw new ParseException("Only the + and - operators are allowed with type MONTHS");
                    }
                }
                else
                {
                    String colString1 = "";
                    if (col1.getTable() != null)
                    {
                        colString1 = col1.getTable();
                    }
                    colString1 += ("." + col1.getColumn());
                    if (exp.getOp().equals("+"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else
                    {
                        throw new ParseException("Only the + and - operators are allowed with type MONTHS");
                    }
                }
            }
            else if (exp2 != null && exp2.getType() == TYPE_YEARS)
            {
                if (exp1 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp1.getName());
                    row.add(exp1.getOp());
                    row.add(exp1.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getLHS());
                    row.add(exp1.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);

                    if (exp.getOp().equals("+"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                        }
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
                        }
                    }
                    else
                    {
                        throw new ParseException("Only the + and - operators are allowed with type YEARS");
                    }
                }
                else
                {
                    String colString1 = "";
                    if (col1.getTable() != null)
                    {
                        colString1 = col1.getTable();
                    }
                    colString1 += ("." + col1.getColumn());
                    if (exp.getOp().equals("+"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else if (exp.getOp().equals("-"))
                    {
                        if (name != null)
                        {
                            String sgetName = name;
                            if (!sgetName.contains("."))
                            {
                                sgetName = "." + sgetName;
                            }
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
                        }
                        else
                        {
                            name = "._E" + model.getAndIncrementSuffix();
                            return new SQLParser.OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
                        }
                    }
                    else
                    {
                        throw new ParseException("Only the + and - operators are allowed with type YEARS");
                    }
                }
            }

            // neither exp1 or exp2 are of type DATE or DAYS or MONTHS or YEARS
            if (exp.getOp().equals("||"))
            {
                // string concatenation
                // externalize both exp1 and exp2 if they are not cols
                int prereq1 = -1;
                int prereq2 = -1;
                ArrayList<Object> row = null;
                if (exp1 != null)
                {
                    row = new ArrayList<Object>();
                    row.add(exp1.getName());
                    row.add(exp1.getOp());
                    row.add(exp1.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getLHS());
                    row.add(exp1.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    prereq1 = (Integer)row.get(3);
                }

                if (exp2 != null)
                {
                    row = new ArrayList<Object>();
                    row.add(exp2.getName());
                    row.add(exp2.getOp());
                    row.add(exp2.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getRHS());
                    row.add(exp2.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    prereq2 = (Integer)row.get(3);
                }

                if (prereq2 == -1)
                {
                    prereq2 = prereq1;
                }
                else if (prereq1 != -1)
                {
                    final ArrayList<Object> bottom = getBottomRow(row);
                    bottom.remove(5);
                    bottom.add(5, prereq1);
                }

                String name1 = null;
                String name2 = null;
                if (exp1 != null)
                {
                    name1 = exp1.getName();
                }
                else
                {
                    name1 = "";
                    if (col1.getTable() != null)
                    {
                        name1 = col1.getTable();
                    }
                    name1 += ("." + col1.getColumn());
                }

                if (exp2 != null)
                {
                    name2 = exp2.getName();
                }
                else
                {
                    name2 = "";
                    if (col2.getTable() != null)
                    {
                        name2 = col2.getTable();
                    }
                    name2 += ("." + col2.getColumn());
                }
                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    return new SQLParser.OperatorTypeAndName(new ConcatOperator(name1, name2, sgetName, meta), TYPE_INLINE, sgetName, prereq2);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    return new SQLParser.OperatorTypeAndName(new ConcatOperator(name1, name2, name, meta), TYPE_INLINE, name, prereq2);
                }
            }

            // +,-,*, or /
            if ((exp1 == null || !(exp1.getOp() instanceof ExtendOperator)) && (exp2 == null || !(exp2.getOp() instanceof ExtendOperator)))
            {
                // externalize both exp1 and exp2
                ArrayList<Object> row = new ArrayList<Object>();
                int prereq1 = -1;
                int prereq2 = -1;
                if (exp1 != null)
                {
                    row.add(exp1.getName());
                    row.add(exp1.getOp());
                    row.add(exp1.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getLHS());
                    row.add(exp1.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    prereq1 = (Integer)row.get(3);
                }

                if (exp2 != null)
                {
                    row = new ArrayList<Object>();
                    row.add(exp2.getName());
                    row.add(exp2.getOp());
                    row.add(exp2.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp.getRHS());
                    row.add(exp2.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    prereq2 = (Integer)row.get(3);
                }

                if (prereq2 == -1)
                {
                    prereq2 = prereq1;
                }
                else if (prereq1 != -1)
                {
                    final ArrayList<Object> bottom = getBottomRow(row);
                    bottom.remove(5);
                    bottom.add(5, prereq1);
                }

                String name1 = null;
                String name2 = null;
                if (exp1 != null)
                {
                    name1 = exp1.getName();
                }
                else
                {
                    name1 = "";
                    if (col1.getTable() != null)
                    {
                        name1 = col1.getTable();
                    }
                    name1 += ("." + col1.getColumn());
                }

                if (exp2 != null)
                {
                    name2 = exp2.getName();
                }
                else
                {
                    name2 = "";
                    if (col2.getTable() != null)
                    {
                        name2 = col2.getTable();
                    }
                    name2 += ("." + col2.getColumn());
                }
                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    return new SQLParser.OperatorTypeAndName(new ExtendOperator(exp.getOp() + "," + name1 + "," + name2, sgetName, meta), TYPE_INLINE, sgetName, prereq2);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    return new SQLParser.OperatorTypeAndName(new ExtendOperator(exp.getOp() + "," + name1 + "," + name2, name, meta), TYPE_INLINE, name, prereq2);
                }
            }
            else if (exp1 != null && exp1.getOp() instanceof ExtendOperator && exp2 != null && exp2.getOp() instanceof ExtendOperator)
            {
                ExtendOperator combined = null;
                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), sgetName, meta);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), name, meta);
                }

                int myPrereq = -1;
                if (exp1.getPrereq() != -1)
                {
                    myPrereq = exp1.getPrereq();
                }

                if (exp2.getPrereq() != -1 && myPrereq == -1)
                {
                    myPrereq = exp2.getPrereq();
                }
                else if (exp2.getPrereq() != -1 && myPrereq != -1)
                {
                    final ArrayList<Object> bottom = getBottomRow(getRow(myPrereq));
                    bottom.remove(5);
                    bottom.add(5, exp2.getPrereq());
                }

                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    return new SQLParser.OperatorTypeAndName(combined, TYPE_INLINE, sgetName, myPrereq);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    return new SQLParser.OperatorTypeAndName(combined, TYPE_INLINE, name, myPrereq);
                }
            }
            else if (exp1 != null && exp1.getOp() instanceof ExtendOperator)
            {
                ExtendOperator combined = null;
                String name2 = null;
                if (exp2 != null)
                {
                    name2 = exp2.getName();
                }
                else
                {
                    name2 = "";
                    if (col2.getTable() != null)
                    {
                        name2 = col2.getTable();
                    }

                    name2 += ("." + col2.getColumn());
                }
                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + name2, sgetName, meta);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + name2, name, meta);
                }

                // externalize exp2
                int prereq = -1;
                if (exp2 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp2.getName());
                    row.add(exp2.getOp());
                    row.add(exp2.getType());
                    prereq = model.getAndIncrementComplexId();
                    row.add(prereq);
                    row.add(exp.getRHS());
                    row.add(exp2.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    final ArrayList<Object> bottom = getBottomRow(row);
                    bottom.remove(5);
                    bottom.add(5, exp1.getPrereq());
                }

                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    return new SQLParser.OperatorTypeAndName(combined, TYPE_INLINE, sgetName, prereq);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    return new SQLParser.OperatorTypeAndName(combined, TYPE_INLINE, name, prereq);
                }
            }
            else
            {
                // exp2 instanceof ExtendOperator
                ExtendOperator combined = null;
                String name1 = null;
                if (exp1 != null)
                {
                    name1 = exp1.getName();
                }
                else
                {
                    name1 = "";
                    if (col1.getTable() != null)
                    {
                        name1 = col1.getTable();
                    }

                    name1 += ("." + col1.getColumn());
                }
                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    combined = new ExtendOperator(exp.getOp() + "," + name1 + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), sgetName, meta);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    combined = new ExtendOperator(exp.getOp() + "," + name1 + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), name, meta);
                }

                // externalize exp1
                int prereq = -1;
                if (exp1 != null)
                {
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(exp1.getName());
                    row.add(exp1.getOp());
                    row.add(exp1.getType());
                    prereq = model.getAndIncrementComplexId();
                    row.add(prereq);
                    row.add(exp.getLHS());
                    row.add(exp1.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    final ArrayList<Object> bottom = getBottomRow(row);
                    bottom.remove(5);
                    bottom.add(5, exp2.getPrereq());
                }

                if (name != null)
                {
                    String sgetName = name;
                    if (!sgetName.contains("."))
                    {
                        sgetName = "." + sgetName;
                    }
                    return new SQLParser.OperatorTypeAndName(combined, TYPE_INLINE, sgetName, prereq);
                }
                else
                {
                    name = "._E" + model.getAndIncrementSuffix();
                    return new SQLParser.OperatorTypeAndName(combined, TYPE_INLINE, name, prereq);
                }
            }
        }

        return null;
    }

    private ArrayList<Object> getBottomRow(ArrayList<Object> row)
    {
        while ((Integer)row.get(5) != -1)
        {
            row = getRow((Integer)row.get(5));
        }

        return row;
    }

    private ArrayList<Object> getRow(final int num)
    {
        for (final ArrayList<Object> row : model.getComplex())
        {
            if (((Integer)row.get(3)) == num)
            {
                return row;
            }
        }

        return null;
    }
}