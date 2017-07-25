package com.exascale.optimizer.parse;

import com.exascale.exceptions.ParseException;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.Utils;
import com.exascale.optimizer.*;

import java.util.*;

/** Utility methods with important logic for parsing SQL */
public class ParseUtils {
    static String negate(String o) {
        if (o.equals("E")) {
            o = "NE";
        } else if (o.equals("NE")) {
            o = "E";
        } else if (o.equals("G")) {
            o = "LE";
        } else if (o.equals("GE")) {
            o = "L";
        } else if (o.equals("L")) {
            o = "GE";
        } else if (o.equals("LE")) {
            o = "G";
        } else if (o.equals("LI")) {
            o = "NL";
        } else if (o.equals("NL")) {
            o = "LI";
        }
        return o;
    }

    static boolean allAnd(final SearchCondition s)
    {
        if (s.getConnected() != null && s.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : s.getConnected())
            {
                if (!csc.isAnd())
                {
                    return false;
                }
            }
        }

        return true;
    }

    static boolean allOredPreds(final SearchCondition s)
    {
        final SearchClause sc = s.getClause();
        boolean allOredPreds = true;
        if (sc.getPredicate() != null)
        {
            for (final ConnectedSearchClause csc : s.getConnected())
            {
                if (csc.isAnd())
                {
                    allOredPreds = false;
                    break;
                }

                if (csc.getSearch().getPredicate() == null)
                {
                    allOredPreds = false;
                    break;
                }
            }

            if (allOredPreds)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        return false;
    }

    static boolean allOrs(final SearchCondition s)
    {
        if (s.getConnected() != null && s.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : s.getConnected())
            {
                if (csc.isAnd())
                {
                    return false;
                }
            }
        }

        return true;
    }

    static boolean allPredicates(final SearchCondition s)
    {
        if (s.getClause().getPredicate() == null)
        {
            return false;
        }

        if (s.getConnected() != null && s.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : s.getConnected())
            {
                if (csc.getSearch().getPredicate() == null)
                {
                    return false;
                }
            }
        }

        return true;
    }

    static boolean allReferencesSatisfied(final List<String> ref, final Operator op)
    {
        final Set<String> set = op.getCols2Pos().keySet();
        for (String r : ref)
        {
            if (set.contains(r))
            {
                continue;
            }

            if (r.indexOf('.') > 0)
            {
                return false;
            }

            if (r.contains("."))
            {
                r = r.substring(1);
            }

            int matches = 0;
            for (String c : set)
            {
                if (c.contains("."))
                {
                    c = c.substring(c.indexOf('.') + 1);
                }

                if (c.equals(r))
                {
                    matches++;
                }
            }

            if (matches != 1)
            {
                return false;
            }
        }

        return true;
    }

    static void checkSizeOfNewCols(final List<Column> newCols, final Operator op) throws ParseException
    {
        if (newCols.size() != op.getPos2Col().size())
        {
            throw new ParseException("The common table expression has the wrong number of columns");
        }
    }

    static List<Column> getRightColumns(final SearchCondition join)
    {
        final List<Column> retval = new ArrayList<Column>();
        retval.add(join.getClause().getPredicate().getRHS().getColumn());

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : join.getConnected())
            {
                retval.add(csc.getSearch().getPredicate().getRHS().getColumn());
            }
        }

        return retval;
    }

    static String getType(String col, final Map<String, String> cols2Types) throws ParseException
    {
        String retval = cols2Types.get(col);

        if (retval != null)
        {
            return retval;
        }

        if (col.indexOf('.') > 0)
        {
            throw new ParseException("Column " + col + " not found");
        }

        if (col.startsWith("."))
        {
            col = col.substring(1);
        }

        int matches = 0;
        for (final Map.Entry entry : cols2Types.entrySet())
        {
            String name2 = (String)entry.getKey();
            if (name2.contains("."))
            {
                name2 = name2.substring(name2.indexOf('.') + 1);
            }

            if (col.equals(name2))
            {
                matches++;
                retval = (String)entry.getValue();
            }
        }

        if (matches == 0)
        {
            throw new ParseException("Column " + col + " not found");
        }

        if (matches > 1)
        {
            throw new ParseException("Column " + col + " is ambiguous");
        }

        return retval;
    }


    static void searchSingleTableForCTE(final String name, final List<Column> cols, final FullSelect cteSelect, final SingleTable table, final TableReference tref)
    {
        final TableName tblName = table.getName();
        if (tblName.getSchema() == null && tblName.getName().equals(name))
        {
            // found a match
            tref.removeSingleTable();
            if (cols.size() > 0)
            {
                cteSelect.addCols(cols);
            }
            tref.addSelect(cteSelect);
            if (table.getAlias() != null)
            {
                tref.setAlias(table.getAlias());
            }
        }
    }

    static void updateSelectList(final SearchCondition join, final SubSelect select)
    {
        final List<Column> needed = new ArrayList<Column>();
        if (join.getClause().getPredicate() != null)
        {
            needed.add(join.getClause().getPredicate().getRHS().getColumn());
        }
        else
        {
            final SearchCondition scond = join.getClause().getSearch();
            needed.add(scond.getClause().getPredicate().getRHS().getColumn());
            for (final ConnectedSearchClause csc : scond.getConnected())
            {
                needed.add(csc.getSearch().getPredicate().getRHS().getColumn());
            }
        }

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause sc : join.getConnected())
            {
                if (sc.getSearch().getPredicate() != null)
                {
                    needed.add(sc.getSearch().getPredicate().getRHS().getColumn());
                }
                else
                {
                    final SearchCondition scond = sc.getSearch().getSearch();
                    needed.add(scond.getClause().getPredicate().getRHS().getColumn());
                    for (final ConnectedSearchClause csc : scond.getConnected())
                    {
                        needed.add(csc.getSearch().getPredicate().getRHS().getColumn());
                    }
                }
            }
        }

        final SelectClause sclause = select.getSelect();
        if (sclause.isSelectStar())
        {
            return;
        }

        final List<SelectListEntry> list = sclause.getSelectList();
        for (final Column col : needed)
        {
            boolean found = false;
            for (final SelectListEntry entry : list)
            {
                // do we already have this col
                if (entry.getName() != null)
                {
                    continue;
                }

                if (!entry.isColumn())
                {
                    continue;
                }

                if (entry.getColumn().equals(col))
                {
                    found = true;
                }
            }

            if (!found)
            {
                list.add(new SelectListEntry(col, null));
            }
        }
    }

    static void verifyColumnsAreTheSame(final Operator lhs, final Operator rhs) throws ParseException
    {
        final Map<Integer, String> lhsPos2Col = lhs.getPos2Col();
        final Map<Integer, String> rhsPos2Col = rhs.getPos2Col();

        if (lhsPos2Col.size() != rhsPos2Col.size())
        {
            throw new ParseException("Cannot combine table with different number of columns");
        }

        int i = 0;
        for (final String col : lhsPos2Col.values())
        {
            if (!lhs.getCols2Types().get(col).equals(rhs.getCols2Types().get(rhsPos2Col.get(new Integer(i)))))
            {
                throw new ParseException("Column types do not match when combining tables");
            }

            i++;
        }
    }

    static void verifyTypes(String lhs, final Operator lOp, String rhs, final Operator rOp) throws ParseException
    {
        String lhsType = null;
        String rhsType = null;
        if (Character.isDigit(lhs.charAt(0)) || lhs.charAt(0) == '-')
        {
            lhsType = "NUMBER";
        }
        else if (lhs.charAt(0) == '\'')
        {
            lhsType = "STRING";
        }
        else
        {
            String type = lOp.getCols2Types().get(lhs);
            if (type == null)
            {
                if (lhs.contains("."))
                {
                    lhs = lhs.substring(lhs.indexOf(".") + 1);
                }
                else
                {
                }

                int matches = 0;
                for (final String col : lOp.getCols2Types().keySet())
                {
                    String col2;
                    if (col.contains("."))
                    {
                        col2 = col.substring(col.indexOf('.') + 1);
                    }
                    else
                    {
                        col2 = col;
                    }

                    if (col2.equals(lhs))
                    {
                        type = lOp.getCols2Types().get(col);
                        matches++;
                    }
                }

                if (matches != 1)
                {
                    if (matches == 0)
                    {
                        HRDBMSWorker.logger.debug("Cols2Types is " + lOp.getCols2Types());
                        throw new ParseException("Column " + lhs + " does not exist");
                    }
                    else
                    {
                        throw new ParseException("Column " + lhs + " is ambiguous");
                    }
                }
            }

            if (type.equals("CHAR"))
            {
                lhsType = "STRING";
            }
            else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
            {
                lhsType = "NUMBER";
            }
            else
            {
                lhsType = type;
            }
        }

        if (Character.isDigit(rhs.charAt(0)) || rhs.charAt(0) == '-')
        {
            rhsType = "NUMBER";
        }
        else if (rhs.charAt(0) == '\'')
        {
            rhsType = "STRING";
        }
        else
        {
            String type = rOp.getCols2Types().get(rhs);
            if (type == null)
            {
                if (rhs.contains("."))
                {
                    rhs = rhs.substring(rhs.indexOf(".") + 1);
                }
                else
                {
                }

                int matches = 0;
                for (final String col : rOp.getCols2Types().keySet())
                {
                    String col2;
                    if (col.contains("."))
                    {
                        col2 = col.substring(col.indexOf('.') + 1);
                    }
                    else
                    {
                        col2 = col;
                    }

                    if (col2.equals(rhs))
                    {
                        type = rOp.getCols2Types().get(col);
                        matches++;
                    }
                }

                if (matches != 1)
                {
                    if (matches == 0)
                    {
                        throw new ParseException("Column " + rhs + " does not exist");
                    }
                    else
                    {
                        throw new ParseException("Column " + rhs + " is ambiguous");
                    }
                }
            }

            if (type.equals("CHAR"))
            {
                rhsType = "STRING";
            }
            else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
            {
                rhsType = "NUMBER";
            }
            else
            {
                rhsType = type;
            }
        }

        if (lhsType.equals("NUMBER") && rhsType.equals("NUMBER"))
        {
            return;
        }
        else if (lhsType.equals("STRING") && rhsType.equals("STRING"))
        {
            return;
        }
        else
        {
            throw new ParseException("Invalid comparison between " + lhsType + " and " + rhsType);
        }
    }

    static void verifyTypes(final String lhs, final String op, final String rhs, final Operator o) throws ParseException
    {
        String lhsType = null;
        String rhsType = null;
        if (Character.isDigit(lhs.charAt(0)) || lhs.charAt(0) == '-')
        {
            lhsType = "NUMBER";
        }
        else if (lhs.charAt(0) == '\'')
        {
            lhsType = "STRING";
        }
        else if (lhs.startsWith("DATE('"))
        {
            lhsType = "DATE";
        }
        else
        {
            final String type = o.getCols2Types().get(lhs);
            if (type == null)
            {
                throw new ParseException("Column " + lhs + " does not exist");
            }
            else if (type.equals("CHAR"))
            {
                lhsType = "STRING";
            }
            else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
            {
                lhsType = "NUMBER";
            }
            else
            {
                lhsType = type;
            }
        }

        if (Character.isDigit(rhs.charAt(0)) || rhs.charAt(0) == '-')
        {
            rhsType = "NUMBER";
        }
        else if (rhs.charAt(0) == '\'')
        {
            rhsType = "STRING";
        }
        else if (rhs.startsWith("DATE('"))
        {
            rhsType = "DATE";
        }
        else
        {
            final String type = o.getCols2Types().get(rhs);
            if (type == null)
            {
                HRDBMSWorker.logger.debug("Looking for " + rhs + " in " + o.getCols2Types());
                Utils.printTree(o, 0);
                throw new ParseException("Column " + rhs + " does not exist");
            }
            else if (type.equals("CHAR"))
            {
                rhsType = "STRING";
            }
            else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
            {
                rhsType = "NUMBER";
            }
            else
            {
                rhsType = type;
            }
        }

        if (lhsType.equals("NUMBER") && rhsType.equals("NUMBER"))
        {
            if (op.equals("E") || op.equals("NE") || op.equals("G") || op.equals("GE") || op.equals("L") || op.equals("LE"))
            {
                return;
            }

            throw new ParseException("Invalid operator for comparing 2 numbers: " + op);
        }
        else if (lhsType.equals("STRING") && rhsType.equals("STRING"))
        {
            if (op.equals("LI") || op.equals("NL") || op.equals("E") || op.equals("NE") || op.equals("G") || op.equals("GE") || op.equals("L") || op.equals("LE"))
            {
                return;
            }

            throw new ParseException("Invalid operator for comparing 2 string: " + op);
        }
        else if (lhsType.equals("DATE") && rhsType.equals("DATE"))
        {
            if (op.equals("E") || op.equals("NE") || op.equals("G") || op.equals("GE") || op.equals("L") || op.equals("LE"))
            {
                return;
            }

            throw new ParseException("Invalid operator for comparing 2 dates: " + op);
        }
        else
        {
            throw new ParseException("Invalid comparison between " + lhsType + " and " + rhsType);
        }
    }

    static boolean containsCorrelatedCol(final Expression exp, final Map<String, Integer> cols2Pos) throws ParseException
    {
        if (exp.isColumn())
        {
            final Column col = exp.getColumn();
            String colString = "";
            if (col.getTable() != null)
            {
                colString += col.getTable();
            }
            colString += ("." + col.getColumn());
            if (cols2Pos.containsKey(colString))
            {
                return false;
            }
            else
            {
                if (colString.indexOf('.') != 0)
                {
                    return true;
                }
                else
                {
                    colString = colString.substring(1);
                }

                int matches = 0;
                for (String c : cols2Pos.keySet())
                {
                    if (c.contains("."))
                    {
                        c = c.substring(c.indexOf('.') + 1);
                    }

                    if (c.equals(colString))
                    {
                        matches++;
                    }
                }

                if (matches == 0)
                {
                    return true;
                }

                if (matches == 1)
                {
                    return false;
                }

                throw new ParseException("Ambiguous reference to " + colString);
            }
        }

        // if it contains a correlated column, it is a ParseException, otherwise
        // return false
        if (exp.isCountStar())
        {
            return false;
        }
        else if (exp.isExpression())
        {
            if (containsCorrelatedCol(exp.getLHS(), cols2Pos) || containsCorrelatedCol(exp.getRHS(), cols2Pos))
            {
                throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
            }
            else
            {
                return false;
            }
        }
        else if (exp.isFunction())
        {
            for (final Expression e : exp.getFunction().getArgs())
            {
                if (containsCorrelatedCol(e, cols2Pos))
                {
                    throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
                }
            }

            return false;
        }
        else if (exp.isCase())
        {
            for (final Case c : exp.getCases())
            {
                if (containsCorrelatedCol(c.getResult(), cols2Pos))
                {
                    throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
                }
            }

            if (containsCorrelatedCol(exp.getDefault(), cols2Pos))
            {
                throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
            }

            return false;
        }
        else if (exp.isList())
        {
            for (final Expression e : exp.getList())
            {
                if (containsCorrelatedCol(e, cols2Pos))
                {
                    throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
                }
            }

            return false;
        }
        else if (exp.isLiteral())
        {
            return false;
        }
        else if (exp.isSelect())
        {
            return false;
        }
        else
        {
            return false;
        }
    }


    static boolean containsAggregation(final Expression exp)
    {
        if (exp.isColumn())
        {
            return false;
        }
        else if (exp.isCountStar())
        {
            return true;
        }
        else if (exp.isExpression())
        {
            if (containsAggregation(exp.getLHS()))
            {
                return true;
            }

            if (containsAggregation(exp.getRHS()))
            {
                return true;
            }

            return false;
        }
        else if (exp.isFunction())
        {
            final Function f = exp.getFunction();
            if (f.getName().equals("AVG") || f.getName().equals("COUNT") || f.getName().equals("MAX") || f.getName().equals("MIN") || f.getName().equals("SUM"))
            {
                return true;
            }

            for (final Expression e : f.getArgs())
            {
                if (containsAggregation(e))
                {
                    return true;
                }
            }

            return false;
        }
        else if (exp.isList())
        {
            for (final Expression e : exp.getList())
            {
                if (containsAggregation(e))
                {
                    return true;
                }
            }

            return false;
        }
        else if (exp.isLiteral())
        {
            return false;
        }
        else if (exp.isSelect())
        {
            return false;
        }
        else
        {
            return false;
        }
    }

    static List<String> getReferences(final Expression exp)
    {
        final List<String> retval = new ArrayList<String>();
        if (exp.isCase())
        {
            for (final Case c : exp.getCases())
            {
                retval.addAll(getReferences(c.getCondition()));
                retval.addAll(getReferences(c.getResult()));
            }

            return retval;
        }
        else if (exp.isColumn())
        {
            String name = "";
            final Column c = exp.getColumn();
            if (c.getTable() != null)
            {
                name += c.getTable();
            }

            name += ("." + c.getColumn());
            retval.add(name);
            return retval;
        }
        else if (exp.isCountStar())
        {
            return retval;
        }
        else if (exp.isExpression())
        {
            retval.addAll(getReferences(exp.getLHS()));
            retval.addAll(getReferences(exp.getRHS()));
            return retval;
        }
        else if (exp.isFunction())
        {
            final Function f = exp.getFunction();
            for (final Expression e : f.getArgs())
            {
                retval.addAll(getReferences(e));
            }

            return retval;
        }
        else if (exp.isList())
        {
            final List<Expression> list = exp.getList();
            for (final Expression e : list)
            {
                retval.addAll(getReferences(e));
            }

            return retval;
        }
        else if (exp.isLiteral())
        {
            return retval;
        }
        else if (exp.isSelect())
        {
            retval.add("_________________________________");
            return retval;
        }

        return null;
    }

    static List<String> getReferences(final SearchClause sc)
    {
        if (sc.getSearch() != null)
        {
            return getReferences(sc.getSearch());
        }
        else
        {
            final Predicate p = sc.getPredicate();
            final List<String> retval = new ArrayList<String>();
            retval.addAll(getReferences(p.getLHS()));
            retval.addAll(getReferences(p.getRHS()));
            return retval;
        }
    }

    static List<String> getReferences(final SearchCondition sc)
    {
        final List<String> retval = new ArrayList<String>();
        retval.addAll(getReferences(sc.getClause()));
        if (sc.getConnected() != null && sc.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : sc.getConnected())
            {
                retval.addAll(getReferences(csc.getSearch()));
            }
        }

        return retval;
    }

    static boolean doesAggregation(final List<Expression> args)
    {
        for (final Expression exp : args)
        {
            if (doesAggregation(exp))
            {
                return true;
            }
        }

        return false;
    }

    static boolean doesAggregation(final Expression exp)
    {
        if (exp.isCountStar())
        {
            return true;
        }

        List<Expression> args = null;

        if (exp.isFunction())
        {
            final Function f = exp.getFunction();
            final String method = f.getName();
            if (method.equals("AVG") || method.equals("COUNT") || method.equals("MAX") || method.equals("MIN") || method.equals("SUM"))
            {
                return true;
            }

            args = f.getArgs();
        }
        else if (exp.isExpression())
        {
            args = new ArrayList<Expression>(2);
            args.add(exp.getLHS());
            args.add(exp.getRHS());
        }

        return doesAggregation(args);
    }

    static boolean ensuresOnlyOneRow(final SubSelect sub) throws Exception
    {
        if (sub.getFetchFirst() != null && sub.getFetchFirst().getNumber() == 1)
        {
            return true;
        }

        if (sub.getGroupBy() != null)
        {
            return false;
        }

        // if we find a column in the select list that uses a aggregation
        // function, return true
        // else return false
        final SelectClause select = sub.getSelect();
        final List<SelectListEntry> list = select.getSelectList();
        for (final SelectListEntry entry : list)
        {
            if (entry.isColumn())
            {
                return false;
            }

            final Expression exp = entry.getExpression();
            if (exp.isCountStar())
            {
                return true;
            }

            List<Expression> args = null;

            if (exp.isFunction())
            {
                final Function f = exp.getFunction();
                final String method = f.getName();
                if (method.equals("AVG") || method.equals("COUNT") || method.equals("MAX") || method.equals("MIN") || method.equals("SUM"))
                {
                    return true;
                }

                args = f.getArgs();
            }
            else if (exp.isExpression())
            {
                args = new ArrayList<Expression>(2);
                args.add(exp.getLHS());
                args.add(exp.getRHS());
            }
            else
            {
                throw new Exception("Unexpected expression type in ensureOnlyOneRow()");
            }

            if (doesAggregation(args)) // NULL args?
            {
                return true;
            }
        }

        return false;
    }


    static Operator handleAlias(final String alias, final Operator op, MetaData meta) throws ParseException
    {
        final List<String> original = new ArrayList<String>();
        final List<String> newCols = new ArrayList<String>();

        for (final String col : op.getPos2Col().values())
        {
            original.add(col);
            final String newCol = alias + "." + col.substring(col.indexOf('.') + 1);
            newCols.add(newCol);
        }

        try
        {
            final RenameOperator rename = new RenameOperator(original, newCols, meta);
            rename.add(op);
            return rename;
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }
    }

    static void searchFromForCTE(final String name, final List<Column> cols, final FullSelect cteSelect, final FromClause from)
    {
        for (final TableReference table : from.getTables())
        {
            searchTableRefForCTE(name, cols, cteSelect, table);
        }
    }

    static void searchFullSelectForCTE(final String name, final List<Column> cols, final FullSelect cteSelect, final FullSelect select)
    {
        if (select.getSubSelect() != null)
        {
            searchSubSelectForCTE(name, cols, cteSelect, select.getSubSelect());
        }
        else
        {
            searchFullSelectForCTE(name, cols, cteSelect, select.getFullSelect());
        }

        for (final ConnectedSelect cs : select.getConnected())
        {
            if (cs.getSub() != null)
            {
                searchSubSelectForCTE(name, cols, cteSelect, cs.getSub());
            }
            else
            {
                searchFullSelectForCTE(name, cols, cteSelect, cs.getFull());
            }
        }
    }

    static void searchSubSelectForCTE(final String name, final List<Column> cols, final FullSelect cteSelect, final SubSelect select)
    {
        searchFromForCTE(name, cols, cteSelect, select.getFrom());
    }

    static void searchTableRefForCTE(final String name, final List<Column> cols, final FullSelect cteSelect, final TableReference table)
    {
        if (table.isSingleTable())
        {
            searchSingleTableForCTE(name, cols, cteSelect, table.getSingleTable(), table);
        }
        else if (table.isSelect())
        {
            searchFullSelectForCTE(name, cols, cteSelect, table.getSelect());
        }
        else
        {
            searchTableRefForCTE(name, cols, cteSelect, table.getLHS());
            searchTableRefForCTE(name, cols, cteSelect, table.getRHS());
        }
    }


    static boolean updateGroupByNeeded(final SubSelect select)
    {
        if (select.getGroupBy() != null || select.getHaving() != null)
        {
            return true;
        }

        final SelectClause s = select.getSelect();
        final List<SelectListEntry> list = s.getSelectList();
        for (final SelectListEntry entry : list)
        {
            if (entry.isColumn())
            {
                continue;
            }

            final Expression exp = entry.getExpression();
            if (containsAggregation(exp))
            {
                return true;
            }
        }

        final Where where = select.getWhere();
        if (where != null)
        {
            final SearchCondition search = where.getSearch();
            if (containsAggregation(search.getClause()))
            {
                return true;
            }

            if (search.getConnected() != null && search.getConnected().size() > 0)
            {
                for (final ConnectedSearchClause csc : search.getConnected())
                {
                    if (containsAggregation(csc.getSearch()))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    static boolean containsAggregation(final SearchClause clause)
    {
        if (clause.getPredicate() != null)
        {
            final Predicate p = clause.getPredicate();
            if (p.getLHS() == null)
            {
                return false;
            }
            if (p.getRHS() == null)
            {
                return false;
            }
            if (containsAggregation(p.getLHS()))
            {
                return true;
            }

            if (containsAggregation(p.getRHS()))
            {
                return true;
            }

            return false;
        }

        final SearchCondition search = clause.getSearch();
        if (containsAggregation(search.getClause()))
        {
            return true;
        }

        if (search.getConnected() != null && search.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : search.getConnected())
            {
                if (containsAggregation(csc.getSearch()))
                {
                    return true;
                }
            }
        }

        return false;
    }

    static String getMatchingCol(final Operator op, String col)
    {
        if (op.getCols2Pos().keySet().contains(col))
        {
            return col;
        }
        else
        {
            if (col.contains("."))
            {
                col = col.substring(col.indexOf('.') + 1);
            }

            for (final String col2 : op.getCols2Pos().keySet())
            {
                String col3 = null;
                if (col2.contains("."))
                {
                    col3 = col2.substring(col2.indexOf('.') + 1);
                }
                else
                {
                    col3 = col2;
                }

                if (col.equals(col3))
                {
                    return col2;
                }
            }

            HRDBMSWorker.logger.debug("Could not find " + col + " in " + op.getCols2Pos().keySet());
            Utils.printTree(op, 0);
            //HRDBMSWorker.logger.debug(model.getComplex());
            return col;
        }
    }

    static void resolveConcat(Operator o, Operator op) throws ParseException {
        final String name1 = o.getReferences().get(0);
        final String type1 = op.getCols2Types().get(name1);
        final String name2 = o.getReferences().get(1);
        final String type2 = op.getCols2Types().get(name2);

        if (type1 == null)
        {
            throw new ParseException("A reference to column " + name1 + " was unable to be resolved.");
        }

        if (type2 == null)
        {
            throw new ParseException("A reference to column " + name2 + " was unable to be resolved.");
        }

        if (!type1.equals("CHAR") || !type2.equals("CHAR"))
        {
            throw new ParseException("Concatenation is not supported for non-character data");
        }
    }
}