package com.exascale.optimizer.parse;

import com.exascale.exceptions.ParseException;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.*;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.exascale.optimizer.parse.ParseUtils.*;
import static com.exascale.optimizer.parse.SQLParser.*;

/** Parsing search clauses and conditions from SQL into query plan operators */
public class SearchOperatorTrees extends AbstractParseController {
    private final OperatorTrees operatorTrees;
    private final ConnectWithJoins connectWithJoins;
    private final ExpressionOperatorTrees expressionOperatorTrees;

    /** Initialize with information that's needed to build a plan for any configuration of operators */
    public SearchOperatorTrees(ConnectionWorker connection, Transaction tx, MetaData meta, SQLParser.Model model) {
        super(connection, tx, meta, model);
        operatorTrees = new OperatorTrees(connection, tx, meta, model);
        connectWithJoins = new ConnectWithJoins(connection, tx, meta, model);
        expressionOperatorTrees = new ExpressionOperatorTrees(connection, tx, meta, model);
    }

    Operator buildOperatorTreeFromSearchCondition(final SearchCondition search, Operator op, final SubSelect sub) throws Exception
    {
        if (search.getConnected() != null && search.getConnected().size() > 0)
        {
            convertToCNF(search);
        }

        final SearchClause clause = search.getClause();

        if (search.getConnected() != null && search.getConnected().size() > 0 && search.getConnected().get(0).isAnd())
        {
            if (search.getClause().getPredicate() == null)
            {
                op = buildOperatorTreeFromSearchCondition(search.getClause().getSearch(), op, sub);
            }
            else
            {
                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                op = buildOperatorTreeFromSearchCondition(new SearchCondition(search.getClause(), cscs), op, sub);
            }

            for (final ConnectedSearchClause csc : search.getConnected())
            {
                if (csc.getSearch().getPredicate() == null)
                {
                    op = buildOperatorTreeFromSearchCondition(csc.getSearch().getSearch(), op, sub);
                }
                else
                {
                    final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                    op = buildOperatorTreeFromSearchCondition(new SearchCondition(csc.getSearch(), cscs), op, sub);
                }
            }

            return op;
        }

        if (search.getConnected() == null || search.getConnected().size() == 0)
        {
            final boolean negated = clause.getNegated();
            if (clause.getPredicate() != null)
            {
                final Predicate pred = clause.getPredicate();
                if (pred instanceof ExistsPredicate)
                {
                    if (isCorrelated(((ExistsPredicate)pred).getSelect()))
                    {
                        final SubSelect clone = ((ExistsPredicate)pred).getSelect().clone();
                        final SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
                        // select
                        // list
                        // and
                        // search
                        // conditions,
                        // and
                        // maybe
                        // group
                        // by
                        if (negated)
                        {
                            Operator op2 = operatorTrees.buildOperatorTreeFromSubSelect(clone, true);
                            op2 = addRename(op2, join);
                            return connectWithJoins.connectWithAntiJoin(op, op2, join);
                        }
                        else
                        {
                            Operator op2 = operatorTrees.buildOperatorTreeFromSubSelect(clone, true);
                            op2 = addRename(op2, join);
                            return connectWithJoins.connectWithSemiJoin(op, op2, join);
                        }
                    }
                    else
                    {
                        throw new ParseException("An EXISTS predicate must be correlated");
                    }
                }
                final Expression lhs = pred.getLHS();
                String o = pred.getOp();
                final Expression rhs = pred.getRHS();

                if (o.equals("IN") || o.equals("NI"))
                {
                    String lhsStr = null;
                    String rhsStr = null;
                    if (!rhs.isList())
                    {
                        StringBuilder lhsBuilder = new StringBuilder();
                        op = handleLhs(lhsBuilder, lhs, op, sub, meta);
                        if(lhsBuilder.length() != 0) {
                            lhsStr = lhsBuilder.toString();
                        }
                    }

                    if (rhs.isLiteral())
                    {
                        throw new ParseException("A literal value cannot be used in this context");
                    }
                    else if (rhs.isColumn())
                    {
                        throw new ParseException("A column cannot be used in this context");
                    }
                    else if (rhs.isCountStar())
                    {
                        throw new ParseException("Count(*) cannot be used in this context");
                    }
                    else if (rhs.isList())
                    {
                        if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
                        {
                            final List<Expression> list = rhs.getList();
                            final Predicate predicate = new Predicate(lhs, "E", list.get(0));
                            final SearchClause s1 = new SearchClause(predicate, false);
                            final List<ConnectedSearchClause> ss = new ArrayList<ConnectedSearchClause>();
                            int i = 1;
                            final int size = list.size();
                            while (i < size)
                            {
                                final Expression exp = list.get(i);
                                final Predicate p = new Predicate(lhs, "E", exp);
                                final SearchClause s = new SearchClause(p, false);
                                final ConnectedSearchClause cs = new ConnectedSearchClause(s, false);
                                ss.add(cs);
                                i++;
                            }

                            final SearchCondition sc = new SearchCondition(s1, ss);
                            return buildOperatorTreeFromSearchCondition(sc, op, sub);
                        }
                        else
                        {
                            final List<Expression> list = rhs.getList();
                            final Predicate predicate = new Predicate(lhs, "NE", list.get(0));
                            final SearchClause s1 = new SearchClause(predicate, false);
                            final List<ConnectedSearchClause> ss = new ArrayList<ConnectedSearchClause>();
                            int i = 1;
                            final int size = list.size();
                            while (i < size)
                            {
                                final Expression exp = list.get(i);
                                final Predicate p = new Predicate(lhs, "NE", exp);
                                final SearchClause s = new SearchClause(p, false);
                                final ConnectedSearchClause cs = new ConnectedSearchClause(s, true);
                                ss.add(cs);
                                i++;
                            }

                            final SearchCondition sc = new SearchCondition(s1, ss);
                            return buildOperatorTreeFromSearchCondition(sc, op, sub);
                        }
                    }
                    else if (rhs.isSelect())
                    {
                        final SubSelect sub2 = rhs.getSelect();
                        if (isCorrelated(sub2))
                        {
                            // rhsStr = getOneCol(sub);
                            final SubSelect clone = sub2.clone();
                            final SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
                            // select
                            // list
                            // and
                            // search
                            // conditions
                            if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
                            {
                                Operator rhs2 = operatorTrees.buildOperatorTreeFromSubSelect(clone, true);
                                rhs2 = addRename(rhs2, join);
                                rhsStr = rhs2.getPos2Col().get(0);
                                verifyTypes(lhsStr, op, rhsStr, rhs2);
                                try
                                {
                                    op = connectWithJoins.connectWithSemiJoin(op, rhs2, join, new Filter(lhsStr, "E", rhsStr));
                                }
                                catch (final Exception e)
                                {
                                    throw new ParseException(e.getMessage());
                                }
                            }
                            else
                            {
                                Operator rhs2 = operatorTrees.buildOperatorTreeFromSubSelect(clone, true);
                                rhs2 = addRename(rhs2, join);
                                rhsStr = rhs2.getPos2Col().get(0);
                                verifyTypes(lhsStr, op, rhsStr, rhs2);
                                try
                                {
                                    op = connectWithJoins.connectWithAntiJoin(op, rhs2, join, new Filter(lhsStr, "E", rhsStr));
                                }
                                catch (final Exception e)
                                {
                                    throw new ParseException(e.getMessage());
                                }
                            }

                            return op;
                        }
                        else
                        {
                            final Operator rhs2 = operatorTrees.buildOperatorTreeFromSubSelect(sub2, true);
                            rhsStr = rhs2.getPos2Col().get(0);
                            verifyTypes(lhsStr, op, rhsStr, rhs2);
                            if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
                            {
                                final List<String> cols = new ArrayList<String>();
                                cols.add(lhsStr);
                                try
                                {
                                    final SemiJoinOperator join = new SemiJoinOperator(cols, meta);
                                    join.add(op);
                                    join.add(rhs2);
                                    return join;
                                }
                                catch (final Exception e)
                                {
                                    throw new ParseException(e.getMessage());
                                }
                            }
                            else
                            {
                                final List<String> cols = new ArrayList<String>();
                                cols.add(lhsStr);
                                try
                                {
                                    final AntiJoinOperator join = new AntiJoinOperator(cols, meta);
                                    join.add(op);
                                    join.add(rhs2);
                                    return join;
                                }
                                catch (final Exception e)
                                {
                                    throw new ParseException(e.getMessage());
                                }
                            }
                        }
                    }
                    else
                    {
                        throw new ParseException("An expression cannot be used in this context");
                    }
                }

                String lhsStr = null;
                String rhsStr = null;

                StringBuilder lhsBuilder = new StringBuilder();
                op = handleLhs(lhsBuilder, lhs, op, sub, meta);
                if(lhsBuilder.length() != 0) {
                    lhsStr = lhsBuilder.toString();
                }

                // do the same for rhs
                StringBuilder rhsBuilder = new StringBuilder();
                op = handleRhs(rhsBuilder, rhs, op, sub, meta);
                if(rhsBuilder.length() != 0) {
                    rhsStr = rhsBuilder.toString();
                }

                // add SelectOperator to top of tree
                verifyTypes(lhsStr, o, rhsStr, op);
                if(negated) {
                    negate(o);
                }
                try
                {
                    final SelectOperator select = new SelectOperator(new Filter(lhsStr, o, rhsStr), meta);
                    select.add(op);
                    return select;
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
            }
            else
            {
                final SearchCondition s = clause.getSearch();
                if (!negated)
                {
                    return buildOperatorTreeFromSearchCondition(s, op, sub);
                }
                else
                {
                    negateSearchCondition(s);
                    return buildOperatorTreeFromSearchCondition(s, op, sub);
                }
            }
        }
        else
        {
            // ored
            final List<Filter> ors = new ArrayList<Filter>();
            final List<SearchClause> preds = new ArrayList<SearchClause>();
            preds.add(search.getClause());
            for (final ConnectedSearchClause csc : search.getConnected())
            {
                preds.add(csc.getSearch());
            }

            int i = 0;
            while (i < preds.size())
            {
                final SearchClause sc = preds.get(i);
                final boolean negated = sc.getNegated();
                final Predicate pred = sc.getPredicate();
                if (pred instanceof ExistsPredicate)
                {
                    throw new ParseException("Restriction: An EXISTS predicate cannot be used with a logical OR");
                }
                final Expression lhs = pred.getLHS();
                String o = pred.getOp();
                final Expression rhs = pred.getRHS();

                if (o.equals("IN") || o.equals("NI"))
                {
                    String lhsStr = null;
                    if (!rhs.isList())
                    {
                        if (lhs.isLiteral())
                        {
                            final Literal literal = lhs.getLiteral();
                            if (literal.isNull())
                            {
                                // TODO
                            }
                            final Object value = literal.getValue();
                            if (value instanceof String)
                            {
                                lhsStr = "'" + value.toString() + "'";
                            }
                            else
                            {
                                lhsStr = value.toString();
                            }
                        }
                        else if (lhs.isColumn())
                        {
                            final Column col = lhs.getColumn();
                            // is column unambiguous?
                            if (col.getTable() != null)
                            {
                                lhsStr = col.getTable() + "." + col.getColumn();
                                int matches = 0;
                                for (final String c : op.getPos2Col().values())
                                {
                                    if (c.equals(lhsStr))
                                    {
                                        matches++;
                                    }
                                }

                                if (matches == 0)
                                {
                                    throw new ParseException("Column " + lhsStr + " does not exist");
                                }
                                else if (matches > 1)
                                {
                                    throw new ParseException("Column " + lhsStr + " is ambiguous");
                                }
                            }
                            else
                            {
                                int matches = 0;
                                String table = null;
                                for (final String c : op.getPos2Col().values())
                                {
                                    final String p1 = c.substring(0, c.indexOf('.'));
                                    final String p2 = c.substring(c.indexOf('.') + 1);

                                    if (p2.equals(col.getColumn()))
                                    {
                                        matches++;
                                        table = p1;
                                    }
                                }

                                if (matches == 0)
                                {
                                    // could be a complex column
                                    for (final List<Object> row : model.getComplex())
                                    {
                                        // colName, op, type, id, exp, prereq,
                                        // done
                                        if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                                        {
                                            // its a match
                                            matches++;
                                        }
                                    }

                                    if (matches == 0)
                                    {
                                        throw new ParseException("Column " + col.getColumn() + " does not exist");
                                    }
                                    else if (matches > 1)
                                    {
                                        throw new ParseException("Column " + lhsStr + " is ambiguous");
                                    }

                                    for (final List<Object> row : model.getComplex())
                                    {
                                        // colName, op, type, id, exp, prereq,
                                        // done
                                        if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                                        {
                                            if (((Boolean)row.get(7)).equals(true))
                                            {
                                                throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
                                            }

                                            op = operatorTrees.addComplexColumn(row, op, sub);
                                        }
                                    }
                                }
                                else if (matches > 1)
                                {
                                    throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
                                }

                                lhsStr = table + "." + col.getColumn();
                            }
                        }
                        else if (lhs.isCountStar())
                        {
                            throw new ParseException("Count(*) cannot be used in this context");
                        }
                        else if (lhs.isList())
                        {
                            throw new ParseException("A list cannot be used in this context");
                        }
                        else if (lhs.isSelect())
                        {
                            throw new ParseException("Restriction: An IN predicate cannot contain a subselect if it is connected with a logical OR");
                        }
                        else
                        {
                            // check to see if complex already contains this
                            // expression
                            boolean found = false;
                            for (final List<Object> row : model.getComplex())
                            {
                                // colName, op, type, id, exp, prereq, sub, done
                                if (row.get(4).equals(lhs) && row.get(6).equals(sub))
                                {
                                    if ((Boolean)row.get(7) == true)
                                    {
                                        // found it
                                        found = true;
                                        lhsStr = (String)row.get(0);
                                        break;
                                    }
                                }
                            }

                            if (!found)
                            {
                                // if not, build it
                                final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(lhs, null, sub);
                                if (otan.getType() == TYPE_DAYS)
                                {
                                    throw new ParseException("A type of DAYS is not allowed for a predicate");
                                }

                                if (otan.getType() == TYPE_MONTHS)
                                {
                                    throw new ParseException("A type of MONTHS is not allowed for a predicate");
                                }

                                if (otan.getType() == TYPE_YEARS)
                                {
                                    throw new ParseException("A type of YEARS is not allowed for a predicate");
                                }

                                if (otan.getType() == TYPE_DATE)
                                {
                                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                    final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                                    lhsStr = "DATE('" + dateString + "')";
                                    // TODO - is this a bug... this code has no effect.
                                }
                                else
                                {
                                    // colName, op, type, id, exp, prereq, sub,
                                    // done
                                    final List<Object> row = new ArrayList<Object>();
                                    row.add(otan.getName());
                                    row.add(otan.getOp());
                                    row.add(otan.getType());
                                    row.add(model.getAndIncrementComplexId());
                                    row.add(lhs);
                                    row.add(otan.getPrereq());
                                    row.add(sub);
                                    row.add(false);
                                    model.getComplex().add(row);
                                    op = operatorTrees.addComplexColumn(row, op, sub);
                                    lhsStr = otan.getName();
                                }
                            }
                        }
                    }

                    if (rhs.isLiteral())
                    {
                        throw new ParseException("A literal value cannot be used in this context");
                    }
                    else if (rhs.isColumn())
                    {
                        throw new ParseException("A column cannot be used in this context");
                    }
                    else if (rhs.isCountStar())
                    {
                        throw new ParseException("Count(*) cannot be used in this context");
                    }
                    else if (rhs.isList())
                    {
                        if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
                        {
                            final List<Expression> list = rhs.getList();
                            final Predicate predicate = new Predicate(lhs, "E", list.get(0));
                            final SearchClause s1 = new SearchClause(predicate, false);
                            preds.add(s1);
                            // new ArrayList<ConnectedSearchClause>();
                            int j = 1;
                            final int size = list.size();
                            while (j < size)
                            {
                                final Expression exp = list.get(j);
                                final Predicate p = new Predicate(lhs, "E", exp);
                                final SearchClause s = new SearchClause(p, false);
                                preds.add(s);
                                j++;
                            }

                            i++;
                            continue;
                        }
                        else
                        {
                            final List<Expression> list = rhs.getList();
                            final Predicate predicate = new Predicate(lhs, "NE", list.get(0));
                            final SearchClause s1 = new SearchClause(predicate, false);
                            preds.add(s1);
                            int j = 1;
                            final int size = list.size();
                            while (j < size)
                            {
                                final Expression exp = list.get(j);
                                final Predicate p = new Predicate(lhs, "NE", exp);
                                final SearchClause s = new SearchClause(p, false);
                                preds.add(s);
                                j++;
                            }

                            i++;
                            continue;
                        }
                    }
                    else if (rhs.isSelect())
                    {
                        throw new ParseException("Restriction: An IN predicate cannot contain a subselect if it participates in a logical OR");
                    }
                    else
                    {
                        throw new ParseException("An expression cannot be used in this context");
                    }
                }

                String lhsStr = null;
                String rhsStr = null;

                //hrs
                StringBuilder lhsBuilder = new StringBuilder();
                op = handleLhs(lhsBuilder, lhs, op, sub, meta);
                if(lhsBuilder.length() != 0) {
                    lhsStr = lhsBuilder.toString();
                }

                // do the same for rhs
                if (rhs.isLiteral())
                {
                    final Literal literal = rhs.getLiteral();
                    if (literal.isNull())
                    {
                        // TODO
                    }
                    final Object value = literal.getValue();
                    if (value instanceof String)
                    {
                        rhsStr = "'" + value.toString() + "'";
                    }
                    else
                    {
                        rhsStr = value.toString();
                    }
                }
                else if (rhs.isColumn())
                {
                    final Column col = rhs.getColumn();
                    // is column unambiguous?
                    if (col.getTable() != null)
                    {
                        rhsStr = col.getTable() + "." + col.getColumn();
                        int matches = 0;
                        for (final String c : op.getPos2Col().values())
                        {
                            if (c.equals(rhsStr))
                            {
                                matches++;
                            }
                        }

                        if (matches == 0)
                        {
                            throw new ParseException("Column " + rhsStr + " does not exist");
                        }
                        else if (matches > 1)
                        {
                            throw new ParseException("Column " + rhsStr + " is ambiguous");
                        }
                    }
                    else
                    {
                        int matches = 0;
                        String table = null;
                        for (final String c : op.getPos2Col().values())
                        {
                            final String p1 = c.substring(0, c.indexOf('.'));
                            final String p2 = c.substring(c.indexOf('.') + 1);

                            if (p2.equals(col.getColumn()))
                            {
                                matches++;
                                table = p1;
                            }
                        }

                        if (matches == 0)
                        {
                            // could be a complex column
                            for (final List<Object> row : model.getComplex())
                            {
                                // colName, op, type, id, exp, prereq, done
                                if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                                {
                                    // its a match
                                    matches++;
                                }
                            }

                            if (matches == 0)
                            {
                                throw new ParseException("Column " + col.getColumn() + " does not exist");
                            }
                            else if (matches > 1)
                            {
                                throw new ParseException("Column " + lhsStr + " is ambiguous");
                            }

                            for (final List<Object> row : model.getComplex())
                            {
                                // colName, op, type, id, exp, prereq, done
                                if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                                {
                                    if (((Boolean)row.get(7)).equals(true))
                                    {
                                        throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
                                    }

                                    op = operatorTrees.addComplexColumn(row, op, sub);
                                }
                            }
                        }
                        else if (matches > 1)
                        {
                            throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
                        }

                        rhsStr = table + "." + col.getColumn();
                    }
                }
                else if (rhs.isCountStar())
                {
                    throw new ParseException("Count(*) cannot be used in this context");
                }
                else if (rhs.isList())
                {
                    throw new ParseException("A list cannot be used in this context");
                }
                else if (rhs.isSelect())
                {
                    final SubSelect sub2 = rhs.getSelect();
                    if (isCorrelated(sub2))
                    {
                        throw new ParseException("Restriction: A correlated subquery cannot be used in a predicate that is part of a logical OR");
                    }
                    else
                    {
                        final ProductOperator extend = new ProductOperator(meta);
                        if (!ensuresOnlyOneRow(sub2))
                        {
                            throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
                        }
                        final Operator rhs2 = operatorTrees.buildOperatorTreeFromSubSelect(sub2, true);
                        if (rhs2.getCols2Pos().size() != 1)
                        {
                            throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
                        }
                        try
                        {
                            extend.add(op);
                            extend.add(rhs2);
                        }
                        catch (final Exception e)
                        {
                            throw new ParseException(e.getMessage());
                        }
                        op = extend;
                        rhsStr = rhs2.getPos2Col().get(0);
                    }
                }
                else
                {
                    // check to see if complex already contains this expression
                    boolean found = false;
                    for (final List<Object> row : model.getComplex())
                    {
                        // colName, op, type, id, exp, prereq, sub, done
                        if (row.get(4).equals(rhs) && row.get(6).equals(sub))
                        {
                            if ((Boolean)row.get(7) == true)
                            {
                                // found it
                                found = true;
                                rhsStr = (String)row.get(0);
                                break;
                            }
                        }
                    }

                    if (!found)
                    {
                        // if not, build it
                        final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(rhs, null, sub);
                        if (otan.getType() == TYPE_DAYS)
                        {
                            throw new ParseException("A type of DAYS is not allowed for a predicate");
                        }

                        if (otan.getType() == TYPE_MONTHS)
                        {
                            throw new ParseException("A type of MONTHS is not allowed for a predicate");
                        }

                        if (otan.getType() == TYPE_YEARS)
                        {
                            throw new ParseException("A type of YEARS is not allowed for a predicate");
                        }

                        if (otan.getType() == TYPE_DATE)
                        {
                            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                            // String name = "._E" + suffix++;
                            // ExtendObjectOperator operator = new
                            // ExtendObjectOperator(DateParser.parse(dateString),
                            // name, meta);
                            // ArrayList<Object> row = new ArrayList<Object>();
                            // row.add(name);
                            // row.add(operator);
                            // row.add(TYPE_INLINE);
                            // row.add(complexID++);
                            // row.add(rhs);
                            // row.add(-1);
                            // row.add(sub);
                            // row.add(false);
                            // complex.add(row);
                            // op = addComplexColumn(row, op);
                            rhsStr = "DATE('" + dateString + "')";
                        }
                        else
                        {
                            // colName, op, type, id, exp, prereq, sub, done
                            final List<Object> row = new ArrayList<Object>();
                            row.add(otan.getName());
                            row.add(otan.getOp());
                            row.add(otan.getType());
                            row.add(model.getAndIncrementComplexId());
                            row.add(rhs);
                            row.add(otan.getPrereq());
                            row.add(sub);
                            row.add(false);
                            model.getComplex().add(row);
                            op = operatorTrees.addComplexColumn(row, op, sub);
                            rhsStr = otan.getName();
                        }
                    }
                }

                // add SelectOperator to top of tree
                verifyTypes(lhsStr, o, rhsStr, op);
                if (negated)
                {
                    negate(o);
                }
                try
                {
                    ors.add(new Filter(lhsStr, o, rhsStr));
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }

                i++;
            }

            try
            {
                final SelectOperator select = new SelectOperator(ors, meta);
                select.add(op);
                return select;
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }
    }

    static boolean isAllEquals(final SearchCondition join)
    {
        if (!"E".equals(join.getClause().getPredicate().getOp()))
        {
            return false;
        }

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : join.getConnected())
            {
                if (!"E".equals(csc.getSearch().getPredicate().getOp()))
                {
                    return false;
                }
            }
        }

        return true;
    }

    static boolean isCNF(final SearchCondition s)
    {
        final SearchClause sc = s.getClause();
        if (sc.getPredicate() != null && (s.getConnected() == null || s.getConnected().size() == 0))
        {
            // single predicate
            return true;
        }

        if (allOredPreds(s))
        {
            return true;
        }

        if (sc.getPredicate() == null)
        {
            if (sc.getSearch().getClause().getPredicate() == null)
            {
                return false;
            }

            if (sc.getSearch().getConnected() != null && sc.getSearch().getConnected().size() > 0)
            {
                for (final ConnectedSearchClause csc : sc.getSearch().getConnected())
                {
                    if (csc.isAnd())
                    {
                        return false;
                    }

                    if (csc.getSearch().getPredicate() == null)
                    {
                        return false;
                    }
                }
            }
        }

        if (s.getConnected() != null && s.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : s.getConnected())
            {
                if (!csc.isAnd())
                {
                    return false;
                }

                if (csc.getSearch().getPredicate() == null)
                {
                    if (csc.getSearch().getSearch().getClause().getPredicate() == null)
                    {
                        return false;
                    }

                    if (csc.getSearch().getSearch().getConnected() != null && csc.getSearch().getSearch().getConnected().size() > 0)
                    {
                        for (final ConnectedSearchClause csc2 : csc.getSearch().getSearch().getConnected())
                        {
                            if (csc2.isAnd())
                            {
                                return false;
                            }

                            if (csc2.getSearch().getPredicate() == null)
                            {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        return true;
    }

    private static SearchCondition removeCorrelatedSearchCondition(final SubSelect select, final Map<String, Integer> cols2Pos) throws ParseException
    {
        final SearchCondition search = select.getWhere().getSearch();
        convertToCNF(search);
        // for the clause and any connected
        final List<ConnectedSearchClause> searches = new ArrayList<ConnectedSearchClause>();
        final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
        searches.add(new ConnectedSearchClause(search.getClause(), true));
        if (search.getConnected() != null && search.getConnected().size() > 0)
        {
            searches.addAll(search.getConnected());
        }

        for (final ConnectedSearchClause csc : searches)
        {
            // if it's a predicate and contains a correlated column, add to
            // retval
            if (csc.getSearch().getPredicate() != null)
            {
                final Predicate p = csc.getSearch().getPredicate();
                if (p.getLHS() != null && containsCorrelatedCol(p.getLHS(), cols2Pos))
                {
                    if (p.getRHS() == null || !p.getRHS().isColumn())
                    {
                        throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
                    }

                    if (containsCorrelatedCol(p.getRHS(), cols2Pos))
                    {
                        throw new ParseException("A correlated predicate cannot have correlated columns on both sides");
                    }

                    cscs.add(csc);
                }
                else if (p.getRHS() != null && containsCorrelatedCol(p.getRHS(), cols2Pos))
                {
                    if (p.getLHS() == null || !p.getLHS().isColumn())
                    {
                        throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
                    }

                    String op = p.getOp();
                    if (op.equals("E"))
                    {
                    }
                    else if (op.equals("NE"))
                    {
                    }
                    else if (op.equals("G"))
                    {
                        op = "L";
                    }
                    else if (op.equals("GE"))
                    {
                        op = "LE";
                    }
                    else if (op.equals("L"))
                    {
                        op = "G";
                    }
                    else if (op.equals("LE"))
                    {
                        op = "GE";
                    }
                    else
                    {
                        throw new ParseException("Restriction: A correlated join condition in a correlated subquery cannot use the LIKE operator");
                    }

                    final Predicate p2 = new Predicate(p.getRHS(), op, p.getLHS());
                    csc.setSearch(new SearchClause(p2, csc.getSearch().getNegated()));
                    cscs.add(csc);
                }
            }
            else
            {
                // if it's a search condition, every predicate in the ored group
                // must contain a correlated col
                // add whole group to retval
                boolean correlated = false;
                final SearchCondition s = csc.getSearch().getSearch();
                Predicate p = s.getClause().getPredicate();
                if (p.getLHS() != null && containsCorrelatedCol(p.getLHS(), cols2Pos))
                {
                    correlated = true;
                }
                else if (p.getRHS() != null && containsCorrelatedCol(p.getRHS(), cols2Pos))
                {
                    correlated = true;
                }

                if (!correlated)
                {
                    for (final ConnectedSearchClause csc2 : s.getConnected())
                    {
                        p = csc2.getSearch().getPredicate();
                        if (p.getLHS() != null && containsCorrelatedCol(p.getLHS(), cols2Pos))
                        {
                            correlated = true;
                            break;
                        }
                        else if (p.getRHS() != null && containsCorrelatedCol(p.getRHS(), cols2Pos))
                        {
                            correlated = true;
                            break;
                        }
                    }
                }

                if (correlated)
                {
                    // Verify that every predicate is correlated
                    p = s.getClause().getPredicate();
                    if (p.getLHS() == null || p.getRHS() == null)
                    {
                        throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
                    }
                    if (!containsCorrelatedCol(p.getLHS(), cols2Pos) && !containsCorrelatedCol(p.getRHS(), cols2Pos))
                    {
                        throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
                    }

                    for (final ConnectedSearchClause csc2 : s.getConnected())
                    {
                        p = csc2.getSearch().getPredicate();
                        if (p.getLHS() == null || p.getRHS() == null)
                        {
                            throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
                        }
                        if (!containsCorrelatedCol(p.getLHS(), cols2Pos) && !containsCorrelatedCol(p.getRHS(), cols2Pos))
                        {
                            throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
                        }
                    }

                    final List<ConnectedSearchClause> cscs2 = new ArrayList<ConnectedSearchClause>();
                    p = s.getClause().getPredicate();
                    if (containsCorrelatedCol(p.getLHS(), cols2Pos))
                    {
                        if (!p.getRHS().isColumn())
                        {
                            throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
                        }

                        if (containsCorrelatedCol(p.getRHS(), cols2Pos))
                        {
                            throw new ParseException("A correlated predicate cannot have correlated columns on both sides");
                        }

                        cscs2.add(new ConnectedSearchClause(s.getClause(), false));
                    }
                    else
                    {
                        if (!p.getLHS().isColumn())
                        {
                            throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
                        }

                        String op = p.getOp();
                        if (op.equals("E"))
                        {
                        }
                        else if (op.equals("NE"))
                        {
                        }
                        else if (op.equals("G"))
                        {
                            op = "L";
                        }
                        else if (op.equals("GE"))
                        {
                            op = "LE";
                        }
                        else if (op.equals("L"))
                        {
                            op = "G";
                        }
                        else if (op.equals("LE"))
                        {
                            op = "GE";
                        }
                        else
                        {
                            throw new ParseException("Restriction: A correlated join condition in a correlated subquery cannot use the LIKE operator");
                        }

                        final Predicate p2 = new Predicate(p.getRHS(), op, p.getLHS());
                        cscs2.add(new ConnectedSearchClause(new SearchClause(p2, csc.getSearch().getNegated()), false));
                    }

                    for (final ConnectedSearchClause csc2 : s.getConnected())
                    {
                        p = csc2.getSearch().getPredicate();
                        if (containsCorrelatedCol(p.getLHS(), cols2Pos))
                        {
                            if (!p.getRHS().isColumn())
                            {
                                throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
                            }

                            if (containsCorrelatedCol(p.getRHS(), cols2Pos))
                            {
                                throw new ParseException("A correlated predicate cannot have correlated columns on both sides");
                            }

                            cscs2.add(csc2);
                        }
                        else
                        {
                            if (!p.getLHS().isColumn())
                            {
                                throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
                            }

                            String op = p.getOp();
                            if (op.equals("E"))
                            {
                            }
                            else if (op.equals("NE"))
                            {
                            }
                            else if (op.equals("G"))
                            {
                                op = "L";
                            }
                            else if (op.equals("GE"))
                            {
                                op = "LE";
                            }
                            else if (op.equals("L"))
                            {
                                op = "G";
                            }
                            else if (op.equals("LE"))
                            {
                                op = "GE";
                            }
                            else
                            {
                                throw new ParseException("Restriction: A correlated join condition in a correlated subquery cannot use the LIKE operator");
                            }

                            final Predicate p2 = new Predicate(p.getRHS(), op, p.getLHS());
                            cscs2.add(new ConnectedSearchClause(new SearchClause(p2, csc2.getSearch().getNegated()), false));
                        }
                    }

                    final SearchClause first = cscs2.remove(0).getSearch();
                    s.setClause(first);
                    s.setConnected(cscs2);
                    cscs.add(csc);
                }
            }
        }

        // update search and build retval
        for (final ConnectedSearchClause csc : cscs)
        {
            searches.remove(csc);
        }

        if (searches.size() > 0)
        {
            final SearchClause first = searches.remove(0).getSearch();
            search.setClause(first);
            search.setConnected(searches);
        }
        else
        {
            select.setWhere(null);
        }

        final SearchClause first = cscs.remove(0).getSearch();
        return new SearchCondition(first, cscs);
    }

    static void negateSearchCondition(final SearchCondition s)
    {
        if (s.getConnected() != null && s.getConnected().size() > 0)
        {
            s.getClause().setNegated(!s.getClause().getNegated());
            for (final ConnectedSearchClause clause : s.getConnected())
            {
                clause.setAnd(!clause.isAnd());
                clause.getSearch().setNegated(!clause.getSearch().getNegated());
            }

            return;
        }
        else
        {
            s.getClause().setNegated(!s.getClause().getNegated());
        }
    }

    private Operator handleLhs(StringBuilder lhsStr, Expression lhs, Operator op, final SubSelect sub, final MetaData meta) throws Exception {
        if (lhs.isLiteral())
        {
            final Literal literal = lhs.getLiteral();
            if (literal.isNull())
            {
                // TODO
            }
            final Object value = literal.getValue();
            if (value instanceof String)
            {
                lhsStr.append("'").append(value).append("'");
            }
            else
            {
                lhsStr.append(value);
            }
        }
        else if (lhs.isColumn())
        {
            final Column col = lhs.getColumn();
            // is column unambiguous?
            if (col.getTable() != null)
            {
                lhsStr.append(col.getTable()).append(".").append(col.getColumn());
                int matches = 0;
                for (final String c : op.getPos2Col().values())
                {
                    if (c.equals(lhsStr))
                    {
                        matches++;
                    }
                }

                if (matches == 0)
                {
                    throw new ParseException("Column " + lhsStr + " does not exist");
                }
                else if (matches > 1)
                {
                    throw new ParseException("Column " + lhsStr + " is ambiguous");
                }
            }
            else
            {
                int matches = 0;
                String table = null;
                for (final String c : op.getPos2Col().values())
                {
                    final String p1 = c.substring(0, c.indexOf('.'));
                    final String p2 = c.substring(c.indexOf('.') + 1);

                    if (p2.equals(col.getColumn()))
                    {
                        matches++;
                        table = p1;
                    }
                }

                if (matches == 0)
                {
                    // could be a complex column
                    for (final List<Object> row : model.getComplex())
                    {
                        // colName, op, type, id, exp, prereq,
                        // done
                        if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                        {
                            // its a match
                            matches++;
                        }
                    }

                    if (matches == 0)
                    {
                        throw new ParseException("Column " + col.getColumn() + " does not exist");
                    }
                    else if (matches > 1)
                    {
                        throw new ParseException("Column " + lhsStr + " is ambiguous");
                    }

                    for (final List<Object> row : model.getComplex())
                    {
                        // colName, op, type, id, exp, prereq,
                        // done
                        if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                        {
                            if (((Boolean)row.get(7)).equals(true))
                            {
                                throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
                            }

                            op = operatorTrees.addComplexColumn(row, op, sub);
                        }
                    }
                }
                else if (matches > 1)
                {
                    throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
                }

                lhsStr.append(table).append(".").append(col.getColumn());
            }
        }
        else if (lhs.isCountStar())
        {
            throw new ParseException("Count(*) cannot be used in this context");
        }
        else if (lhs.isList())
        {
            throw new ParseException("A list cannot be used in this context");
        }
        else if (lhs.isSelect())
        {
            final SubSelect sub2 = lhs.getSelect();
            if (isCorrelated(sub))
            {
                // lhsStr = getOneCol(sub);
                final SubSelect clone = sub2.clone();
                final SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
                // select
                // list
                // and
                // search
                // conditions
                final ProductOperator product = new ProductOperator(meta);
                try
                {
                    product.add(op);
                    Operator op2 = operatorTrees.buildOperatorTreeFromSubSelect(clone, true);
                    op2 = addRename(op2, join);
                    lhsStr.append(op2.getPos2Col().get(0));
                    product.add(op2);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                op = buildOperatorTreeFromSearchCondition(join, product, sub);
            }
            else
            {
                final ProductOperator extend = new ProductOperator(meta);
                if (!ensuresOnlyOneRow(sub))
                {
                    throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
                }
                final Operator rhs2 = operatorTrees.buildOperatorTreeFromSubSelect(sub, true);
                if (rhs2.getCols2Pos().size() != 1)
                {
                    throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
                }
                try
                {
                    extend.add(op);
                    extend.add(rhs2);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                op = extend;
                lhsStr.append(rhs2.getPos2Col().get(0));
            }
        }
        else
        {
            // check to see if complex already contains this
            // expression
            boolean found = false;
            for (final List<Object> row : model.getComplex())
            {
                // colName, op, type, id, exp, prereq, sub, done
                if (row.get(4).equals(lhs) && row.get(6).equals(sub))
                {
                    if ((Boolean)row.get(7) == true)
                    {
                        // found it
                        found = true;
                        lhsStr.append(row.get(0));
                        break;
                    }
                }
            }

            if (!found)
            {
                // if not, build it
                final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(lhs, null, sub);
                if (otan.getType() == TYPE_DAYS)
                {
                    throw new ParseException("A type of DAYS is not allowed for a predicate");
                }

                if (otan.getType() == TYPE_MONTHS)
                {
                    throw new ParseException("A type of MONTHS is not allowed for a predicate");
                }

                if (otan.getType() == TYPE_YEARS)
                {
                    throw new ParseException("A type of YEARS is not allowed for a predicate");
                }

                if (otan.getType() == TYPE_DATE)
                {
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                    // String name = "._E" + suffix++;
                    // ExtendObjectOperator operator = new
                    // ExtendObjectOperator(DateParser.parse(dateString),
                    // name, meta);
                    // ArrayList<Object> row = new
                    // ArrayList<Object>();
                    // row.add(name);
                    // row.add(operator);
                    // row.add(TYPE_INLINE);
                    // row.add(complexID++);
                    // row.add(lhs);
                    // row.add(-1);
                    // row.add(sub);
                    // row.add(false);
                    // complex.add(row);
                    // op = addComplexColumn(row, op);
                    lhsStr.append("DATE('").append(dateString).append("')");
                }
                else
                {
                    // colName, op, type, id, exp, prereq, sub,
                    // done
                    final List<Object> row = new ArrayList<Object>();
                    row.add(otan.getName());
                    row.add(otan.getOp());
                    row.add(otan.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(lhs);
                    row.add(otan.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    op = operatorTrees.addComplexColumn(row, op, sub);
                    lhsStr.append(otan.getName());
                }
            }
        }
        return op;
    }

    private Operator handleRhs(StringBuilder rhsStr, Expression rhs, Operator op, final SubSelect sub, final MetaData meta) throws Exception {
        if (rhs.isLiteral())
        {
            final Literal literal = rhs.getLiteral();
            if (literal.isNull())
            {
                // TODO
            }
            final Object value = literal.getValue();
            if (value instanceof String)
            {
                rhsStr.append("'").append(value).append("'");
            }
            else
            {
                rhsStr.append(value);
            }
        }
        else if (rhs.isColumn())
        {
            final Column col = rhs.getColumn();
            // is column unambiguous?
            if (col.getTable() != null)
            {
                rhsStr.append(col.getTable()).append(".").append(col.getColumn());
                int matches = 0;
                for (final String c : op.getPos2Col().values())
                {
                    if (c.equals(rhsStr))
                    {
                        matches++;
                    }
                }

                if (matches == 0)
                {
                    throw new ParseException("Column " + rhsStr + " does not exist");
                }
                else if (matches > 1)
                {
                    throw new ParseException("Column " + rhsStr + " is ambiguous");
                }
            }
            else
            {
                int matches = 0;
                String table = null;
                for (final String c : op.getPos2Col().values())
                {
                    final String p1 = c.substring(0, c.indexOf('.'));
                    final String p2 = c.substring(c.indexOf('.') + 1);

                    if (p2.equals(col.getColumn()))
                    {
                        matches++;
                        table = p1;
                    }
                }

                if (matches == 0)
                {
                    // could be a complex column
                    for (final List<Object> row : model.getComplex())
                    {
                        // colName, op, type, id, exp, prereq, done
                        if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                        {
                            // its a match
                            matches++;
                        }
                    }

                    if (matches == 0)
                    {
                        throw new ParseException("Column " + col.getColumn() + " does not exist");
                    }
                    else if (matches > 1)
                    {
                        throw new ParseException("Column " + rhsStr + " is ambiguous");
                    }

                    for (final List<Object> row : model.getComplex())
                    {
                        // colName, op, type, id, exp, prereq, done
                        if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
                        {
                            if (((Boolean)row.get(7)).equals(true))
                            {
                                throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
                            }

                            op = operatorTrees.addComplexColumn(row, op, sub);
                        }
                    }
                }
                else if (matches > 1)
                {
                    throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
                }

                rhsStr.append(table).append(".").append(col.getColumn());
            }
        }
        else if (rhs.isCountStar())
        {
            throw new ParseException("Count(*) cannot be used in this context");
        }
        else if (rhs.isList())
        {
            throw new ParseException("A list cannot be used in this context");
        }
        else if (rhs.isSelect())
        {
            final SubSelect sub2 = rhs.getSelect();
            if (isCorrelated(sub2))
            {
                // rhsStr = getOneCol(sub);
                final SubSelect clone = sub2.clone();
                final SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
                // select
                // list
                // and
                // search
                // conditions
                final ProductOperator product = new ProductOperator(meta);
                try
                {
                    product.add(op);
                    Operator op2 = operatorTrees.buildOperatorTreeFromSubSelect(clone, true);
                    op2 = addRename(op2, join);
                    rhsStr.append(op2.getPos2Col().get(0));
                    product.add(op2);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                op = buildOperatorTreeFromSearchCondition(join, product, sub);
            }
            else
            {
                final ProductOperator extend = new ProductOperator(meta);
                if (!ensuresOnlyOneRow(sub2))
                {
                    throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
                }
                final Operator rhs2 = operatorTrees.buildOperatorTreeFromSubSelect(sub2, true);
                if (rhs2.getCols2Pos().size() != 1)
                {
                    throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
                }
                try
                {
                    extend.add(op);
                    extend.add(rhs2);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                op = extend;
                rhsStr.append(rhs2.getPos2Col().get(0));
            }
        }
        else
        {
            // check to see if complex already contains this expression
            boolean found = false;
            for (final List<Object> row : model.getComplex())
            {
                // colName, op, type, id, exp, prereq, sub, done
                if (row.get(4).equals(rhs) && row.get(6).equals(sub))
                {
                    if ((Boolean)row.get(7) == true)
                    {
                        // found it
                        found = true;
                        rhsStr.append(row.get(0));
                        break;
                    }
                }
            }

            if (!found)
            {
                // if not, build it
                final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(rhs, null, sub);
                if (otan.getType() == TYPE_DAYS)
                {
                    throw new ParseException("A type of DAYS is not allowed for a predicate");
                }

                if (otan.getType() == TYPE_MONTHS)
                {
                    throw new ParseException("A type of MONTHS is not allowed for a predicate");
                }

                if (otan.getType() == TYPE_YEARS)
                {
                    throw new ParseException("A type of YEARS is not allowed for a predicate");
                }

                if (otan.getType() == TYPE_DATE)
                {
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                    rhsStr.append("DATE('").append(dateString).append("')");
                }
                else
                {
                    // colName, op, type, id, exp, prereq, sub, done
                    final List<Object> row = new ArrayList<Object>();
                    row.add(otan.getName());
                    row.add(otan.getOp());
                    row.add(otan.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(rhs);
                    row.add(otan.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                    op = operatorTrees.addComplexColumn(row, op, sub);
                    rhsStr.append(otan.getName());
                }
            }
        }
        return op;
    }

    private static void convertToCNF(SearchCondition s)
    {
        tail: while (true)
        {
            //HRDBMSWorker.logger.debug("Entering convertToCNF search condition = " + s);
            SearchClause sc = s.getClause();
            if (sc.getPredicate() == null)
            {
                pushInwardAnyNegation(sc);
                //HRDBMSWorker.logger.debug("After negating first search clause, search condition = " + s);
            }

            if (s.getConnected() != null && s.getConnected().size() > 0)
            {
                for (final ConnectedSearchClause csc : s.getConnected())
                {
                    if (csc.getSearch().getPredicate() == null)
                    {
                        pushInwardAnyNegation(csc.getSearch());
                    }
                }
            }

            //HRDBMSWorker.logger.debug("After negating the rest, search condition = " + s);

            if (isCNF(s))
            {
                return;
            }

            if ((s.getConnected() == null || s.getConnected().size() == 0) && sc.getPredicate() == null)
            {
                final SearchCondition s2 = sc.getSearch();
                s.setClause(s2.getClause());
                s.setConnected(s2.getConnected());
                continue tail;
            }

            if (allPredicates(s))
            {
                // we must have mixed ands and ors
                ArrayList<SearchClause> preds = new ArrayList<SearchClause>();
                SearchClause next = null;
                ArrayList<ConnectedSearchClause> remainder = new ArrayList<ConnectedSearchClause>();
                preds.add(sc);
                boolean found = false;

                for (final ConnectedSearchClause csc : s.getConnected())
                {
                    if (found)
                    {
                        remainder.add(csc);
                        continue;
                    }

                    if (csc.isAnd())
                    {
                        preds.add(csc.getSearch());
                    }
                    else
                    {
                        found = true;
                        next = csc.getSearch();
                    }
                }

                ArrayList<SearchCondition> searches = new ArrayList<SearchCondition>();
                for (final SearchClause a : preds)
                {
                    final List<ConnectedSearchClause> b = new ArrayList<ConnectedSearchClause>(1);
                    final ConnectedSearchClause b1 = new ConnectedSearchClause(next, false);
                    b.add(b1);
                    final SearchCondition search = new SearchCondition(a, b);
                    searches.add(search);
                }

                final SearchCondition first = searches.remove(0);
                ArrayList<ConnectedSearchClause> b = new ArrayList<ConnectedSearchClause>();
                for (final SearchCondition s2 : searches)
                {
                    final SearchClause sc2 = new SearchClause(s2, false);
                    b.add(new ConnectedSearchClause(sc2, true));
                }

                SearchCondition s3 = new SearchCondition(new SearchClause(first, false), b);
                s.setClause(new SearchClause(s3, false));
                s.setConnected(remainder);
                preds = null;
                remainder = null;
                searches = null;
                b = null;
                s3 = null;
                continue tail;
            }

            if (s.getConnected().get(0).isAnd())
            {
                // starts with and
                if (sc.getPredicate() == null)
                {
                    convertToCNF(sc.getSearch());
                }

                if (s.getConnected().get(0).getSearch().getPredicate() == null)
                {
                    convertToCNF(s.getConnected().get(0).getSearch().getSearch());
                }

                if (sc.getPredicate() != null || (allPredicates(sc.getSearch()) && allAnd(sc.getSearch())))
                {
                    // A is all anded predicates
                    ArrayList<SearchClause> A = new ArrayList<SearchClause>();
                    if (sc.getPredicate() != null)
                    {
                        A.add(sc);
                    }
                    else
                    {
                        final SearchCondition s2 = sc.getSearch();
                        A.add(s2.getClause());
                        for (final ConnectedSearchClause csc : s2.getConnected())
                        {
                            A.add(csc.getSearch());
                        }
                    }

                    // figure out what B is
                    final SearchClause sc2 = s.getConnected().get(0).getSearch();
                    if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
                    {
                        // B is all anded predicates
                        if (sc2.getPredicate() != null)
                        {
                            A.add(sc2);
                        }
                        else
                        {
                            final SearchCondition s2 = sc2.getSearch();
                            A.add(s2.getClause());
                            for (final ConnectedSearchClause csc : s2.getConnected())
                            {
                                A.add(csc.getSearch());
                            }
                        }

                        s.getConnected().remove(0);
                        ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        final SearchClause first = A.remove(0);
                        for (final SearchClause search : A)
                        {
                            cscs.add(new ConnectedSearchClause(search, true));
                        }

                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        A = null;
                        cscs = null;
                        continue tail;
                    }
                    else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
                    {
                        // B all ors
                        s.getConnected().remove(0);
                        final SearchClause first = A.remove(0);
                        ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchClause search : A)
                        {
                            cscs.add(new ConnectedSearchClause(search, true));
                        }

                        cscs.add(new ConnectedSearchClause(sc2, true));
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        A = null;
                        cscs = null;
                        continue tail;
                    }
                    else
                    {
                        // A AND B CNF
                        s.getConnected().remove(0);
                        List<ConnectedSearchClause> cscs = sc2.getSearch().getConnected();
                        if (cscs == null)
                        {
                            cscs = new ArrayList<ConnectedSearchClause>();
                        }
                        for (final SearchClause search : A)
                        {
                            cscs.add(new ConnectedSearchClause(search, true));
                        }
                        sc2.getSearch().setConnected(cscs);
                        s.setClause(sc2);
                        A = null;
                        continue tail;
                    }
                }
                else if (allPredicates(sc.getSearch()) && allOrs(sc.getSearch()))
                {
                    // A all ored preds
                    // figure out what B is
                    final SearchClause sc2 = s.getConnected().get(0).getSearch();
                    if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
                    {
                        // B is all anded predicates
                        final SearchClause first = sc;
                        ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        if (sc2.getPredicate() != null)
                        {
                            s.getConnected().remove(0);
                            cscs.add(new ConnectedSearchClause(sc2, true));
                            s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                            cscs = null;
                            continue tail;
                        }

                        cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), true));
                        if (sc2.getSearch().getConnected() != null)
                        {
                            cscs.addAll(sc2.getSearch().getConnected());
                        }
                        s.getConnected().remove(0);
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        cscs = null;
                        continue tail;
                    }
                    else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
                    {
                        // B all ors
                        ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        final SearchClause first = sc;
                        cscs.add(new ConnectedSearchClause(sc2, true));
                        s.getConnected().remove(0);
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        cscs = null;
                        continue tail;
                    }
                    else
                    {
                        // A ors B CNF
                        s.getConnected().remove(0);
                        List<ConnectedSearchClause> cscs = sc2.getSearch().getConnected();
                        if (cscs == null)
                        {
                            cscs = new ArrayList<ConnectedSearchClause>();
                        }
                        cscs.add(new ConnectedSearchClause(sc, true));
                        sc2.getSearch().setConnected(cscs);
                        s.setClause(sc2);
                        cscs = null;
                        continue tail;
                    }
                }
                else
                {
                    // A is CNF
                    // figure out what B is
                    final SearchClause sc2 = s.getConnected().get(0).getSearch();
                    if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
                    {
                        // A CNF B AND
                        s.getConnected().remove(0);
                        List<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
                        if (cscs == null)
                        {
                            cscs = new ArrayList<ConnectedSearchClause>();
                        }

                        if (sc2.getPredicate() != null)
                        {
                            cscs.add(new ConnectedSearchClause(sc2, true));
                            sc.getSearch().setConnected(cscs);
                            cscs = null;
                            continue tail;
                        }

                        cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), true));
                        if (sc2.getSearch().getConnected() != null)
                        {
                            cscs.addAll(sc2.getSearch().getConnected());
                        }

                        sc.getSearch().setConnected(cscs);
                        cscs = null;
                        continue tail;
                    }
                    else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
                    {
                        // A CNF B OR
                        s.getConnected().remove(0);
                        List<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
                        if (cscs == null)
                        {
                            cscs = new ArrayList<ConnectedSearchClause>();
                        }

                        sc.getSearch().setConnected(cscs);
                        cscs.add(new ConnectedSearchClause(sc2, true));
                        cscs = null;
                        continue tail;
                    }
                    else
                    {
                        // A CNF B CNF
                        s.getConnected().remove(0);
                        List<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
                        if (cscs == null)
                        {
                            cscs = new ArrayList<ConnectedSearchClause>();
                            sc.getSearch().setConnected(cscs);
                        }

                        cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), true));
                        if (sc2.getSearch().getConnected() != null)
                        {
                            cscs.addAll(sc2.getSearch().getConnected());
                        }

                        cscs = null;
                        continue tail;
                    }
                }
            }
            else
            {
                // starts with or
                if (sc.getPredicate() == null)
                {
                    convertToCNF(sc.getSearch());
                }

                if (s.getConnected().get(0).getSearch().getPredicate() == null)
                {
                    convertToCNF(s.getConnected().get(0).getSearch().getSearch());
                }

                if (sc.getPredicate() != null || (allPredicates(sc.getSearch()) && allAnd(sc.getSearch())))
                {
                    // A is all anded predicates
                    ArrayList<SearchClause> A = new ArrayList<SearchClause>();
                    if (sc.getPredicate() != null)
                    {
                        A.add(sc);
                    }
                    else
                    {
                        final SearchCondition s2 = sc.getSearch();
                        A.add(s2.getClause());
                        for (final ConnectedSearchClause csc : s2.getConnected())
                        {
                            A.add(csc.getSearch());
                        }
                    }

                    // figure out what B is
                    final SearchClause sc2 = s.getConnected().get(0).getSearch();
                    if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
                    {
                        // B is all anded predicates
                        ArrayList<SearchClause> B = new ArrayList<SearchClause>();
                        if (sc2.getPredicate() != null)
                        {
                            B.add(sc2);
                        }
                        else
                        {
                            final SearchCondition s2 = sc2.getSearch();
                            B.add(s2.getClause());
                            for (final ConnectedSearchClause csc : s2.getConnected())
                            {
                                B.add(csc.getSearch());
                            }
                        }

                        List<SearchClause> searches = new ArrayList<SearchClause>();
                        for (final SearchClause p1 : A)
                        {
                            for (final SearchClause p2 : B)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                cscs.add(new ConnectedSearchClause(p2, false));
                                searches.add(new SearchClause(new SearchCondition(p1, cscs), false));
                            }
                        }

                        final SearchClause first = searches.remove(0);
                        ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchClause search : searches)
                        {
                            cscs.add(new ConnectedSearchClause(search, true));
                        }
                        s.getConnected().remove(0);
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        A = null;
                        B = null;
                        searches = null;
                        cscs = null;
                        continue tail;
                    }
                    else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
                    {
                        // B all ors
                        List<SearchClause> searches = new ArrayList<SearchClause>();
                        for (final SearchClause a : A)
                        {
                            final SearchClause search = sc2.clone();
                            search.getSearch().getConnected().add(new ConnectedSearchClause(a, false));
                            searches.add(search);
                        }

                        final SearchClause first = searches.remove(0);
                        List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchClause search : searches)
                        {
                            cscs.add(new ConnectedSearchClause(search, true));
                        }
                        s.getConnected().remove(0);
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        A = null;
                        searches = null;
                        cscs = null;
                        continue tail;
                    }
                    else
                    {
                        // A AND B CNF
                        s.getConnected().remove(0);
                        // build list of ored clauses in B
                        final SearchCondition scond = sc2.getSearch();
                        List<SearchCondition> searches = new ArrayList<SearchCondition>();
                        if (scond.getClause().getPredicate() != null)
                        {
                            final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                            searches.add(new SearchCondition(scond.getClause(), cscs));
                        }
                        else
                        {
                            searches.add(scond.getClause().getSearch());
                        }

                        for (final ConnectedSearchClause csc : scond.getConnected())
                        {
                            if (csc.getSearch().getPredicate() != null)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                searches.add(new SearchCondition(csc.getSearch(), cscs));
                            }
                            else
                            {
                                searches.add(csc.getSearch().getSearch());
                            }
                        }

                        List<SearchCondition> AB = new ArrayList<SearchCondition>();
                        for (final SearchClause a : A)
                        {
                            for (final SearchCondition b : searches)
                            {
                                final SearchCondition ab = b.clone();
                                ab.getConnected().add(new ConnectedSearchClause(a, false));
                                AB.add(ab);
                            }
                        }

                        SearchClause first = new SearchClause(AB.remove(0), false);
                        List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchCondition ab : AB)
                        {
                            cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
                        }

                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        A = null;
                        searches = null;
                        AB = null;
                        first = null;
                        cscs = null;
                        continue tail;
                    }
                }
                else if (allPredicates(sc.getSearch()) && allOrs(sc.getSearch()))
                {
                    // A all ored preds
                    // figure out what B is
                    final SearchClause sc2 = s.getConnected().get(0).getSearch();
                    if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
                    {
                        // B is all anded predicates
                        List<SearchClause> B = new ArrayList<SearchClause>();
                        if (sc2.getPredicate() != null)
                        {
                            B.add(sc2);
                        }
                        else
                        {
                            final SearchCondition s2 = sc2.getSearch();
                            B.add(s2.getClause());
                            for (final ConnectedSearchClause csc : s2.getConnected())
                            {
                                B.add(csc.getSearch());
                            }
                        }

                        List<SearchClause> searches = new ArrayList<SearchClause>();
                        for (final SearchClause b : B)
                        {
                            final SearchClause search = sc.clone();
                            search.getSearch().getConnected().add(new ConnectedSearchClause(b, false));
                            searches.add(search);
                        }

                        final SearchClause first = searches.remove(0);
                        List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchClause search : searches)
                        {
                            cscs.add(new ConnectedSearchClause(search, true));
                        }
                        s.getConnected().remove(0);
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        B = null;
                        searches = null;
                        cscs = null;
                        continue tail;
                    }
                    else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
                    {
                        // B all ors
                        s.getConnected().remove(0);
                        final List<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
                        cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), false));
                        cscs.addAll(sc2.getSearch().getConnected());
                        continue tail;
                    }
                    else
                    {
                        // A ors B CNF
                        s.getConnected().remove(0);
                        // build list of ored clauses in B
                        final SearchCondition scond = sc2.getSearch();
                        List<SearchCondition> searches = new ArrayList<SearchCondition>();
                        if (scond.getClause().getPredicate() != null)
                        {
                            final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                            searches.add(new SearchCondition(scond.getClause(), cscs));
                        }
                        else
                        {
                            searches.add(scond.getClause().getSearch());
                        }

                        for (final ConnectedSearchClause csc : scond.getConnected())
                        {
                            if (csc.getSearch().getPredicate() != null)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                searches.add(new SearchCondition(csc.getSearch(), cscs));
                            }
                            else
                            {
                                searches.add(csc.getSearch().getSearch());
                            }
                        }

                        for (final SearchCondition search : searches)
                        {
                            search.getConnected().add(new ConnectedSearchClause(sc.getSearch().getClause(), false));
                            for (final ConnectedSearchClause csc : sc.getSearch().getConnected())
                            {
                                search.getConnected().add(csc);
                            }
                        }

                        final SearchClause first = new SearchClause(searches.remove(0), false);
                        List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchCondition ab : searches)
                        {
                            cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
                        }

                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        searches = null;
                        cscs = null;
                        continue tail;
                    }
                }
                else
                {
                    // A is CNF
                    // figure out what B is
                    SearchClause sc2 = s.getConnected().get(0).getSearch();
                    if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
                    {
                        // A CNF B AND
                        List<SearchClause> B = new ArrayList<SearchClause>();
                        if (sc2.getPredicate() != null)
                        {
                            B.add(sc2);
                        }
                        else
                        {
                            final SearchCondition s2 = sc2.getSearch();
                            B.add(s2.getClause());
                            for (final ConnectedSearchClause csc : s2.getConnected())
                            {
                                B.add(csc.getSearch());
                            }
                        }

                        sc2 = null;
                        s.getConnected().remove(0);
                        s.setClause(null);
                        // build list of ored clauses in A
                        SearchCondition scond = sc.getSearch();
                        sc = null;
                        List<SearchCondition> searches = new ArrayList<SearchCondition>();
                        if (scond.getClause().getPredicate() != null)
                        {
                            final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                            searches.add(new SearchCondition(scond.getClause(), cscs));
                        }
                        else
                        {
                            searches.add(scond.getClause().getSearch());
                        }

                        for (final ConnectedSearchClause csc : scond.getConnected())
                        {
                            if (csc.getSearch().getPredicate() != null)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                searches.add(new SearchCondition(csc.getSearch(), cscs));
                            }
                            else
                            {
                                searches.add(csc.getSearch().getSearch());
                            }
                        }

                        scond = null;
                        List<SearchCondition> AB = new ArrayList<SearchCondition>();
                        HRDBMSWorker.logger.debug("Attempting to create " + (B.size() * 1l * searches.size()) + " new search conditions");
                        for (final SearchClause b : B)
                        {
                            for (final SearchCondition a : searches)
                            {
                                final SearchCondition ab = a.clone();
                                ab.getConnected().add(new ConnectedSearchClause(b, false));
                                AB.add(ab);
                            }
                        }

                        searches = null;
                        B = null;
                        SearchClause first = new SearchClause(AB.remove(0), false);
                        ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchCondition ab : AB)
                        {
                            cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
                        }

                        AB = null;
                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        cscs = null;
                        first = null;
                        continue tail;
                    }
                    else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
                    {
                        // A CNF B OR
                        s.getConnected().remove(0);
                        // build list of ored clauses in A
                        final SearchCondition scond = sc.getSearch();
                        List<SearchCondition> searches = new ArrayList<SearchCondition>();
                        if (scond.getClause().getPredicate() != null)
                        {
                            final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                            searches.add(new SearchCondition(scond.getClause(), cscs));
                        }
                        else
                        {
                            searches.add(scond.getClause().getSearch());
                        }

                        for (final ConnectedSearchClause csc : scond.getConnected())
                        {
                            if (csc.getSearch().getPredicate() != null)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                searches.add(new SearchCondition(csc.getSearch(), cscs));
                            }
                            else
                            {
                                searches.add(csc.getSearch().getSearch());
                            }
                        }

                        for (final SearchCondition search : searches)
                        {
                            search.getConnected().add(new ConnectedSearchClause(sc2.getSearch().getClause(), false));
                            for (final ConnectedSearchClause csc : sc2.getSearch().getConnected())
                            {
                                search.getConnected().add(csc);
                            }
                        }

                        SearchClause first = new SearchClause(searches.remove(0), false);
                        List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchCondition ab : searches)
                        {
                            cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
                        }

                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        searches = null;
                        first = null;
                        cscs = null;
                        continue tail;
                    }
                    else
                    {
                        // A CNF B CNF
                        SearchCondition scond = sc.getSearch();
                        List<SearchCondition> A = new ArrayList<SearchCondition>();
                        if (scond.getClause().getPredicate() != null)
                        {
                            final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                            A.add(new SearchCondition(scond.getClause(), cscs));
                        }
                        else
                        {
                            A.add(scond.getClause().getSearch());
                        }

                        for (final ConnectedSearchClause csc : scond.getConnected())
                        {
                            if (csc.getSearch().getPredicate() != null)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                A.add(new SearchCondition(csc.getSearch(), cscs));
                            }
                            else
                            {
                                A.add(csc.getSearch().getSearch());
                            }
                        }

                        scond = sc2.getSearch();
                        List<SearchCondition> B = new ArrayList<SearchCondition>();
                        if (scond.getClause().getPredicate() != null)
                        {
                            final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                            B.add(new SearchCondition(scond.getClause(), cscs));
                        }
                        else
                        {
                            B.add(scond.getClause().getSearch());
                        }

                        for (final ConnectedSearchClause csc : scond.getConnected())
                        {
                            if (csc.getSearch().getPredicate() != null)
                            {
                                final List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                                B.add(new SearchCondition(csc.getSearch(), cscs));
                            }
                            else
                            {
                                B.add(csc.getSearch().getSearch());
                            }
                        }

                        List<SearchCondition> AB = new ArrayList<SearchCondition>();
                        for (final SearchCondition a : A)
                        {
                            for (final SearchCondition b : B)
                            {
                                final SearchCondition ab = a.clone();
                                ab.getConnected().add(new ConnectedSearchClause(b.getClause(), false));
                                ab.getConnected().addAll(b.getConnected());
                                AB.add(ab);
                            }
                        }

                        SearchClause first = new SearchClause(AB.remove(0), false);
                        List<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
                        for (final SearchCondition ab : AB)
                        {
                            cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
                        }

                        s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
                        A = null;
                        B = null;
                        AB = null;
                        first = null;
                        cscs = null;
                        continue tail;
                    }
                }
            }
        }
    }


    private static void pushInwardAnyNegation(SearchClause sc)
    {
        //HRDBMSWorker.logger.debug("Upon entering pushInwardAnyNegation(), sc = " + sc);
        if (sc.getNegated())
        {
            negateSearchCondition(sc.getSearch());
            sc.setNegated(false);
        }

        //HRDBMSWorker.logger.debug("After negating search condition, sc = " + sc);

        final SearchCondition s = sc.getSearch();
        sc = s.getClause();
        if (sc.getPredicate() == null)
        {
            pushInwardAnyNegation(sc);
            //HRDBMSWorker.logger.debug("After calling pushInwardAnyNegation on first clause, sc = " + sc);
        }

        if (s.getConnected() != null && s.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause csc : s.getConnected())
            {
                if (csc.getSearch().getPredicate() == null)
                {
                    pushInwardAnyNegation(csc.getSearch());
                    //HRDBMSWorker.logger.debug("After calling pushInwardAnyNegation on subsequent clause, sc = " + sc);
                }
            }
        }
    }

    private SearchCondition rewriteCorrelatedSubSelect(final SubSelect select) throws Exception
    {
        // update select list and search conditions and group by
        final FromClause from = select.getFrom();
        final Operator temp = operatorTrees.buildOperatorTreeFromFrom(from, select);
        SearchCondition retval = removeCorrelatedSearchCondition(select, temp.getCols2Pos());
        updateSelectList(retval, select);
        updateGroupBy(retval, select);
        retval = retval.clone();
        return retval;
    }

    private boolean isCorrelated(final SubSelect select) throws Exception
    {
        try
        {
            operatorTrees.buildOperatorTreeFromSubSelect(select, true);
        }
        catch (final ParseException e)
        {
            final String msg = e.getMessage();
            if (msg.startsWith("Column ") && msg.endsWith(" does not exist"))
            {
                // delete any entries in complex with sub=select
                int i = 0;
                while (i < model.getComplex().size())
                {
                    final List<Object> row = model.getComplex().get(i);
                    if (select.equals(row.get(6)))
                    {
                        model.getComplex().remove(i);
                        continue;
                    }

                    i++;
                }
                return true;
            }

            HRDBMSWorker.logger.debug("", e);
            throw new ParseException(e.getMessage());
        }

        // delete any entries in complex with sub=select
        int i = 0;
        while (i < model.getComplex().size())
        {
            final List<Object> row = model.getComplex().get(i);
            if (select.equals(row.get(6)))
            {
                model.getComplex().remove(i);
                continue;
            }

            i++;
        }
        return false;
    }

    private Operator addRename(final Operator op, final SearchCondition join) throws Exception
    {
        final List<String> olds = new ArrayList<String>();
        final List<String> news = new ArrayList<String>();

        if (join.getClause().getPredicate() != null)
        {
            final Column col = join.getClause().getPredicate().getRHS().getColumn();
            String c = "";
            if (col.getTable() != null)
            {
                c += col.getTable();
            }

            c += ("." + col.getColumn());
            olds.add(getMatchingCol(op, c));
            String c2 = c.substring(0, c.indexOf('.') + 1);
            c2 += ("_" + model.getRewrites());
            c2 += c.substring(c.indexOf('.') + 1);
            col.setColumn("_" + model.getAndIncrementRewrites() + col.getColumn());
            news.add(c2);
        }
        else
        {
            final SearchCondition scond = join.getClause().getSearch();
            Column col = scond.getClause().getPredicate().getRHS().getColumn();
            String c = "";
            if (col.getTable() != null)
            {
                c += col.getTable();
            }

            c += ("." + col.getColumn());
            olds.add(getMatchingCol(op, c));
            String c2 = c.substring(0, c.indexOf('.') + 1);
            c2 += ("_" + model.getRewrites());
            c2 += c.substring(c.indexOf('.') + 1);
            col.setColumn("_" + model.getAndIncrementRewrites() + col.getColumn());
            news.add(c2);
            for (final ConnectedSearchClause csc : scond.getConnected())
            {
                col = csc.getSearch().getPredicate().getRHS().getColumn();
                c = "";
                if (col.getTable() != null)
                {
                    c += col.getTable();
                }

                c += ("." + col.getColumn());
                olds.add(getMatchingCol(op, c));
                c2 = c.substring(0, c.indexOf('.') + 1);
                c2 += ("_" + model.getRewrites());
                c2 += c.substring(c.indexOf('.') + 1);
                col.setColumn("_" + model.getAndIncrementRewrites() + col.getColumn());
                news.add(c2);
            }
        }

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause sc : join.getConnected())
            {
                if (sc.getSearch().getPredicate() != null)
                {
                    final Column col = sc.getSearch().getPredicate().getRHS().getColumn();
                    String c = "";
                    if (col.getTable() != null)
                    {
                        c += col.getTable();
                    }

                    c += ("." + col.getColumn());
                    olds.add(getMatchingCol(op, c));
                    String c2 = c.substring(0, c.indexOf('.') + 1);
                    c2 += ("_" + model.getRewrites());
                    c2 += c.substring(c.indexOf('.') + 1);
                    col.setColumn("_" + model.getAndIncrementRewrites() + col.getColumn());
                    news.add(c2);
                }
                else
                {
                    final SearchCondition scond = sc.getSearch().getSearch();
                    Column col = scond.getClause().getPredicate().getRHS().getColumn();
                    String c = "";
                    if (col.getTable() != null)
                    {
                        c += col.getTable();
                    }

                    c += ("." + col.getColumn());
                    olds.add(getMatchingCol(op, c));
                    String c2 = c.substring(0, c.indexOf('.') + 1);
                    c2 += ("_" + model.getRewrites());
                    c2 += c.substring(c.indexOf('.') + 1);
                    col.setColumn("_" + model.getAndIncrementRewrites() + col.getColumn());
                    news.add(c2);
                    for (final ConnectedSearchClause csc : scond.getConnected())
                    {
                        col = csc.getSearch().getPredicate().getRHS().getColumn();
                        c = "";
                        if (col.getTable() != null)
                        {
                            c += col.getTable();
                        }

                        c += ("." + col.getColumn());
                        olds.add(getMatchingCol(op, c));
                        c2 = c.substring(0, c.indexOf('.') + 1);
                        c2 += ("_" + model.getRewrites());
                        c2 += c.substring(c.indexOf('.') + 1);
                        col.setColumn("_" + model.getAndIncrementRewrites() + col.getColumn());
                        news.add(c2);
                    }
                }
            }
        }

        final Operator rename = new RenameOperator(olds, news, meta);
        rename.add(op);
        return rename;
    }

    private void updateGroupBy(final SearchCondition join, final SubSelect select) throws ParseException
    {
        if (updateGroupByNeeded(select))
        {
            if (!allPredicates(join) || !allAnd(join))
            {
                throw new ParseException("Restriction: A correlated subquery is not allowed if it involves aggregation and also contains correlated predicates that are not all anded together");
            }

            // verify all predicates are also equijoins
            if (!isAllEquals(join))
            {
                throw new ParseException("Restriction: Correlated subqueries with aggregation can only have equality operators on the correlated predicates");
            }

            // if group by already exists, add to it
            if (select.getGroupBy() != null)
            {
                final GroupBy groupBy = select.getGroupBy();
                groupBy.getCols().addAll(getRightColumns(join));
            }
            else
            {
                // otherwise create it
                select.setGroupBy(new GroupBy(getRightColumns(join)));
            }
        }
    }
}