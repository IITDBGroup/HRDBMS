package com.exascale.optimizer.parse;

import com.exascale.exceptions.ParseException;
import com.exascale.optimizer.*;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.exascale.optimizer.parse.ParseUtils.getMatchingCol;
import static com.exascale.optimizer.parse.ParseUtils.verifyTypes;

/** Parsing logic to connect operators by a join operator */
public class ConnectWithJoins extends AbstractParseController {
    /** Initialize with information that's needed to build a plan for any configuration of operators */
    public ConnectWithJoins(ConnectionWorker connection, Transaction tx, MetaData meta, SQLParser.Model model) {
        super(connection, tx, meta, model);
    }

    Operator connectWithAntiJoin(final Operator left, final Operator right, final SearchCondition join) throws ParseException
    {
        // assume join is already cnf
        // and contains only columns
        final Set<Map<Filter, Filter>> hshm = new HashSet<Map<Filter, Filter>>();
        HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
        if (join.getClause().getPredicate() != null)
        {
            final Column l = join.getClause().getPredicate().getLHS().getColumn();
            final Column r = join.getClause().getPredicate().getRHS().getColumn();
            String o = join.getClause().getPredicate().getOp();

            if (join.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }
        else
        {
            final SearchCondition scond = join.getClause().getSearch();
            Column l = scond.getClause().getPredicate().getLHS().getColumn();
            Column r = scond.getClause().getPredicate().getRHS().getColumn();
            String o = scond.getClause().getPredicate().getOp();

            if (scond.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }

            for (final ConnectedSearchClause csc : scond.getConnected())
            {
                l = csc.getSearch().getPredicate().getLHS().getColumn();
                r = csc.getSearch().getPredicate().getRHS().getColumn();
                o = csc.getSearch().getPredicate().getOp();

                if (csc.getSearch().getNegated())
                {
                    o = ParseUtils.negate(o);
                }

                lhs = "";
                if (l.getTable() != null)
                {
                    lhs += l.getTable();
                }

                lhs += ("." + l.getColumn());

                rhs = "";
                if (r.getTable() != null)
                {
                    rhs += r.getTable();
                }
                rhs += ("." + r.getColumn());
                lhs = getMatchingCol(left, lhs);
                rhs = getMatchingCol(right, rhs);
                try
                {
                    verifyTypes(lhs, left, rhs, right);
                    final Filter f = new Filter(lhs, o, rhs);
                    hm.put(f, f);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
            }
        }

        hshm.add(hm);

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause sc : join.getConnected())
            {
                hm = new HashMap<Filter, Filter>();
                if (sc.getSearch().getPredicate() != null)
                {
                    final Column l = sc.getSearch().getPredicate().getLHS().getColumn();
                    final Column r = sc.getSearch().getPredicate().getRHS().getColumn();
                    String o = sc.getSearch().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        if (e instanceof ParseException)
                        {
                            throw (ParseException)e;
                        }
                        else
                        {
                            throw new ParseException(e.getMessage());
                        }
                    }
                }
                else
                {
                    final SearchCondition scond = sc.getSearch().getSearch();
                    Column l = scond.getClause().getPredicate().getLHS().getColumn();
                    Column r = scond.getClause().getPredicate().getRHS().getColumn();
                    String o = scond.getClause().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }

                    for (final ConnectedSearchClause csc : scond.getConnected())
                    {
                        l = csc.getSearch().getPredicate().getLHS().getColumn();
                        r = csc.getSearch().getPredicate().getRHS().getColumn();
                        o = csc.getSearch().getPredicate().getOp();

                        if (csc.getSearch().getNegated())
                        {
                            o = ParseUtils.negate(o);
                        }

                        lhs = "";
                        if (l.getTable() != null)
                        {
                            lhs += l.getTable();
                        }

                        lhs += ("." + l.getColumn());

                        rhs = "";
                        if (r.getTable() != null)
                        {
                            rhs += r.getTable();
                        }
                        rhs += ("." + r.getColumn());
                        lhs = getMatchingCol(left, lhs);
                        rhs = getMatchingCol(right, rhs);
                        try
                        {
                            verifyTypes(lhs, left, rhs, right);
                            final Filter f = new Filter(lhs, o, rhs);
                            hm.put(f, f);
                        }
                        catch (final Exception e)
                        {
                            throw new ParseException(e.getMessage());
                        }
                    }
                }

                hshm.add(hm);
            }
        }

        try
        {
            final AntiJoinOperator anti = new AntiJoinOperator(hshm, meta);
            anti.add(left);
            anti.add(right);
            return anti;
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }
    }

    Operator connectWithAntiJoin(final Operator left, final Operator right, final SearchCondition join, final Filter filter) throws ParseException
    {
        // assume join is already cnf
        // and contains only columns
        final Set<Map<Filter, Filter>> hshm = new HashSet<Map<Filter, Filter>>();
        HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
        if (join.getClause().getPredicate() != null)
        {
            final Column l = join.getClause().getPredicate().getLHS().getColumn();
            final Column r = join.getClause().getPredicate().getRHS().getColumn();
            String o = join.getClause().getPredicate().getOp();

            if (join.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }
        else
        {
            final SearchCondition scond = join.getClause().getSearch();
            Column l = scond.getClause().getPredicate().getLHS().getColumn();
            Column r = scond.getClause().getPredicate().getRHS().getColumn();
            String o = scond.getClause().getPredicate().getOp();

            if (scond.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }

            for (final ConnectedSearchClause csc : scond.getConnected())
            {
                l = csc.getSearch().getPredicate().getLHS().getColumn();
                r = csc.getSearch().getPredicate().getRHS().getColumn();
                o = csc.getSearch().getPredicate().getOp();

                if (csc.getSearch().getNegated())
                {
                    o = ParseUtils.negate(o);
                }

                lhs = "";
                if (l.getTable() != null)
                {
                    lhs += l.getTable();
                }

                lhs += ("." + l.getColumn());

                rhs = "";
                if (r.getTable() != null)
                {
                    rhs += r.getTable();
                }
                rhs += ("." + r.getColumn());
                lhs = getMatchingCol(left, lhs);
                rhs = getMatchingCol(right, rhs);
                try
                {
                    verifyTypes(lhs, left, rhs, right);
                    final Filter f = new Filter(lhs, o, rhs);
                    hm.put(f, f);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
            }
        }

        hshm.add(hm);

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause sc : join.getConnected())
            {
                hm = new HashMap<Filter, Filter>();
                if (sc.getSearch().getPredicate() != null)
                {
                    final Column l = sc.getSearch().getPredicate().getLHS().getColumn();
                    final Column r = sc.getSearch().getPredicate().getRHS().getColumn();
                    String o = sc.getSearch().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }
                }
                else
                {
                    final SearchCondition scond = sc.getSearch().getSearch();
                    Column l = scond.getClause().getPredicate().getLHS().getColumn();
                    Column r = scond.getClause().getPredicate().getRHS().getColumn();
                    String o = scond.getClause().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }

                    for (final ConnectedSearchClause csc : scond.getConnected())
                    {
                        l = csc.getSearch().getPredicate().getLHS().getColumn();
                        r = csc.getSearch().getPredicate().getRHS().getColumn();
                        o = csc.getSearch().getPredicate().getOp();

                        if (csc.getSearch().getNegated())
                        {
                            o = ParseUtils.negate(o);
                        }

                        lhs = "";
                        if (l.getTable() != null)
                        {
                            lhs += l.getTable();
                        }

                        lhs += ("." + l.getColumn());

                        rhs = "";
                        if (r.getTable() != null)
                        {
                            rhs += r.getTable();
                        }
                        rhs += ("." + r.getColumn());
                        lhs = getMatchingCol(left, lhs);
                        rhs = getMatchingCol(right, rhs);
                        try
                        {
                            verifyTypes(lhs, left, rhs, right);
                            final Filter f = new Filter(lhs, o, rhs);
                            hm.put(f, f);
                        }
                        catch (final Exception e)
                        {
                            throw new ParseException(e.getMessage());
                        }
                    }
                }

                hshm.add(hm);
            }
        }

        hm = new HashMap<Filter, Filter>();
        hm.put(filter, filter);
        hshm.add(hm);

        try
        {
            final AntiJoinOperator anti = new AntiJoinOperator(hshm, meta);
            anti.add(left);
            anti.add(right);
            return anti;
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }
    }

    Operator connectWithSemiJoin(final Operator left, final Operator right, final SearchCondition join) throws ParseException
    {
        // assume join is already cnf
        // and contains only columns
        final Set<Map<Filter, Filter>> hshm = new HashSet<Map<Filter, Filter>>();
        HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
        if (join.getClause().getPredicate() != null)
        {
            final Column l = join.getClause().getPredicate().getLHS().getColumn();
            final Column r = join.getClause().getPredicate().getRHS().getColumn();
            String o = join.getClause().getPredicate().getOp();

            if (join.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }
        else
        {
            final SearchCondition scond = join.getClause().getSearch();
            Column l = scond.getClause().getPredicate().getLHS().getColumn();
            Column r = scond.getClause().getPredicate().getRHS().getColumn();
            String o = scond.getClause().getPredicate().getOp();

            if (scond.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }

            for (final ConnectedSearchClause csc : scond.getConnected())
            {
                l = csc.getSearch().getPredicate().getLHS().getColumn();
                r = csc.getSearch().getPredicate().getRHS().getColumn();
                o = csc.getSearch().getPredicate().getOp();

                if (csc.getSearch().getNegated())
                {
                    o = ParseUtils.negate(o);
                }

                lhs = "";
                if (l.getTable() != null)
                {
                    lhs += l.getTable();
                }

                lhs += ("." + l.getColumn());

                rhs = "";
                if (r.getTable() != null)
                {
                    rhs += r.getTable();
                }
                rhs += ("." + r.getColumn());
                lhs = getMatchingCol(left, lhs);
                rhs = getMatchingCol(right, rhs);
                try
                {
                    verifyTypes(lhs, left, rhs, right);
                    final Filter f = new Filter(lhs, o, rhs);
                    hm.put(f, f);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
            }
        }

        hshm.add(hm);

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause sc : join.getConnected())
            {
                hm = new HashMap<Filter, Filter>();
                if (sc.getSearch().getPredicate() != null)
                {
                    final Column l = sc.getSearch().getPredicate().getLHS().getColumn();
                    final Column r = sc.getSearch().getPredicate().getRHS().getColumn();
                    String o = sc.getSearch().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }
                }
                else
                {
                    final SearchCondition scond = sc.getSearch().getSearch();
                    Column l = scond.getClause().getPredicate().getLHS().getColumn();
                    Column r = scond.getClause().getPredicate().getRHS().getColumn();
                    String o = scond.getClause().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }

                    for (final ConnectedSearchClause csc : scond.getConnected())
                    {
                        l = csc.getSearch().getPredicate().getLHS().getColumn();
                        r = csc.getSearch().getPredicate().getRHS().getColumn();
                        o = csc.getSearch().getPredicate().getOp();

                        if (csc.getSearch().getNegated())
                        {
                            o = ParseUtils.negate(o);
                        }

                        lhs = "";
                        if (l.getTable() != null)
                        {
                            lhs += l.getTable();
                        }

                        lhs += ("." + l.getColumn());

                        rhs = "";
                        if (r.getTable() != null)
                        {
                            rhs += r.getTable();
                        }
                        rhs += ("." + r.getColumn());
                        lhs = getMatchingCol(left, lhs);
                        rhs = getMatchingCol(right, rhs);
                        try
                        {
                            verifyTypes(lhs, left, rhs, right);
                            final Filter f = new Filter(lhs, o, rhs);
                            hm.put(f, f);
                        }
                        catch (final Exception e)
                        {
                            throw new ParseException(e.getMessage());
                        }
                    }
                }

                hshm.add(hm);
            }
        }

        try
        {
            final SemiJoinOperator semi = new SemiJoinOperator(hshm, meta);
            semi.add(left);
            semi.add(right);
            return semi;
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }
    }

    Operator connectWithSemiJoin(final Operator left, final Operator right, final SearchCondition join, final Filter filter) throws ParseException
    {
        // assume join is already cnf
        // and contains only columns
        final Set<Map<Filter, Filter>> hshm = new HashSet<Map<Filter, Filter>>();
        HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
        if (join.getClause().getPredicate() != null)
        {
            final Column l = join.getClause().getPredicate().getLHS().getColumn();
            final Column r = join.getClause().getPredicate().getRHS().getColumn();
            String o = join.getClause().getPredicate().getOp();

            if (join.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }
        else
        {
            final SearchCondition scond = join.getClause().getSearch();
            Column l = scond.getClause().getPredicate().getLHS().getColumn();
            Column r = scond.getClause().getPredicate().getRHS().getColumn();
            String o = scond.getClause().getPredicate().getOp();

            if (scond.getClause().getNegated())
            {
                o = ParseUtils.negate(o);
            }

            String lhs = "";
            if (l.getTable() != null)
            {
                lhs += l.getTable();
            }

            lhs += ("." + l.getColumn());

            String rhs = "";
            if (r.getTable() != null)
            {
                rhs += r.getTable();
            }
            rhs += ("." + r.getColumn());
            lhs = getMatchingCol(left, lhs);
            rhs = getMatchingCol(right, rhs);
            try
            {
                verifyTypes(lhs, left, rhs, right);
                final Filter f = new Filter(lhs, o, rhs);
                hm.put(f, f);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }

            for (final ConnectedSearchClause csc : scond.getConnected())
            {
                l = csc.getSearch().getPredicate().getLHS().getColumn();
                r = csc.getSearch().getPredicate().getRHS().getColumn();
                o = csc.getSearch().getPredicate().getOp();

                if (csc.getSearch().getNegated())
                {
                    o = ParseUtils.negate(o);
                }

                lhs = "";
                if (l.getTable() != null)
                {
                    lhs += l.getTable();
                }

                lhs += ("." + l.getColumn());

                rhs = "";
                if (r.getTable() != null)
                {
                    rhs += r.getTable();
                }
                rhs += ("." + r.getColumn());
                lhs = getMatchingCol(left, lhs);
                rhs = getMatchingCol(right, rhs);
                try
                {
                    verifyTypes(lhs, left, rhs, right);
                    final Filter f = new Filter(lhs, o, rhs);
                    hm.put(f, f);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
            }
        }

        hshm.add(hm);

        if (join.getConnected() != null && join.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause sc : join.getConnected())
            {
                hm = new HashMap<Filter, Filter>();
                if (sc.getSearch().getPredicate() != null)
                {
                    final Column l = sc.getSearch().getPredicate().getLHS().getColumn();
                    final Column r = sc.getSearch().getPredicate().getRHS().getColumn();
                    String o = sc.getSearch().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }
                }
                else
                {
                    final SearchCondition scond = sc.getSearch().getSearch();
                    Column l = scond.getClause().getPredicate().getLHS().getColumn();
                    Column r = scond.getClause().getPredicate().getRHS().getColumn();
                    String o = scond.getClause().getPredicate().getOp();

                    if (sc.getSearch().getNegated())
                    {
                        o = ParseUtils.negate(o);
                    }

                    String lhs = "";
                    if (l.getTable() != null)
                    {
                        lhs += l.getTable();
                    }

                    lhs += ("." + l.getColumn());

                    String rhs = "";
                    if (r.getTable() != null)
                    {
                        rhs += r.getTable();
                    }
                    rhs += ("." + r.getColumn());
                    lhs = getMatchingCol(left, lhs);
                    rhs = getMatchingCol(right, rhs);
                    try
                    {
                        verifyTypes(lhs, left, rhs, right);
                        final Filter f = new Filter(lhs, o, rhs);
                        hm.put(f, f);
                    }
                    catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage());
                    }

                    for (final ConnectedSearchClause csc : scond.getConnected())
                    {
                        l = csc.getSearch().getPredicate().getLHS().getColumn();
                        r = csc.getSearch().getPredicate().getRHS().getColumn();
                        o = csc.getSearch().getPredicate().getOp();

                        if (csc.getSearch().getNegated())
                        {
                            o = ParseUtils.negate(o);
                        }

                        lhs = "";
                        if (l.getTable() != null)
                        {
                            lhs += l.getTable();
                        }

                        lhs += ("." + l.getColumn());

                        rhs = "";
                        if (r.getTable() != null)
                        {
                            rhs += r.getTable();
                        }
                        rhs += ("." + r.getColumn());
                        lhs = getMatchingCol(left, lhs);
                        rhs = getMatchingCol(right, rhs);
                        try
                        {
                            verifyTypes(lhs, left, rhs, right);
                            final Filter f = new Filter(lhs, o, rhs);
                            hm.put(f, f);
                        }
                        catch (final Exception e)
                        {
                            throw new ParseException(e.getMessage());
                        }
                    }
                }

                hshm.add(hm);
            }
        }

        hm = new HashMap<Filter, Filter>();
        hm.put(filter, filter);
        hshm.add(hm);

        try
        {
            final SemiJoinOperator semi = new SemiJoinOperator(hshm, meta);
            semi.add(left);
            semi.add(right);
            return semi;
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }
    }
}