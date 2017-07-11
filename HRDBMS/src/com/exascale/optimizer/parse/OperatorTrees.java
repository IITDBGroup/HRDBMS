package com.exascale.optimizer.parse;

import com.exascale.exceptions.ParseException;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DateParser;
import com.exascale.misc.Utils;
import com.exascale.optimizer.*;
import com.exascale.optimizer.externalTable.*;
import com.exascale.optimizer.load.Load;
import com.exascale.optimizer.load.LoadOperator;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.exascale.optimizer.parse.SQLParser.*;
import static com.exascale.optimizer.parse.ParseUtils.*;

/** Utilities used by the SQL parser to build operator trees from statements passed by the CLI */
public class OperatorTrees extends AbstractParseController {
    /** Initialize with information that's needed to build a plan for any configuration of operators */
    public OperatorTrees(ConnectionWorker connection, Transaction tx, MetaData meta, SQLParser.Model model) {
        super(connection, tx, meta, model);
    }

    Operator buildOperatorTreeFromCreateTable(final CreateTable createTable) throws Exception
    {
        final TableName table = createTable.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (MetaData.verifyTableExistence(schema, tbl, tx) || MetaData.verifyViewExistence(schema, tbl, tx))
        {
            throw new ParseException("Table or view already exists");
        }

        final ArrayList<ColDef> colDefs = createTable.getCols();
        final HashSet<String> colNames = new HashSet<String>();
        final HashSet<String> pks = new HashSet<String>();
        final ArrayList<String> orderedPks = new ArrayList<String>();
        for (final ColDef def : colDefs)
        {
            final String col = def.getCol().getColumn();
            if (def.getCol().getTable() != null)
            {
                throw new ParseException("Column names cannot be qualified with table names in a CREATE TABLE statement");
            }

            if (!colNames.add(col))
            {
                throw new ParseException("CREATE TABLE statement had a duplicate column names");
            }

            if (def.isNullable() && def.isPK())
            {
                throw new ParseException("A column cannot be nullable and part of the primary key");
            }

            if (def.isPK())
            {
                if (pks.add(col))
                {
                    orderedPks.add(col);
                }
            }
        }

        if (createTable.getPK() != null)
        {
            final ArrayList<Column> cols = createTable.getPK().getCols();
            for (final Column col : cols)
            {
                final String c = col.getColumn();
                if (col.getTable() != null)
                {
                    throw new ParseException("Column names cannot be qualified with table names in a CREATE TABLE statement");
                }

                if (pks.add(c))
                {
                    orderedPks.add(c);
                }
            }
        }

        if (createTable.getType() != 0 && colDefs.size() > 26212)
        {
            throw new ParseException("Maximum number of columns for a column table is 26212");
        }

        CreateTableOperator retval = null;

        if (createTable.getType() != 0 && createTable.getColOrder() != null)
        {
            if (colDefs.size() != createTable.getColOrder().size())
            {
                throw new ParseException("Explicit COLORDER defined on a column table but it had the wrong number of columns");
            }

            int z = 1;
            boolean ok = true;
            final ArrayList<Integer> colOrder = createTable.getColOrder();
            while (z <= colDefs.size())
            {
                if (!colOrder.contains(z++))
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                throw new ParseException("COLORDER clause is the right size but is invalid");
            }

            retval = new CreateTableOperator(schema, tbl, colDefs, orderedPks, createTable.getNodeGroupExp(), createTable.getNodeExp(), createTable.getDeviceExp(), meta, createTable.getType(), colOrder);
        }
        else
        {
            retval = new CreateTableOperator(schema, tbl, colDefs, orderedPks, createTable.getNodeGroupExp(), createTable.getNodeExp(), createTable.getDeviceExp(), meta, createTable.getType());
        }

        if (createTable.getType() != 0 && createTable.getOrganization() != null)
        {
            final ArrayList<Integer> organization = createTable.getOrganization();
            if (organization.size() < 1 || organization.size() > colDefs.size())
            {
                throw new ParseException("ORGANIZATION clause has an invalid size");
            }

            for (final int index : organization)
            {
                if (index < 1 || index > colDefs.size())
                {
                    throw new ParseException("There is an invalid entry in the ORGANIZATION clause (" + index + ")");
                }
            }

            retval.setOrganization(organization);
        }

        return retval;
    }

    /** Handles parsing of the create external table statement. */
    Operator buildOperatorTreeFromCreateExternalTable(CreateExternalTable createExternalTable) throws Exception {
        TableName table = createExternalTable.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null) {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        } else {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (meta.verifyTableExistence(schema, tbl, tx) || meta.verifyViewExistence(schema, tbl, tx)) {
            throw new ParseException("External Table already exists");
        }

        ArrayList<ColDef> colDefs = createExternalTable.getCols();
        HashSet<String> colNames = new HashSet<String>();
        for (ColDef def : colDefs) {
            String col = def.getCol().getColumn();
            if (def.getCol().getTable() != null) {
                throw new ParseException("Column names cannot be qualified with table names in a CREATE TABLE statement");
            }

            if (!colNames.add(col)) {
                throw new ParseException("CREATE TABLE statement had a duplicate column names");
            }

            if (def.isNullable() && def.isPK()) {
                throw new ParseException("A column cannot be nullable and part of the primary key");
            }
        }

        CreateExternalTableOperator op = null;
        if (createExternalTable.getJavaClassExtTableSpec() != null) {
            JavaClassExtTableSpec tableSpec = createExternalTable.getJavaClassExtTableSpec();
            op = new CreateExternalTableOperator(meta, schema, tbl, colDefs, tableSpec.getJavaClassName(), tableSpec.getParams());
        } else {
            throw new UnsupportedOperationException("External table implementation requires a Java class");
        }
        op.setTransaction(tx);
        return op;
    }

    Operator buildOperatorTreeFromCreateIndex(final CreateIndex createIndex) throws Exception
    {
        final TableName table = createIndex.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyTableExistence(schema, tbl, tx))
        {
            throw new ParseException("Table does not exist");
        }

        final String index = createIndex.getIndex().getName();
        if (createIndex.getIndex().getSchema() != null)
        {
            throw new ParseException("Schemas cannot be specified for index names");
        }

        if (MetaData.verifyIndexExistence(schema, index, tx))
        {
            throw new ParseException("Index already exists");
        }

        final boolean unique = createIndex.getUnique();
        final ArrayList<IndexDef> indexDefs = createIndex.getCols();
        for (final IndexDef def : indexDefs)
        {
            final String col = def.getCol().getColumn();
            if (def.getCol().getTable() != null)
            {
                throw new ParseException("Column names cannot be qualified with table names in a CREATE INDEX statement");
            }
            if (!MetaData.verifyColExistence(schema, tbl, col, tx))
            {
                throw new ParseException("Column " + col + " does not exist");
            }
        }

        return new CreateIndexOperator(schema, tbl, index, indexDefs, unique, meta);
    }

    Operator buildOperatorTreeFromCreateView(final CreateView createView) throws Exception
    {
        buildOperatorTreeFromFullSelect(createView.getSelect());
        final TableName table = createView.getView();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (MetaData.verifyTableExistence(schema, tbl, tx) || MetaData.verifyViewExistence(schema, tbl, tx))
        {
            throw new ParseException("Table or view already exists");
        }

        return new CreateViewOperator(schema, tbl, createView.getText(), meta);
    }

    Operator buildOperatorTreeFromDelete(final Delete delete) throws Exception
    {
        final TableName table = delete.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyTableExistence(schema, tbl, tx))
        {
            throw new ParseException("Table does not exist");
        }

        if (delete.getWhere() == null)
        {
            return new MassDeleteOperator(schema, tbl, meta);
        }

        final TableScanOperator scan = new TableScanOperator(schema, tbl, meta, tx);
        Operator op = buildOperatorTreeFromWhere(delete.getWhere(), scan, null);
        scan.getRID();
        final ArrayList<String> cols = new ArrayList<String>();
        cols.add("_RID1");
        cols.add("_RID2");
        cols.add("_RID3");
        cols.add("_RID4");
        cols.addAll(MetaData.getIndexColsForTable(schema, tbl, tx));
        final ProjectOperator project = new ProjectOperator(cols, meta);
        project.add(op);
        op = project;
        final RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
        retval.add(op);
        final Phase1 p1 = new Phase1(retval, tx);
        p1.optimize();
        new Phase2(retval, tx).optimize();
        new Phase3(retval, tx).optimize();
        new Phase4(retval, tx).optimize();
        new Phase5(retval, tx, p1.likelihoodCache).optimize();
        final DeleteOperator dOp = new DeleteOperator(schema, tbl, meta);
        final Operator child = retval.children().get(0);
        retval.removeChild(child);
        dOp.add(child);
        return dOp;
    }

    Operator buildOperatorTreeFromDropIndex(final DropIndex dropIndex) throws Exception
    {
        final TableName table = dropIndex.getIndex();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyIndexExistence(schema, tbl, tx))
        {
            throw new ParseException("Index does not exist");
        }

        return new DropIndexOperator(schema, tbl, meta);
    }

    Operator buildOperatorTreeFromDropTable(final DropTable dropTable) throws Exception
    {
        final TableName table = dropTable.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyTableExistence(schema, tbl, tx))
        {
            throw new ParseException("Table does not exist");
        }

        return new DropTableOperator(schema, tbl, meta);
    }

    Operator buildOperatorTreeFromDropView(final DropView dropView) throws Exception
    {
        final TableName table = dropView.getView();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyViewExistence(schema, tbl, tx))
        {
            throw new ParseException("Table or view does not exist");
        }

        return new DropViewOperator(schema, tbl, meta);
    }

    Operator buildOperatorTreeFromFetchFirst(final FetchFirst fetchFirst, final Operator op) throws ParseException
    {
        try
        {
            final TopOperator top = new TopOperator(fetchFirst.getNumber(), meta);
            top.add(op);
            return top;
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }
    }

    Operator buildOperatorTreeFromFrom(final FromClause from, final SubSelect sub) throws Exception
    {
        final ArrayList<TableReference> tables = from.getTables();
        final ArrayList<Operator> ops = new ArrayList<Operator>(tables.size());
        for (final TableReference table : tables)
        {
            ops.add(buildOperatorTreeFromTableReference(table, sub));
        }

        if (ops.size() == 1)
        {
            return ops.get(0);
        }

        Operator top = ops.get(0);
        ops.remove(0);
        while (ops.size() > 0)
        {
            final Operator product = new ProductOperator(meta);
            try
            {
                product.add(top);
                product.add(ops.get(0));
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
            ops.remove(0);
            top = product;
        }

        return top;
    }

    Operator buildOperatorTreeFromFullSelect(final FullSelect select) throws Exception
    {
        Operator op;
        if (select.getSubSelect() != null)
        {
            op = buildOperatorTreeFromSubSelect(select.getSubSelect(), false);
        }
        else
        {
            op = buildOperatorTreeFromFullSelect(select.getFullSelect());
        }

        // handle connectedSelects
        final ArrayList<ConnectedSelect> connected = select.getConnected();
        Operator lhs = op;
        for (final ConnectedSelect cs : connected)
        {
            Operator rhs;
            if (cs.getFull() != null)
            {
                rhs = buildOperatorTreeFromFullSelect(cs.getFull());
            }
            else
            {
                rhs = buildOperatorTreeFromSubSelect(cs.getSub(), false);
            }

            verifyColumnsAreTheSame(lhs, rhs);
            final String combo = cs.getCombo();
            if (combo.equals("UNION ALL"))
            {
                final Operator newOp = new UnionOperator(false, meta);
                try
                {
                    newOp.add(lhs);
                    newOp.add(rhs);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                lhs = newOp;
            }
            else if (combo.equals("UNION"))
            {
                final Operator newOp = new UnionOperator(true, meta);
                try
                {
                    newOp.add(lhs);
                    newOp.add(rhs);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                lhs = newOp;
            }
            else if (combo.equals("INTERSECT"))
            {
                final Operator newOp = new IntersectOperator(meta);
                try
                {
                    newOp.add(lhs);
                    newOp.add(rhs);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                lhs = newOp;
            }
            else if (combo.equals("EXCEPT"))
            {
                final Operator newOp = new ExceptOperator(meta);
                try
                {
                    newOp.add(lhs);
                    newOp.add(rhs);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
                lhs = newOp;
            }
            else
            {
                throw new ParseException("Unknown table combination type " + combo);
            }
        }

        op = lhs;

        // handle orderBy
        if (select.getOrderBy() != null)
        {
            op = buildOperatorTreeFromOrderBy(select.getOrderBy(), op);
        }
        // handle fetchFirst
        if (select.getFetchFirst() != null)
        {
            op = buildOperatorTreeFromFetchFirst(select.getFetchFirst(), op);
        }

        return op;
    }

    Operator buildOperatorTreeFromGroupBy(final GroupBy groupBy, final Operator op, final SubSelect select) throws ParseException
    {
        ArrayList<Column> cols;
        ArrayList<String> vStr;

        if (groupBy != null)
        {
            cols = groupBy.getCols();
            vStr = new ArrayList<>(cols.size());
            for (final Column col : cols)
            {
                String colString = "";
                if (col.getTable() != null)
                {
                    colString += col.getTable();
                }

                colString += ("." + col.getColumn());
                vStr.add(colString);
            }
        }
        else
        {
            vStr = new ArrayList<String>();
        }

        final ArrayList<AggregateOperator> ops = new ArrayList<>();
        for (final ArrayList<Object> row : model.getComplex())
        {
            // colName, op, type, id, exp, prereq, done
            if ((Boolean)row.get(7) == false && (Integer)row.get(2) == TYPE_GROUPBY && (row.get(6) == null || ((SubSelect)row.get(6)).equals(select)))
            {
                final AggregateOperator agop = (AggregateOperator)row.get(1);
                if (agop instanceof AvgOperator)
                {
                    final String col = getMatchingCol(op, agop.getInputColumn());
                    agop.setInput(col);
                    final String type = op.getCols2Types().get(col);

                    if (type == null)
                    {
                        throw new ParseException("Column " + col + " was referenced but not found");
                    }

                    if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
                    {
                    }
                    else
                    {
                        throw new ParseException("The argument to AVG() must be numeric");
                    }
                }
                else if (agop instanceof CountOperator)
                {
                    if (!agop.getInputColumn().equals(agop.outputColumn()))
                    {
                        final String col = getMatchingCol(op, agop.getInputColumn());
                        agop.setInput(col);
                        final String type = op.getCols2Types().get(col);

                        if (type == null)
                        {
                            throw new ParseException("Column " + col + " was referenced but not found");
                        }
                    }
                }
                else if (agop instanceof CountDistinctOperator)
                {
                    if (!agop.getInputColumn().equals(agop.outputColumn()))
                    {
                        final String col = getMatchingCol(op, agop.getInputColumn());
                        agop.setInput(col);
                        final String type = op.getCols2Types().get(col);

                        if (type == null)
                        {
                            HRDBMSWorker.logger.debug("Could not find " + col + " in " + op.getCols2Types());
                            throw new ParseException("Column " + col + " was referenced but not found");
                        }
                    }
                }
                else if (agop instanceof MaxOperator)
                {
                    final String col = getMatchingCol(op, agop.getInputColumn());
                    agop.setInput(col);
                    final String type = op.getCols2Types().get(col);

                    if (type == null)
                    {
                        throw new ParseException("Column " + col + " was referenced but not found");
                    }

                    if (type.equals("INT"))
                    {
                        ((MaxOperator)agop).setIsInt(true);
                        ((MaxOperator)agop).setIsLong(false);
                        ((MaxOperator)agop).setIsFloat(false);
                        ((MaxOperator)agop).setIsChar(false);
                        ((MaxOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("LONG"))
                    {
                        ((MaxOperator)agop).setIsInt(false);
                        ((MaxOperator)agop).setIsLong(true);
                        ((MaxOperator)agop).setIsFloat(false);
                        ((MaxOperator)agop).setIsChar(false);
                        ((MaxOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("FLOAT"))
                    {
                        ((MaxOperator)agop).setIsInt(false);
                        ((MaxOperator)agop).setIsLong(false);
                        ((MaxOperator)agop).setIsFloat(true);
                        ((MaxOperator)agop).setIsChar(false);
                        ((MaxOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("CHAR"))
                    {
                        ((MaxOperator)agop).setIsInt(false);
                        ((MaxOperator)agop).setIsLong(false);
                        ((MaxOperator)agop).setIsFloat(false);
                        ((MaxOperator)agop).setIsChar(true);
                        ((MaxOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("DATE"))
                    {
                        ((MaxOperator)agop).setIsInt(false);
                        ((MaxOperator)agop).setIsLong(false);
                        ((MaxOperator)agop).setIsFloat(false);
                        ((MaxOperator)agop).setIsChar(false);
                        ((MaxOperator)agop).setIsDate(true);
                    }
                }
                else if (agop instanceof MinOperator)
                {
                    final String col = getMatchingCol(op, agop.getInputColumn());
                    agop.setInput(col);
                    final String type = op.getCols2Types().get(col);

                    if (type == null)
                    {
                        throw new ParseException("Column " + col + " was referenced but not found");
                    }

                    if (type.equals("INT"))
                    {
                        ((MinOperator)agop).setIsInt(true);
                        ((MinOperator)agop).setIsLong(false);
                        ((MinOperator)agop).setIsFloat(false);
                        ((MinOperator)agop).setIsChar(false);
                        ((MinOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("LONG"))
                    {
                        ((MinOperator)agop).setIsInt(false);
                        ((MinOperator)agop).setIsLong(true);
                        ((MinOperator)agop).setIsFloat(false);
                        ((MinOperator)agop).setIsChar(false);
                        ((MinOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("FLOAT"))
                    {
                        ((MinOperator)agop).setIsInt(false);
                        ((MinOperator)agop).setIsLong(false);
                        ((MinOperator)agop).setIsFloat(true);
                        ((MinOperator)agop).setIsChar(false);
                        ((MinOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("CHAR"))
                    {
                        ((MinOperator)agop).setIsInt(false);
                        ((MinOperator)agop).setIsLong(false);
                        ((MinOperator)agop).setIsFloat(false);
                        ((MinOperator)agop).setIsChar(true);
                        ((MinOperator)agop).setIsDate(false);
                    }
                    else if (type.equals("DATE"))
                    {
                        ((MinOperator)agop).setIsInt(false);
                        ((MinOperator)agop).setIsLong(false);
                        ((MinOperator)agop).setIsFloat(false);
                        ((MinOperator)agop).setIsChar(false);
                        ((MinOperator)agop).setIsDate(true);
                    }
                }
                else if (agop instanceof SumOperator)
                {
                    final String col = getMatchingCol(op, agop.getInputColumn());
                    agop.setInput(col);
                    final String type = op.getCols2Types().get(col);

                    if (type == null)
                    {
                        throw new ParseException("Column " + col + " was referenced but not found");
                    }

                    if (type.equals("INT") || type.equals("LONG"))
                    {
                        ((SumOperator)agop).setIsInt(true);
                    }
                    else if (type.equals("FLOAT"))
                    {
                        ((SumOperator)agop).setIsInt(false);
                    }
                    else
                    {
                        throw new ParseException("The argument to SUM() must be numeric");
                    }
                }

                ops.add(agop);
                row.remove(7);
                row.add(true);
            }
        }

        if (ops.size() > 0)
        {
            try
            {
                final MultiOperator multi = new MultiOperator(ops, vStr, meta, false);
                try
                {
                    multi.add(op);
                }
                catch (final Exception e)
                {
                    HRDBMSWorker.logger.debug("Exception trying to add MultiOperator.  Tree is: ");
                    Utils.printTree(op, 0);
                    throw e;
                }
                return multi;
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }
        else
        {
            return op;
        }
    }

    Operator buildOperatorTreeFromHaving(final Having having, final Operator op, final SubSelect sub) throws Exception
    {
        SearchOperatorTrees searchOperatorTrees = new SearchOperatorTrees(connection, tx, meta, model);
        return searchOperatorTrees.buildOperatorTreeFromSearchCondition(having.getSearch(), op, sub);
    }

    ArrayList<Operator> buildOperatorTreeFromInsert(final Insert insert) throws Exception
    {
        final TableName table = insert.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyTableExistence(schema, tbl, tx))
        {
            throw new ParseException("Table does not exist");
        }

        if (insert.fromSelect())
        {
            final Operator op = buildOperatorTreeFromFullSelect(insert.getSelect());
            final RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
            retval.add(op);
            final Phase1 p1 = new Phase1(retval, tx);
            p1.optimize();
            new Phase2(retval, tx).optimize();
            new Phase3(retval, tx).optimize();
            new Phase4(retval, tx).optimize();
            new Phase5(retval, tx, p1.likelihoodCache).optimize();

            if (!MetaData.verifyInsert(schema, tbl, op, tx))
            {
                throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
            }

            final Operator iOp = new InsertOperator(schema, tbl, meta);
            final Operator child = retval.children().get(0);
            retval.removeChild(child);
            iOp.add(child);
            final ArrayList<Operator> iOps = new ArrayList<Operator>(1);
            iOps.add(iOp);
            return iOps;
        }
        else
        {
            ExpressionOperatorTrees expressionOperatorTrees = new ExpressionOperatorTrees(connection, tx, meta, model);
            if (!insert.isMulti())
            {
                Operator op = new DummyOperator(meta);
                for (final Expression exp : insert.getExpressions())
                {
                    final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(exp, null, null);
                    if (otan.getType() != TYPE_INLINE && otan.getType() != SQLParser.TYPE_DATE)
                    {
                        throw new ParseException("Invalid expression in insert statement");
                    }

                    if (otan.getType() == SQLParser.TYPE_DATE)
                    {
                        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                        final String name = "._E" + model.getAndIncrementSuffix();
                        final ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
                        operator.add(op);
                        op = operator;
                    }
                    else
                    {
                        final Operator operator = (Operator)otan.getOp();
                        operator.add(op);
                        op = operator;
                    }
                }

                if (!MetaData.verifyInsert(schema, tbl, op, tx))
                {
                    throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
                }

                Operator temp = op;
                while (temp.children() != null && temp.children().size() > 0)
                {
                    if (temp instanceof ExtendOperator)
                    {
                        ((ExtendOperator)temp).setSingleThreaded();
                    }

                    temp = temp.children().get(0);
                }

                if (temp instanceof ExtendOperator)
                {
                    ((ExtendOperator)temp).setSingleThreaded();
                }

                final Operator iOp = new InsertOperator(schema, tbl, meta);
                iOp.add(op);
                final ArrayList<Operator> iOps = new ArrayList<Operator>(1);
                iOps.add(iOp);
                return iOps;
            }
            else
            {
                // multi-row insert
                final ArrayList<Operator> iOps = new ArrayList<Operator>(1);
                for (final ArrayList<Expression> exps : insert.getMultiExpressions())
                {
                    Operator op = new DummyOperator(meta);
                    for (final Expression exp : exps)
                    {
                        final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(exp, null, null);
                        if (otan.getType() != TYPE_INLINE && otan.getType() != SQLParser.TYPE_DATE)
                        {
                            throw new ParseException("Invalid expression in insert statement");
                        }

                        if (otan.getType() == SQLParser.TYPE_DATE)
                        {
                            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                            final String name = "._E" + model.getAndIncrementSuffix();
                            final ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
                            operator.add(op);
                            op = operator;
                        }
                        else
                        {
                            final Operator operator = (Operator)otan.getOp();
                            operator.add(op);
                            op = operator;
                        }
                    }

                    if (!MetaData.verifyInsert(schema, tbl, op, tx))
                    {
                        throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
                    }

                    Operator temp = op;
                    while (temp.children() != null && temp.children().size() > 0)
                    {
                        if (temp instanceof ExtendOperator)
                        {
                            ((ExtendOperator)temp).setSingleThreaded();
                        }

                        temp = temp.children().get(0);
                    }

                    if (temp instanceof ExtendOperator)
                    {
                        ((ExtendOperator)temp).setSingleThreaded();
                    }

                    final Operator iOp = new InsertOperator(schema, tbl, meta);
                    iOp.add(op);
                    iOps.add(iOp);
                }

                return iOps;
            }
        }
    }

    Operator buildOperatorTreeFromLoad(final Load load) throws Exception
    {
        final TableName table = load.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyTableExistence(schema, tbl, tx))
        {
            throw new ParseException("Table does not exist");
        }

        ExternalTableType extTable = meta.getExternalTable(schema, load.getExtTable().getName(), tx);
        Operator child = new ExternalTableScanOperator(extTable, schema, load.getExtTable().getName(), meta, tx);
        Operator op = new LoadOperator(schema, tbl, load.isReplace(), load.getDelimiter(), load.getGlob(), meta);
        op.add(child);

        final RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
        retval.add(op);
        final Phase1 p1 = new Phase1(retval, tx);
        p1.optimize();
        new Phase2(retval, tx).optimize();
        new Phase3(retval, tx).optimize();
        new Phase4(retval, tx).optimize();
        new Phase5(retval, tx, p1.likelihoodCache).optimize();
        return retval;
    }

    Operator buildOperatorTreeFromOrderBy(final OrderBy orderBy, final Operator op) throws ParseException
    {
        final ArrayList<SortKey> keys = orderBy.getKeys();
        final ArrayList<String> columns = new ArrayList<>(keys.size());
        final ArrayList<Boolean> orders = new ArrayList<>(keys.size());

        for (final SortKey key : keys)
        {
            String colStr = null;
            if (key.isColumn())
            {
                final Column col = key.getColumn();
                if (col.getTable() != null)
                {
                    colStr = col.getTable() + "." + col.getColumn();
                    int matches = 0;
                    for (final String c : op.getPos2Col().values())
                    {
                        if (c.equals(colStr))
                        {
                            matches++;
                        }
                    }

                    if (matches == 0)
                    {
                        throw new ParseException("Column " + colStr + " does not exist");
                    }
                    else if (matches > 1)
                    {
                        throw new ParseException("Column " + colStr + " is ambiguous");
                    }
                }
                else
                {
                    int matches = 0;
                    String table = null;
                    boolean withDot = true;
                    for (final String c : op.getPos2Col().values())
                    {
                        if (c.contains("."))
                        {
                            final String p1 = c.substring(0, c.indexOf('.'));
                            final String p2 = c.substring(c.indexOf('.') + 1);

                            if (p2.equals(col.getColumn()))
                            {
                                matches++;
                                table = p1;
                            }
                        }
                        else
                        {
                            if (c.equals(col.getColumn()))
                            {
                                matches++;
                                withDot = false;
                            }
                        }
                    }

                    if (matches == 0)
                    {
                        Utils.printTree(op, 0);
                        throw new ParseException("Column " + col.getColumn() + " does not exist");
                    }
                    else if (matches > 1)
                    {
                        throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
                    }

                    if (withDot)
                    {
                        colStr = table + "." + col.getColumn();
                    }
                    else
                    {
                        colStr = col.getColumn();
                    }
                }

                columns.add(colStr);
                orders.add(key.getDirection());
            }
            else
            {
                // handle numbered keys
                colStr = op.getPos2Col().get(new Integer(key.getNum()));
                columns.add(colStr);
                orders.add(key.getDirection());
            }
        }

        try
        {
            final SortOperator sort = new SortOperator(columns, orders, meta);
            sort.add(op);
            return sort;
        }
        catch (final Exception e)
        {
            HRDBMSWorker.logger.debug("", e);
            throw new ParseException(e.getMessage());
        }
    }

    Operator buildOperatorTreeFromRunstats(final Runstats runstats) throws Exception
    {
        final TableName table = runstats.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        final Transaction tx2 = new Transaction(Transaction.ISOLATION_RR);
        if (!MetaData.verifyTableExistence(schema, tbl, tx2))
        {
            tx2.commit();
            throw new ParseException("Table does not exist");
        }

        tx2.commit();
        return new RunstatsOperator(schema, tbl, meta);
    }

    /** Builds execution plan for the entire select statement including common table expressions and the select itself (fullSelect) */
    Operator buildOperatorTreeFromSelect(final Select select) throws Exception
    {
        if (select.getCTEs().size() > 0)
        {
            for (final CTE cte : select.getCTEs())
            {
                importCTE(cte, select.getFullSelect());
            }
        }
        return buildOperatorTreeFromFullSelect(select.getFullSelect());
    }

    private Operator buildOperatorTreeFromSelectClause(final SelectClause select, Operator op, final SubSelect sub, final boolean subquery) throws ParseException
    {
        if (!select.isSelectStar())
        {
            ArrayList<String> cols = new ArrayList<String>();
            final ArrayList<SelectListEntry> selects = select.getSelectList();
            boolean needsRename = false;
            for (final SelectListEntry entry : selects)
            {
                if (entry.isColumn())
                {
                    final Column col = entry.getColumn();
                    String colName = "";
                    if (col.getTable() != null)
                    {
                        colName += col.getTable();
                    }

                    colName += ("." + col.getColumn());
                    cols.add(colName);
                }
                else
                {
                    if (entry.getName() != null)
                    {
                        String theName = entry.getName();
                        if (!theName.contains("."))
                        {
                            theName = "." + theName;
                        }
                        cols.add(theName);
                    }
                    else
                    {
                        // unnamed complex column
                        for (final ArrayList<Object> row : model.getComplex())
                        {
                            if (row.get(4).equals(entry.getExpression()) && row.get(6).equals(sub))
                            {
                                String name = (String)row.get(0);
                                if (name.indexOf('.') == 0)
                                {
                                    name = name.substring(1);
                                }
                                cols.add(name);
                            }
                        }
                    }
                }
            }

            final ArrayList<String> newCols = new ArrayList<String>();
            final ArrayList<String> olds = new ArrayList<String>();
            final ArrayList<String> news = new ArrayList<String>();
            // HRDBMSWorker.logger.debug("Cols = " + cols);
            for (String col : cols)
            {
                String col2 = null;
                SelectListEntry sle = null;
                for (final SelectListEntry entry : selects)
                {
                    if (entry.isColumn())
                    {
                        final Column c = entry.getColumn();
                        col2 = "";
                        if (c.getTable() != null)
                        {
                            col2 += c.getTable();
                        }

                        col2 += ("." + c.getColumn());
                    }
                    else
                    {
                        if (entry.getName() != null)
                        {
                            String theName = entry.getName();
                            if (!theName.contains("."))
                            {
                                theName = "." + theName;
                            }
                            col2 = theName;
                        }
                        else
                        {
                            for (final ArrayList<Object> row : model.getComplex())
                            {
                                if (row.get(4).equals(entry.getExpression()) && row.get(6).equals(sub))
                                {
                                    col2 = (String)row.get(0);
                                    if (col2.indexOf('.') == 0)
                                    {
                                        col2 = col2.substring(1);
                                        if (col.equals(col2))
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (col.equals(col2))
                    {
                        sle = entry;
                        break;
                    }
                }

                final Integer pos = op.getCols2Pos().get(col);
                // HRDBMSWorker.logger.debug("Cols2Pos = " + op.getCols2Pos());
                if (pos == null)
                {
                    // try without schema
                    // must only be 1 match
                    if (col.contains("."))
                    {
                        if (col.indexOf('.') != 0)
                        {
                            throw new ParseException("Column not found: " + col);
                        }
                        else
                        {
                            col = col.substring(1);
                        }
                    }
                    for (final String col3 : op.getCols2Pos().keySet())
                    {
                        int matches = 0;
                        final String u = col3.substring(col3.indexOf('.') + 1);
                        if (col.equals(u))
                        {
                            newCols.add(col3);
                            if (sle != null && sle.getName() != null && !sle.getName().equals(col3))
                            {
                                needsRename = true;
                                olds.add(col3);
                                news.add(sle.getName());
                            }
                            matches++;
                        }

                        if (matches > 1)
                        {
                            throw new ParseException("Ambiguous acolumn: " + col);
                        }
                    }
                }
                else
                {
                    newCols.add(col);
                    if (sle != null && sle.getName() != null && !sle.getName().equals(col))
                    {
                        needsRename = true;
                        olds.add(col);
                        news.add(sle.getName());
                    }
                }
            }

            cols = newCols;
            // HRDBMSWorker.logger.debug("After all the magic, cols = " + cols);
            try
            {
                final ReorderOperator reorder = new ReorderOperator(cols, meta);
                reorder.add(op);
                op = reorder;
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }

            if (needsRename)
            {
                RenameOperator rename = null;
                try
                {
                    rename = new RenameOperator(olds, news, meta);
                    rename.add(op);
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }

                op = rename;
            }
        }
        else
        {
            if (!subquery)
            {
                final ArrayList<String> cols = new ArrayList<String>(op.getPos2Col().values().size());
                for (final String col : op.getPos2Col().values())
                {
                    if (!col.startsWith("_"))
                    {
                        cols.add(col);
                    }
                }
                try
                {
                    final ReorderOperator reorder = new ReorderOperator(cols, meta);
                    reorder.add(op);
                    op = reorder;
                }
                catch (final Exception e)
                {
                    throw new ParseException(e.getMessage());
                }
            }
        }

        if (!select.isSelectAll())
        {
            final UnionOperator distinct = new UnionOperator(true, meta);
            try
            {
                distinct.add(op);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
            op = distinct;
        }

        return op;
    }

    private Operator buildOperatorTreeFromSingleTable(final SingleTable table) throws Exception
    {
        final TableName name = table.getName();
        String schema, tblName;
        if (name.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            model.useCurrentSchema();
        }
        else
        {
            schema = name.getSchema();
        }

        tblName = name.getName();

        if (!MetaData.verifyTableExistence(schema, tblName, tx))
        {
            if (!MetaData.verifyViewExistence(schema, tblName, tx))
            {
                throw new ParseException("Table or view " + schema + "." + tblName + " does not exist");
            }

            final SQLParser viewParser = new SQLParser(MetaData.getViewSQL(schema, tblName, tx), connection, tx);
            Operator op = null;
            try
            {
                op = viewParser.parse().get(0);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
            if (!viewParser.doesNotUseCurrentSchema())
            {
                model.useCurrentSchema();
            }

            final Operator retval = op.children().get(0);
            op.removeChild(retval);

            final ArrayList<String> olds = new ArrayList<String>();
            final ArrayList<String> news = new ArrayList<String>();

            for (final String c : retval.getCols2Pos().keySet())
            {
                if (!c.contains("."))
                {
                    olds.add(c);
                    news.add("." + c);
                }
            }

            if (news.size() == 0)
            {
                return retval;
            }
            else
            {
                final RenameOperator rename = new RenameOperator(olds, news, meta);
                rename.add(retval);
                return rename;
            }
        }

        AbstractTableScanOperator op = null;
        try
        {
            // Determine if table is external
            if(1 == MetaData.getTypeForTable(schema, tblName, tx)) {
                ExternalTableType extTable = meta.getExternalTable(schema, tblName, tx);
                op = new ExternalTableScanOperator(extTable, schema, tblName, meta, tx);
            } else {
                op = new TableScanOperator(schema, tblName, meta, tx);
            }
        }
        catch (final Exception e)
        {
            throw new ParseException(e.getMessage());
        }

        if (table.getAlias() != null)
        {
            op.setAlias(table.getAlias());
        }

        return op;
    }

    Operator buildOperatorTreeFromSubSelect(final SubSelect select, final boolean subquery) throws Exception
    {
        Operator op = buildOperatorTreeFromFrom(select.getFrom(), select);
        getComplexColumns(select.getSelect(), select, select.getHaving());
        // HRDBMSWorker.logger.debug(complex);
        op = buildNGBExtends(op, select);
        // HRDBMSWorker.logger.debug("Did buildNGBExtends");
        // HRDBMSWorker.logger.debug(complex);
        if (select.getWhere() != null)
        {
            op = buildOperatorTreeFromWhere(select.getWhere(), op, select);
        }

        // handle groupBy
        if (updateGroupByNeeded(select))
        {
            op = buildOperatorTreeFromGroupBy(select.getGroupBy(), op, select);
        }

        // handle extends that haven't been done
        op = buildNGBExtends(op, select);
        // handle having
        if (select.getHaving() != null)
        {
            op = buildOperatorTreeFromHaving(select.getHaving(), op, select);
        }

        op = buildOperatorTreeFromSelectClause(select.getSelect(), op, select, subquery);

		/*
		 * for (String col : op.getCols2Pos().keySet()) { if
		 * (!col.contains(".")) { ok = false; badCol = col; break; } }
		 *
		 * if (!ok) { throw new Exception("Error processing the column " +
		 * badCol + " in the select clause. It does not contain a period."); }
		 */

        // handle orderBy
        if (select.getOrderBy() != null)
        {
            op = buildOperatorTreeFromOrderBy(select.getOrderBy(), op);
        }

        // handle fetchFirst
        if (select.getFetchFirst() != null)
        {
            op = buildOperatorTreeFromFetchFirst(select.getFetchFirst(), op);
        }
        return op;
    }

    private Operator buildOperatorTreeFromTableReference(final TableReference table, final SubSelect sub) throws Exception
    {
        if (table.isSingleTable())
        {
            return buildOperatorTreeFromSingleTable(table.getSingleTable());
        }

        if (table.isSelect())
        {
            final Operator op = buildOperatorTreeFromFullSelect(table.getSelect());
            if (table.getSelect().getCols() != null && table.getSelect().getCols().size() > 0)
            {
                // rename cols
                checkSizeOfNewCols(table.getSelect().getCols(), op);
                final ArrayList<String> original = new ArrayList<String>();
                final ArrayList<String> newCols = new ArrayList<String>();
                if (table.getAlias() == null)
                {
                    int i = 0;
                    for (final String col : op.getPos2Col().values())
                    {
                        original.add(col);
                        String newCol = col.substring(0, col.indexOf('.'));
                        String end = (table.getSelect().getCols().get(i)).getColumn();
                        if (!end.contains("."))
                        {
                            end = "." + end;
                        }

                        newCol += end;
                        newCols.add(newCol);
                        i++;
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
                else
                {
                    // rename cols and table
                    int i = 0;
                    for (final String col : op.getPos2Col().values())
                    {
                        original.add(col);
                        String newCol = table.getAlias();
                        String end = (table.getSelect().getCols().get(i)).getColumn();
                        if (!end.contains("."))
                        {
                            end = "." + end;
                        }

                        newCol += end;
                        newCols.add(newCol);
                        i++;
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
            }
            else
            {
                // check for need to rename table
                if (table.getAlias() != null)
                {
                    // rename table
                    final ArrayList<String> original = new ArrayList<String>();
                    final ArrayList<String> newCols = new ArrayList<String>();
                    for (final String col : op.getPos2Col().values())
                    {
                        original.add(col);
                        final String newCol = col.substring(col.indexOf('.') + 1);
                        newCols.add(table.getAlias() + "." + newCol);
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
                else
                {
                    return op;
                }
            }
        }

        final Operator op1 = buildOperatorTreeFromTableReference(table.getLHS(), sub);
        final Operator op2 = buildOperatorTreeFromTableReference(table.getRHS(), sub);
        final String op = table.getOp();

        if (op.equals("CP"))
        {
            try
            {
                final ProductOperator product = new ProductOperator(meta);
                product.add(op1);
                product.add(op2);
                if (table.getAlias() != null)
                {
                    return handleAlias(table.getAlias(), product, meta);
                }
                return product;
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }

        if (op.equals("I"))
        {
            try
            {
                final ProductOperator product = new ProductOperator(meta);
                product.add(op1);
                product.add(op2);
                SearchOperatorTrees searchOperatorTrees = new SearchOperatorTrees(connection, tx, meta, model);
                final Operator o = searchOperatorTrees.buildOperatorTreeFromSearchCondition(table.getSearch(), product, sub);
                if (table.getAlias() != null)
                {
                    return handleAlias(table.getAlias(), o, meta);
                }

                return o;
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
        }

        if (op.equals("L"))
        {
            // TODO
        }

        if (op.equals("R"))
        {
            // TODO
        }

        if (op.equals("F"))
        {
            // TODO
        }

        return null;
    }

    private void handleExpressions(Operator op, List<Expression> exps, List<String> buildList) throws Exception {
        ExpressionOperatorTrees expressionOperatorTrees = new ExpressionOperatorTrees(connection, tx, meta, model);
        for (final Expression exp : exps)
        {
            if (exp.isColumn())
            {
                buildList.add(exp.getColumn().getColumn());
                continue;
            }
            final SQLParser.OperatorTypeAndName otan = expressionOperatorTrees.buildOperatorTreeFromExpression(exp, null, null);
            if (otan.getType() != TYPE_INLINE && otan.getType() != SQLParser.TYPE_DATE)
            {
                throw new ParseException("Invalid expression in update statement");
            }

            if (otan.getType() == SQLParser.TYPE_DATE)
            {
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                final String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
                final String name = "._E" + model.getAndIncrementSuffix();
                final ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
                operator.add(op);
                op = operator;
                buildList.add(name);
            }
            else
            {
                final Operator operator = (Operator)otan.getOp();
                operator.add(op);
                op = operator;
                buildList.add(otan.getName());
            }
        }
    }

    ArrayList<Operator> buildOperatorTreeFromUpdate(final Update update) throws Exception
    {
        final TableName table = update.getTable();
        String schema = null;
        String tbl = null;
        if (table.getSchema() == null)
        {
            schema = new MetaData(connection).getCurrentSchema();
            tbl = table.getName();
        }
        else
        {
            schema = table.getSchema();
            tbl = table.getName();
        }

        if (!MetaData.verifyTableExistence(schema, tbl, tx))
        {
            throw new ParseException("Table does not exist");
        }

        TableScanOperator scan = new TableScanOperator(schema, tbl, meta, tx);

        if (!update.isMulti())
        {
            Operator op = null;
            if (update.getWhere() != null)
            {
                op = buildOperatorTreeFromWhere(update.getWhere(), scan, null);
            }
            else
            {
                op = scan;
            }
            scan.getRID();
            Operator op2;
            if (op != scan)
            {
                op2 = scan.firstParent();
                op2.removeChild(scan);
                op2.add(scan);
                while (op2 != op)
                {
                    final Operator op3 = op2.parent();
                    op3.removeChild(op2);
                    op3.add(op2);
                    op2 = op3;
                }
            }

            final ArrayList<String> buildList = new ArrayList<String>();

            ArrayList<Expression> exps = null;
            if (update.getExpression().isList())
            {
                exps = update.getExpression().getList();
            }
            else
            {
                exps = new ArrayList<Expression>();
                exps.add(update.getExpression());
            }

            handleExpressions(op, exps, buildList);

            if (!MetaData.verifyUpdate(schema, tbl, update.getCols(), buildList, op, tx))
            {
                throw new ParseException("The number of columns and/or data types do not match the columns being updated");
            }

            final ArrayList<String> cols = new ArrayList<String>();
            for (final String col : op.getPos2Col().values())
            {
                cols.add(col);
            }

            final ReorderOperator reorder = new ReorderOperator(cols, meta);
            reorder.add(op);
            op = reorder;
            final RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
            retval.add(op);
            final Phase1 p1 = new Phase1(retval, tx);
            p1.optimize();
            new Phase2(retval, tx).optimize();
            new Phase3(retval, tx).optimize();
            new Phase4(retval, tx).optimize();
            new Phase5(retval, tx, p1.likelihoodCache).optimize();
            final UpdateOperator uOp = new UpdateOperator(schema, tbl, update.getCols(), buildList, meta);
            final Operator child = retval.children().get(0);
            retval.removeChild(child);
            uOp.add(child);
            final ArrayList<Operator> uOps = new ArrayList<Operator>(1);
            uOps.add(uOp);
            return uOps;
        }
        else
        {
            final TableScanOperator master = scan;
            final ArrayList<ArrayList<Column>> cols2 = update.getCols2();
            final ArrayList<Expression> exps2 = update.getExps2();
            final ArrayList<Where> wheres2 = update.getWheres2();
            int i = 0;
            final ArrayList<Operator> uOps = new ArrayList<Operator>();
            for (final ArrayList<Column> cols : cols2)
            {
                scan = master.clone();
                Operator op = null;
                if (wheres2.get(i) != null)
                {
                    op = buildOperatorTreeFromWhere(wheres2.get(i), scan, null);
                }
                else
                {
                    op = scan;
                }
                scan.getRID();
                Operator op2;
                if (op != scan)
                {
                    op2 = scan.firstParent();
                    op2.removeChild(scan);
                    op2.add(scan);
                    while (op2 != op)
                    {
                        final Operator op3 = op2.parent();
                        op3.removeChild(op2);
                        op3.add(op2);
                        op2 = op3;
                    }
                }

                final ArrayList<String> buildList = new ArrayList<String>();

                ArrayList<Expression> exps = null;
                if (exps2.get(i).isList())
                {
                    exps = exps2.get(i).getList();
                }
                else
                {
                    exps = new ArrayList<Expression>();
                    exps.add(exps2.get(i));
                }
                handleExpressions(op, exps, buildList);

                if (!MetaData.verifyUpdate(schema, tbl, cols, buildList, op, tx))
                {
                    throw new ParseException("The number of columns and/or data types do not match the columns being updated");
                }

                final ArrayList<String> cols3 = new ArrayList<String>();
                for (final String col : op.getPos2Col().values())
                {
                    cols3.add(col);
                }

                final ReorderOperator reorder = new ReorderOperator(cols3, meta);
                reorder.add(op);
                op = reorder;
                final RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
                retval.add(op);
                final Phase1 p1 = new Phase1(retval, tx);
                p1.optimize();
                new Phase2(retval, tx).optimize();
                new Phase3(retval, tx).optimize();
                new Phase4(retval, tx).optimize();
                new Phase5(retval, tx, p1.likelihoodCache).optimize();
                final UpdateOperator uOp = new UpdateOperator(schema, tbl, cols, buildList, meta);
                final Operator child = retval.children().get(0);
                retval.removeChild(child);
                uOp.add(child);
                uOps.add(uOp);
                i++;
            }

            return uOps;
        }
    }

    private Operator buildOperatorTreeFromWhere(final Where where, final Operator op, final SubSelect sub) throws Exception
    {
        return new SearchOperatorTrees(connection, tx, meta, model).buildOperatorTreeFromSearchCondition(where.getSearch(), op, sub);
    }

    private static void importCTE(final CTE cte, final FullSelect select)
    {
        final String name = cte.getName();
        final ArrayList<Column> cols = cte.getCols();
        final FullSelect cteSelect = cte.getSelect();

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

    Operator addComplexColumn(final ArrayList<Object> row, Operator op, final SubSelect sub) throws Exception
    {
        // colName, op, type, id, exp, prereq, done
        if (!((Integer)row.get(5)).equals(-1))
        {
            // get the row
            for (final ArrayList<Object> r : model.getComplex())
            {
                if (r.get(3).equals(row.get(5)))
                {
                    op = addComplexColumn(r, op, sub);
                    break;
                }
            }
        }

        if ((Boolean)row.get(7) == true)
        {
            return op;
        }

        if ((Integer)row.get(2) == TYPE_INLINE)
        {
            final Operator o = (Operator)row.get(1);

            if(checkForCaseOperator(o, row, op, sub)) {
                return o;
            }
            try
            {
                o.add(op);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
            row.remove(7);
            row.add(true);
            if (o instanceof YearOperator)
            {
                final String name = o.getReferences().get(0);
                final String type = op.getCols2Types().get(name);

                if (type == null)
                {
                    throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                }

                if (!type.equals("DATE"))
                {
                    throw new ParseException("The YEAR() function cannot be used on a non-DATE column");
                }
            }
            else if (o instanceof SubstringOperator)
            {
                final String name = o.getReferences().get(0);
                final String type = op.getCols2Types().get(name);

                if (type == null)
                {
                    throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                }

                if (!type.equals("CHAR"))
                {
                    throw new ParseException("The SUBSTRING() function cannot be used on a non-character data");
                }
            }
            else if (o instanceof DateMathOperator)
            {
                final String name = o.getReferences().get(0);
                final String type = op.getCols2Types().get(name);

                if (type == null)
                {
                    throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                }

                if (!type.equals("DATE"))
                {
                    throw new ParseException("A DATE was expected but a different data type was found");
                }
            }
            else if (o instanceof ConcatOperator)
            {
                resolveConcat(o, op);
            }
            else if (o instanceof ExtendOperator)
            {
                for (final String name : o.getReferences())
                {
                    final String type = op.getCols2Types().get(name);

                    if (type == null)
                    {
                        HRDBMSWorker.logger.debug("Looking for " + name + " in " + op.getCols2Types());
                        throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                    }

                    if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
                    {
                    }
                    else
                    {
                        throw new ParseException("A numeric data type was expected but a different data type was found");
                    }
                }
            }

            return o;
        }
        else
        {
            // type group by
            throw new ParseException("A WHERE clause cannot refer to an aggregate column. This must be done in the HAVING clause.");
        }
    }

    private Object buildNGBExtend(Operator op, final ArrayList<Object> row, final SubSelect sub) throws Exception
    {
        // colName, op, type, id, exp, prereq, done
        if (!(row.get(5)).equals(-1))
        {
            // get the row
            for (final ArrayList<Object> r : model.getComplex())
            {
                if (r.get(3).equals(row.get(5)))
                {
                    try
                    {
                        final Object o = addComplexColumn(r, op, sub);
                        op = (Operator)o;
                        break;
                    }
                    catch (final Exception e)
                    {
                        if (row.get(1) instanceof Operator && !(row.get(1) instanceof CaseOperator) && allReferencesSatisfied(((Operator)row.get(1)).getReferences(), op))
                        {
                            break;
                        }
                        else if (row.get(1) instanceof CaseOperator)
                        {
                            if (allReferencesSatisfied(((CaseOperator)row.get(1)).getReferences(), op))
                            {
                                final ArrayList<String> references = new ArrayList<>();
                                final Expression exp = (Expression)row.get(4);
                                final ArrayList<Case> cases = exp.getCases();
                                for (final Case c : cases)
                                {
                                    references.addAll(getReferences(c.getCondition()));
                                }

                                if (allReferencesSatisfied(references, op))
                                {
                                    break;
                                }
                            }
                        }

                        return null;
                    }
                }
            }
        }

        if ((Boolean)row.get(7) == true)
        {
            return op;
        }

        if ((Integer)row.get(2) == TYPE_INLINE)
        {
            final Operator o = (Operator)row.get(1);
            if(checkForCaseOperator(o, row, op, sub)) {
                return o;
            }
            try
            {
                o.add(op);
            }
            catch (final Exception e)
            {
                HRDBMSWorker.logger.debug("", e);
                throw new ParseException(e.getMessage());
            }
            row.remove(7);
            row.add(true);
            if (o instanceof YearOperator)
            {
                final String name = o.getReferences().get(0);
                final String type = op.getCols2Types().get(name);

                if (type == null)
                {
                    throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                }

                if (!type.equals("DATE"))
                {
                    throw new ParseException("The YEAR() function cannot be used on a non-DATE column");
                }
            }
            else if (o instanceof SubstringOperator)
            {
                final String name = o.getReferences().get(0);
                final String type = op.getCols2Types().get(name);

                if (type == null)
                {
                    throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                }

                if (!type.equals("CHAR"))
                {
                    throw new ParseException("The SUBSTRING() function cannot be used on a non-character data");
                }
            }
            else if (o instanceof DateMathOperator)
            {
                final String name = o.getReferences().get(0);
                final String type = op.getCols2Types().get(name);

                if (type == null)
                {
                    throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                }

                if (!type.equals("DATE"))
                {
                    throw new ParseException("A DATE was expected but a different data type was found");
                }
            }
            else if (o instanceof ConcatOperator)
            {
                resolveConcat(o, op);
            }
            else if (o instanceof ExtendOperator)
            {
                for (final String name : o.getReferences())
                {
                    final String type = op.getCols2Types().get(name);

                    if (type == null)
                    {
                        HRDBMSWorker.logger.debug("Could not find " + name + " in " + op.getCols2Types());
                        throw new ParseException("A reference to column " + name + " was unable to be resolved.");
                    }

                    if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
                    {
                    }
                    else
                    {
                        throw new ParseException("A numeric data type was expected but a different data type was found");
                    }
                }
            }

            return o;
        }
        else
        {
            // type group by
            return false;
        }
    }

    private Operator buildNGBExtends(Operator op, final SubSelect sub) throws Exception
    {
        for (final ArrayList<Object> row : model.getComplex())
        {
            if ((Boolean)row.get(7) == false && (Integer)row.get(2) != TYPE_GROUPBY && (row.get(6) == null || (row.get(6)).equals(sub)))
            {
                final Object o = buildNGBExtend(op, row, sub);
                if (o != null && !(o instanceof Boolean))
                {
                    op = (Operator)o;
                }
            }
        }

        return op;
    }

    private void getComplexColumns(final SelectClause select, final SubSelect sub, final Having having) throws ParseException
    {
        final ArrayList<SelectListEntry> selects = select.getSelectList();
        for (final SelectListEntry s : selects)
        {
            if (!s.isColumn())
            {
                // complex column
                ExpressionOperatorTrees expressionOperatorTrees = new ExpressionOperatorTrees(connection, tx, meta, model);
                final Expression exp = s.getExpression();
                final SQLParser.OperatorTypeAndName op = expressionOperatorTrees.buildOperatorTreeFromExpression(exp, s.getName(), sub);
                if (op.getType() == TYPE_DAYS)
                {
                    throw new ParseException("A type of DAYS is not allowed for a column in a select list");
                }

                if (op.getType() == TYPE_MONTHS)
                {
                    throw new ParseException("A type of MONTHS is not allowed for a column in a select list");
                }

                if (op.getType() == TYPE_YEARS)
                {
                    throw new ParseException("A type of YEARS is not allowed for a column in a select list");
                }

                if (op.getType() == TYPE_DATE)
                {
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    final String dateString = sdf.format(((GregorianCalendar)op.getOp()).getTime());

                    if (s.getName() != null)
                    {
                        String sgetName = s.getName();
                        if (!sgetName.contains("."))
                        {
                            sgetName = "." + sgetName;
                        }

                        final ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), sgetName, meta);
                        final ArrayList<Object> row = new ArrayList<Object>();
                        row.add(sgetName);
                        row.add(operator);
                        row.add(TYPE_INLINE);
                        row.add(model.getAndIncrementComplexId());
                        row.add(exp);
                        row.add(-1);
                        row.add(sub);
                        row.add(false);
                        model.getComplex().add(row);
                    }
                    else
                    {
                        final String name = "._E" + model.getAndIncrementSuffix();
                        final ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
                        final ArrayList<Object> row = new ArrayList<Object>();
                        row.add(name);
                        row.add(operator);
                        row.add(TYPE_INLINE);
                        row.add(model.getAndIncrementComplexId());
                        row.add(exp);
                        row.add(-1);
                        row.add(sub);
                        row.add(false);
                        model.getComplex().add(row);
                    }
                }
                else
                {
                    // colName, op, type, id, exp, prereq, sub, done
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(op.getName());
                    row.add(op.getOp());
                    row.add(op.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(exp);
                    row.add(op.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                }
            }
        }

        if (having != null)
        {
            final SearchCondition sc = having.getSearch();
            processHavingSC(sc, sub);
        }
    }

    private void processHavingExpression(final Expression e, final SubSelect sub) throws ParseException
    {
        ExpressionOperatorTrees expressionOperatorTrees = new ExpressionOperatorTrees(connection, tx, meta, model);
        if (e.isCountStar())
        {
            // see if complex has a row for this expression, if not add one
            boolean ok = false;
            for (final ArrayList<Object> row : model.getComplex())
            {
                if (row.get(4).equals(e) && row.get(6).equals(sub))
                {
                    ok = true;
                    break;
                }
            }

            if (!ok)
            {
                // add it
                final SQLParser.OperatorTypeAndName op = expressionOperatorTrees.buildOperatorTreeFromExpression(e, null, sub);
                final ArrayList<Object> row = new ArrayList<Object>();
                row.add(op.getName());
                row.add(op.getOp());
                row.add(op.getType());
                row.add(model.getAndIncrementComplexId());
                row.add(e);
                row.add(op.getPrereq());
                row.add(sub);
                row.add(false);
                model.getComplex().add(row);
            }
        }
        else if (e.isExpression())
        {
            Expression e2 = e.getLHS();
            processHavingExpression(e2, sub);
            e2 = e.getRHS();
            processHavingExpression(e2, sub);
        }
        else if (e.isFunction())
        {
            final Function f = e.getFunction();
            final String name = f.getName();
            if (name.equals("AVG") || name.equals("SUM") || name.equals("COUNT") || name.equals("MAX") || name.equals("MIN"))
            {
                // see if complex has a row for this expression, if not add one
                boolean ok = false;
                for (final ArrayList<Object> row : model.getComplex())
                {
                    if (row.get(4).equals(e) && row.get(6).equals(sub))
                    {
                        ok = true;
                        break;
                    }
                }

                if (!ok)
                {
                    // add it
                    final SQLParser.OperatorTypeAndName op = expressionOperatorTrees.buildOperatorTreeFromExpression(e, null, sub);
                    final ArrayList<Object> row = new ArrayList<Object>();
                    row.add(op.getName());
                    row.add(op.getOp());
                    row.add(op.getType());
                    row.add(model.getAndIncrementComplexId());
                    row.add(e);
                    row.add(op.getPrereq());
                    row.add(sub);
                    row.add(false);
                    model.getComplex().add(row);
                }
            }
            else
            {
                final ArrayList<Expression> args = f.getArgs();
                if (args != null && args.size() > 0)
                {
                    for (final Expression arg : args)
                    {
                        processHavingExpression(arg, sub);
                    }
                }
            }
        }
        else if (e.isList())
        {
            final ArrayList<Expression> list = e.getList();
            for (final Expression e2 : list)
            {
                processHavingExpression(e2, sub);
            }
        }
        else if (e.isCase())
        {
            final ArrayList<Case> cases = e.getCases();
            for (final Case c : cases)
            {
                processHavingExpression(c.getResult(), sub);
                processHavingSC(c.getCondition(), sub);
            }

            processHavingExpression(e.getDefault(), sub);
        }
    }

    private void processHavingPredicate(final Predicate p, final SubSelect sub) throws ParseException
    {
        final Expression l = p.getLHS();
        final Expression r = p.getRHS();
        if (l != null)
        {
            processHavingExpression(l, sub);
        }

        if (r != null)
        {
            processHavingExpression(r, sub);
        }
    }

    private void processHavingSC(final SearchCondition sc, final SubSelect sub) throws ParseException
    {
        final SearchClause search = sc.getClause();
        if (search.getPredicate() != null)
        {
            processHavingPredicate(search.getPredicate(), sub);
        }
        else
        {
            processHavingSC(search.getSearch(), sub);
        }

        if (sc.getConnected() != null && sc.getConnected().size() > 0)
        {
            for (final ConnectedSearchClause search2 : sc.getConnected())
            {
                final SearchClause search3 = search2.getSearch();
                if (search3.getPredicate() != null)
                {
                    processHavingPredicate(search3.getPredicate(), sub);
                }
                else
                {
                    processHavingSC(search3.getSearch(), sub);
                }
            }
        }
    }

    private boolean checkForCaseOperator(Operator o, final ArrayList<Object> row, Operator op, final SubSelect sub) throws Exception {
        if (o instanceof CaseOperator)
        {
            SearchOperatorTrees searchOperatorTrees = new SearchOperatorTrees(connection, tx, meta, model);
            final Expression exp = (Expression)row.get(4);
            final ArrayList<HashSet<HashMap<Filter, Filter>>> alhshm = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
            // HRDBMSWorker.logger.debug("Build SC has " +
            // exp.getCases().size() + " cases to examine");
            for (final Case c : exp.getCases())
            {
                final Operator top = op;
                op = searchOperatorTrees.buildOperatorTreeFromSearchCondition(c.getCondition(), top, sub);
                if (op == top)
                {
                    HRDBMSWorker.logger.debug("Build SC when building a CaseOperator did not do anything");
                }
                else if (!(op instanceof SelectOperator))
                {
                    HRDBMSWorker.logger.debug("Build SC when building a CaseOperator did something, but it wasn't a SelectOperator");
                }
                // add filters to alhshm and remove from tree
                final HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
                while (op instanceof SelectOperator)
                {
                    final HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
                    for (final Filter f : ((SelectOperator)op).getFilter())
                    {
                        hm.put(f, f);
                    }

                    hshm.add(hm);
                    op = op.children().get(0);
                    op.parent().removeChild(op);
                }

                final Operator newTop = op;
                while (op != top)
                {
                    if (op.children().size() > 1)
                    {
                        throw new ParseException("Invalid search condition in a case expression");
                    }

                    if (op instanceof SelectOperator)
                    {
                        throw new ParseException("Internal error: stranded select operator when processing a case expression");
                    }

                    op = op.children().get(0);
                }

                op = newTop;
                alhshm.add(hshm);
            }

            ((CaseOperator)o).setFilters(alhshm);
            String type = ((CaseOperator)o).getType();
            final ArrayList<Object> results = ((CaseOperator)o).getResults();
            for (final Object o2 : results)
            {
                if (o2 instanceof String)
                {
                    if (((String)o2).startsWith("\u0000"))
                    {
                        final String newType = getType(((String)o2).substring(1), op.getCols2Types());
                        if (type == null)
                        {
                            type = newType;
                        }
                        else if (type.equals("INT") && !newType.equals("INT"))
                        {
                            if (newType.equals("LONG") || newType.equals("FLOAT"))
                            {
                                type = newType;
                            }
                            else
                            {
                                throw new ParseException("All possible results in a case expression must have the same type");
                            }
                        }
                        else if (type.equals("LONG") && !newType.equals("LONG"))
                        {
                            if (newType.equals("INT"))
                            {
                            }
                            else if (newType.equals("FLOAT"))
                            {
                                type = newType;
                            }
                            else
                            {
                                throw new ParseException("All possible results in a case expression must have the same type");
                            }
                        }
                        else if (type.equals("FLOAT") && !newType.equals("FLOAT"))
                        {
                            if (newType.equals("INT") || newType.equals("LONG"))
                            {
                            }
                            else
                            {
                                throw new ParseException("All possible results in a case expression must have the same type");
                            }
                        }
                        else if (!type.equals(newType))
                        {
                            throw new ParseException("All possible results in a case expression must have the same type");
                        }
                    }
                }
            }
            ((CaseOperator)o).setType(type);
            try
            {
                o.add(op);
            }
            catch (final Exception e)
            {
                throw new ParseException(e.getMessage());
            }
            row.remove(7);
            row.add(true);
            return true;
        }
        return false;
    }
}