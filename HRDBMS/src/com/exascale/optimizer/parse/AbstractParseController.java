package com.exascale.optimizer.parse;

import com.exascale.optimizer.MetaData;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

/** Base class of controlling code for parsing SQL */
public abstract class AbstractParseController {
    protected ConnectionWorker connection;
    protected Transaction tx;
    protected MetaData meta;
    protected SQLParser.Model model;

    /** Initialize with information that's needed to build a plan for any configuration of operators */
    public AbstractParseController(ConnectionWorker connection, Transaction tx, MetaData meta, SQLParser.Model model) {
        this.connection = connection;
        this.tx = tx;
        this.meta = meta;
        this.model = model;
    }
}
