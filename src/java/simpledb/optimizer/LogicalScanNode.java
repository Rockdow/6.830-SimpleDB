package simpledb.optimizer;

import simpledb.common.Catalog;

/** A LogicalScanNode represents table in the FROM list in a
 * LogicalQueryPlan */
// 该类是存储 查询的表别名 以及 表ID
public class LogicalScanNode {

    /** The name (alias) of the table as it is used in the query */
    public final String alias;

    /** The table identifier (can be passed to {@link Catalog#getDatabaseFile})
     *   to retrieve a DbFile */
    public final int t;

    public LogicalScanNode(int table, String tableAlias) {
        this.alias = tableAlias;
        this.t = table;
    }
}

