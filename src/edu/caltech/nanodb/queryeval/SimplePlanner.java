package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.plannodes.*;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.AggregateExpressionProcessor;

import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.AggregateFunction;
import java.util.Map;

import edu.caltech.nanodb.relations.TableInfo;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    private NestedLoopJoinNode makeJoinPlan(FromClause fromClause) throws IOException {
        if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.JOIN_EXPR
                && fromClause.getRightChild().getClauseType() == FromClause.ClauseType.JOIN_EXPR) {

            return new NestedLoopJoinNode(makeJoinPlan(fromClause.getLeftChild()),
                    makeJoinPlan(fromClause.getRightChild()), fromClause.getJoinType(),
                    fromClause.getOnExpression());
        } else if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.JOIN_EXPR
                && fromClause.getRightChild().getClauseType() != FromClause.ClauseType.JOIN_EXPR) {

            if (fromClause.getRightChild().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
                TableInfo tableInfo = storageManager.getTableManager()
                        .openTable(fromClause.getRightChild().getTableName());
                FileScanNode fileScanNode = new FileScanNode(tableInfo, null);
                return new NestedLoopJoinNode(makeJoinPlan(fromClause.getLeftChild()),
                        fileScanNode, fromClause.getJoinType(),
                        fromClause.getOnExpression());

            }
            if (fromClause.getRightChild().getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY){

                SelectClause fromSelClause = fromClause.getRightChild().getSelectClause();
                RenameNode fromSelNode = new RenameNode(makePlan(fromSelClause, null),
                        fromClause.getResultName());
                return new NestedLoopJoinNode(makeJoinPlan(fromClause.getLeftChild()),
                        fromSelNode, fromClause.getJoinType(),
                        fromClause.getOnExpression());

            }
        } else if (fromClause.getLeftChild().getClauseType() != FromClause.ClauseType.JOIN_EXPR
                && fromClause.getRightChild().getClauseType() == FromClause.ClauseType.JOIN_EXPR){

            if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
                TableInfo tableInfo = storageManager.getTableManager()
                        .openTable(fromClause.getLeftChild().getTableName());
                FileScanNode fileScanNode = new FileScanNode(tableInfo, null);
                return new NestedLoopJoinNode(fileScanNode, makeJoinPlan(fromClause.getRightChild()),
                        fromClause.getJoinType(),
                        fromClause.getOnExpression());
            }
            if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY){

                SelectClause fromSelClause = fromClause.getLeftChild().getSelectClause();
                RenameNode fromSelNode = new RenameNode(makePlan(fromSelClause, null),
                        fromClause.getResultName());
                return new NestedLoopJoinNode(fromSelNode, makeJoinPlan(fromClause.getRightChild()),
                        fromClause.getJoinType(), fromClause.getOnExpression());

            }
        } else { // Both not join_expressions
            if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.BASE_TABLE &&
                    fromClause.getRightChild().getClauseType() == FromClause.ClauseType.BASE_TABLE) {
                TableInfo tableInfoL = storageManager.getTableManager()
                        .openTable(fromClause.getLeftChild().getTableName());
                TableInfo tableInfoR = storageManager.getTableManager()
                        .openTable(fromClause.getRightChild().getTableName());
                FileScanNode fileScanNodeL = new FileScanNode(tableInfoL, null);
                FileScanNode fileScanNodeR = new FileScanNode(tableInfoR, null);
                return new NestedLoopJoinNode(fileScanNodeL, fileScanNodeR,
                        fromClause.getJoinType(),
                        fromClause.getOnExpression());
            } else if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.BASE_TABLE &&
                    fromClause.getRightChild().getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY){
                TableInfo tableInfoL = storageManager.getTableManager()
                        .openTable(fromClause.getLeftChild().getTableName());
                SelectClause fromSelClause = fromClause.getRightChild().getSelectClause();
                RenameNode fromSelNode = new RenameNode(makePlan(fromSelClause, null),
                        fromClause.getResultName());
                FileScanNode fileScanNodeL = new FileScanNode(tableInfoL, null);
                return new NestedLoopJoinNode(fileScanNodeL, fromSelNode,
                        fromClause.getJoinType(),
                        fromClause.getOnExpression());
            } else if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY &&
                    fromClause.getRightChild().getClauseType() == FromClause.ClauseType.BASE_TABLE){
                SelectClause fromSelClause = fromClause.getLeftChild().getSelectClause();
                RenameNode fromSelNode = new RenameNode(makePlan(fromSelClause, null),
                        fromClause.getResultName());
                TableInfo tableInfoR = storageManager.getTableManager()
                        .openTable(fromClause.getRightChild().getTableName());
                FileScanNode fileScanNodeR = new FileScanNode(tableInfoR, null);
                return new NestedLoopJoinNode(fromSelNode, fileScanNodeR,
                        fromClause.getJoinType(),
                        fromClause.getOnExpression());
            } else if (fromClause.getLeftChild().getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY &&
                    fromClause.getRightChild().getClauseType() == FromClause.ClauseType.SELECT_SUBQUERY){
                SelectClause fromSelClauseL = fromClause.getLeftChild().getSelectClause();
                RenameNode fromSelNodeL = new RenameNode(makePlan(fromSelClauseL, null),
                        fromClause.getResultName());
                SelectClause fromSelClauseR = fromClause.getRightChild().getSelectClause();
                RenameNode fromSelNodeR = new RenameNode(makePlan(fromSelClauseR, null),
                        fromClause.getResultName());
                return new NestedLoopJoinNode(fromSelNodeL, fromSelNodeR,
                        fromClause.getJoinType(),
                        fromClause.getOnExpression());
            }
            }
        return null;
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) throws IOException {

        // For HW1, we have a very simple implementation that defers to
        // makeSimpleSelect() to handle simple SELECT queries with one table,
        // and an optional WHERE clause.

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }
        AggregateExpressionProcessor processor = new AggregateExpressionProcessor();
        processor.setErrorCheck(1);
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            Expression new_exp = whereExpr.traverse(processor);
        }

        Expression onExpr = selClause.getFromClause().getOnExpression();
        if (onExpr != null) {
            Expression new_exp = onExpr.traverse(processor);
        }

        processor.setErrorCheck(0);
        for (SelectValue sv : selClause.getSelectValues()) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;
            Expression e = sv.getExpression();
            Expression new_exp = sv.getExpression().traverse(processor);
            sv.setExpression(new_exp);

        }
        Expression havingExpr = selClause.getHavingExpr();
        if (havingExpr != null) {
            Expression new_exp = havingExpr.traverse(processor);
            selClause.setHavingExpr(new_exp);
        }

        FromClause fromClause = selClause.getFromClause();
        if (fromClause == null) {
            ProjectNode projNode = new ProjectNode(selClause.getSelectValues());
            return projNode;
        }
        if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR){
            NestedLoopJoinNode joinNode = makeJoinPlan(fromClause);
            if (!selClause.isTrivialProject()) {
                TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getTableName());
                ProjectNode projNode;
                if (processor.getAggFunct() != null) {
                    HashedGroupAggregateNode aggregateNode = new HashedGroupAggregateNode(joinNode,
                            selClause.getGroupByExprs(),processor.getAggFunct());
                    projNode = new ProjectNode(aggregateNode, selClause.getSelectValues());
                }
                else {
                    projNode = new ProjectNode(joinNode, selClause.getSelectValues());
                }
                return projNode;
            } else {
                SimpleFilterNode whereNode = new SimpleFilterNode(joinNode,
                        selClause.getWhereExpr());
                if (processor.getAggFunct() != null) {
                    HashedGroupAggregateNode aggregateNode = new HashedGroupAggregateNode(whereNode,
                            selClause.getGroupByExprs(),processor.getAggFunct());
                    if(selClause.getHavingExpr() != null) {
                        SimpleFilterNode havingNode = new SimpleFilterNode(aggregateNode, selClause.getHavingExpr());
                        return havingNode;
                    }
                    return aggregateNode;
                }
                return whereNode;
            }
        }

        if (fromClause.isBaseTable()) {

            if (!selClause.isTrivialProject()) {
                TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getTableName());
                FileScanNode fileScanNode = new FileScanNode(tableInfo, selClause.getWhereExpr());
                ProjectNode projNode;
                if (processor.getAggFunct() != null) {
                    HashedGroupAggregateNode aggregateNode = new HashedGroupAggregateNode(fileScanNode,
                            selClause.getGroupByExprs(),processor.getAggFunct());
                    projNode = new ProjectNode(aggregateNode, selClause.getSelectValues());
                }
                else {
                    projNode = new ProjectNode(fileScanNode, selClause.getSelectValues());
                }
                return projNode;
            } else {
                SelectNode selectNode = makeSimpleSelect(fromClause.getTableName(),
                        selClause.getWhereExpr(), null);
                if (processor.getAggFunct() != null) {
                    HashedGroupAggregateNode aggregateNode = new HashedGroupAggregateNode(selectNode,
                            selClause.getGroupByExprs(),processor.getAggFunct());
                    if(selClause.getHavingExpr() != null) {
                        SimpleFilterNode havingNode = new SimpleFilterNode(aggregateNode, selClause.getHavingExpr());
                        return havingNode;
                    }
                    return aggregateNode;
                }
                return selectNode;
            }
        }
        if (fromClause.getSelectClause() != null){

                SelectClause fromSelClause = fromClause.getSelectClause();
                RenameNode fromSelNode = new RenameNode(makePlan(fromSelClause, null),
                        fromClause.getResultName());
                SimpleFilterNode whereNode = new SimpleFilterNode(fromSelNode,
                        selClause.getWhereExpr());
                if (!selClause.isTrivialProject()) {
                    ProjectNode projNode;
                    if (processor.getAggFunct() != null) {
                        HashedGroupAggregateNode aggregateNode = new HashedGroupAggregateNode(whereNode,
                                selClause.getGroupByExprs(),processor.getAggFunct());
                        projNode = new ProjectNode(aggregateNode, selClause.getSelectValues());
                    }
                    else {
                        projNode = new ProjectNode(whereNode, selClause.getSelectValues());
                    }
                    return projNode;
                } else {
                    if (processor.getAggFunct() != null) {
                        HashedGroupAggregateNode aggregateNode = new HashedGroupAggregateNode(whereNode,
                                selClause.getGroupByExprs(),processor.getAggFunct());
                        if(selClause.getHavingExpr() != null) {
                            SimpleFilterNode havingNode = new SimpleFilterNode(aggregateNode,
                                    selClause.getHavingExpr());
                            return havingNode;
                        }
                        return aggregateNode;
                    }
                    return whereNode;
                }
        }
            throw new UnsupportedOperationException(
                    "Not implemented:  joins or subqueries in FROM clause");

    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                    "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}


