package edu.caltech.nanodb.queryeval;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;
import edu.caltech.nanodb.queryeval.CostBasedJoinPlanner;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;

/**
 * This SubqueryPlanner implementation can traverse all relavant expressions in
 * a SelectClause, find the SubqueryOperator expressions, and fill their plan nodes. 
 *
 * 
 */
public class SubqueryPlanner {


    /**
     * The SubqueryPlannerExpressionProcessor is used to add SubqueryOperator
     * expressions to the external expressions_to_plan list. It is also used
     * to check (for order and group by clauses) if there are any SubqueryOperators.
     */
    public class SubqueryPlannerExpressionProcessor implements ExpressionProcessor {
        private int errorCheck = 0;

        public void enter(Expression e){};
        public void setErrorCheck (int err){
            errorCheck = err;
        }

        public Expression leave(Expression e) {
            // This function never changes the node that is traversed.

            if(e instanceof SubqueryOperator) {
                if (errorCheck == 1){
                    throw new IllegalArgumentException("Cannot contain subqueries in ORDER BY/GROUP BY clauses");
                }
                expressions_to_plan.add(e);
            }


            return e;
        }
    }


    public static class DecorrelationExpressionProcessor implements ExpressionProcessor {
        /* This is the set of the names of correlated columns within the entire WHERE
         * expression being processed. These values are ignored in this processor. */
        private Set<ColumnName> correlatedNames;

        /* The name of the base table of the subquery being decorrelated */
        private String tableName;

        public void enter(Expression e){};


        public void setCorrelatedColumnsSet (Set<ColumnName> columnsSet){
            correlatedNames = columnsSet;
        }

        public void setTableName (String inputTableName){
            tableName = inputTableName;
        }

        public Expression leave(Expression e) {
            // This function never changes the node that is traversed.
            if (e instanceof ColumnValue){
                ColumnValue colV = (ColumnValue) e;
                String prevName = colV.getColumnName().getColumnName();
                // Adds tableName to columnValue object's columnName value, to avoid
                // errors from an uninitialized table name.
                if (!correlatedNames.contains(colV.getColumnName())){
                    ColumnName colN = new ColumnName(tableName, prevName);
                    colV.setColumnName(colN);
                }
                Expression finalExp = colV;
                return finalExp;
            }
            return e;
        }
    }

    /**
     * Since ExpressionProcessor cannot call planSubqueryOperatorExpression,
     * we store the expressions in this list and process later, inside the
     * subquery planner itself.
     */
    private List<Expression> expressions_to_plan;

    /**
     * This field specifies the planner used by the Subquery Planner to plan
     * subqueries.
     */
    private Planner parentPlanner;

    /**
     * This field specifies the parent environment of all subqueries planned
     * in an expression. It is added as a parent environment to each node in
     * the subquery's parentNode's plan tree.
     */
    private Environment subqueryEnvironment;

    /**
     * Constructor that takes a Planner as an argument.
     * 
     * @param parentPlanner a Planner object. Should be the Planner 
     *        that creates this instance of SubqueryPlanner
     *
     */ 
    public SubqueryPlanner(Planner parentPlanner) {
        this.parentPlanner = parentPlanner;
        expressions_to_plan = new ArrayList<Expression>();
        this.subqueryEnvironment = new Environment();
    }

    /**
     * Resets the relevant fields in preperation to handle a new
     * subquery.
     */
    public void reset() {
        expressions_to_plan = new ArrayList<Expression>();
        this.subqueryEnvironment = new Environment();
    }

    /**
     * Takes an Expression and if it is a SubqueryOperator type, create its
     * subqueryPlan. Correctly organizes environments to support correlated
     * subqueries.
     *
     * @param e an expression
     *
     * @param enclosingSelects a list of enclosing select clauses that this subquery
     * resides inside of. Is passed to subsequent makePlan function.
     *
     * @param parentNode the PlanNode from which this subquery processes its tuples,
     * helps set up environments for correlated evaluation.
     *
     * @return Nothing
     *
     * @throws
     */
    private void planSubqueryOperatorExpression(Expression e, List<SelectClause> enclosingSelects, PlanNode parentNode)
            throws IOException {
        if (e instanceof ScalarSubquery) {
            ScalarSubquery sub = (ScalarSubquery) e;
            PlanNode subqueryNode = parentPlanner.makePlan(sub.getSubquery(), enclosingSelects);
            subqueryNode.addParentEnvironmentToPlanTree(parentNode.getEnvironment());
            subqueryNode.addParentEnvironmentToPlanTree(subqueryEnvironment);
            subqueryNode.prepare();
            sub.setSubqueryPlan(subqueryNode);
        }
        if (e instanceof InSubqueryOperator){
            InSubqueryOperator sub = (InSubqueryOperator) e;
            PlanNode subqueryNode = parentPlanner.makePlan(sub.getSubquery(), enclosingSelects);
            subqueryNode.addParentEnvironmentToPlanTree(parentNode.getEnvironment());
            subqueryNode.addParentEnvironmentToPlanTree(subqueryEnvironment);
            subqueryNode.prepare();
            sub.setSubqueryPlan(subqueryNode);
        }
        if (e instanceof ExistsOperator){
            ExistsOperator sub = (ExistsOperator) e;
            PlanNode subqueryNode = parentPlanner.makePlan(sub.getSubquery(), enclosingSelects);
            subqueryNode.addParentEnvironmentToPlanTree(parentNode.getEnvironment());
            subqueryNode.addParentEnvironmentToPlanTree(subqueryEnvironment);
            subqueryNode.prepare();
            sub.setSubqueryPlan(subqueryNode);
        }

    }

    /**
     * Processes an expression and plans all subqueries.
     *
     * @param e the expression
     *
     * @param enclosingSelects the selects that are the parents of this select
     *
     * @param parentNode the PlanNode from which this subquery processes its tuples,
     * helps set up environments for correlated evaluation.
     *
     * @return Nothing
     *
     * @throws
     */

    public void planSubqueryInExpression(Expression e, List<SelectClause> enclosingSelects,
                                                             PlanNode parentNode) throws IOException{
        SubqueryPlannerExpressionProcessor processor = new SubqueryPlannerExpressionProcessor();

        e.traverse(processor);


        for(int i = 0; i < expressions_to_plan.size(); i++) {
            planSubqueryOperatorExpression(expressions_to_plan.get(i),
                    enclosingSelects, parentNode);

        }
        if (expressions_to_plan.size() > 0){
            parentNode.setEnvironment(subqueryEnvironment);
            parentNode.prepare();
        }
        this.reset();

    }

    /**
     * Processes an expression, and checks if it has a subquery in an ORDER BY or GROUP BY clause
     *
     * @param e the expression
     *
     * @return Nothing
     *
     * @throws IllegalArgumentException if subquery is found in expression
     */

    public void findErrorSubquery(Expression e) throws IOException {
        SubqueryPlannerExpressionProcessor processor = new SubqueryPlannerExpressionProcessor();
        processor.setErrorCheck(1);
        e.traverse(processor);
    }



}
