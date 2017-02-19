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
            // This does not work because leave() is not supposed to throw IOExcpetion
            // so we must do the above method.
            // planSubqueryOperatorExpression(e);


            return e;
        }
    }

    // Since ExpressionProcesser cant call planSubqueryOperatorExpression, 
    // store the expressions in this list and process later 
    private List<Expression> expressions_to_plan; 

    private Planner parentPlanner;

    private Environment subqueryEnvironment;

    /**
     * Constructor that takes as an argument a Planner. 
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
     * Iterates through the select clause and plans all subqueries.
     *
     * @param selClause the select clause
     *
     * @param enclosingSelects the selects that are the parents of this select
     *
     * @return Nothing
     *
     * @throws 
     */
    /*
    public void planAllSubqueriesInSelectClause(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // Throw error if there are subqueries in group or order clauses
        for (OrderByExpression e : selClause.getOrderByExprs()){
            findErrorSubquery(e.getExpression());
        }
        for (Expression e : selClause.getGroupByExprs()){
            findErrorSubquery(e);
        }


        // Subqueries as select values 
        for (SelectValue sv : selClause.getSelectValues()) {
            planSubqueryOperatorExpression(sv.getExpression(), null, null);

            // TODO delete
            // Below is same as above^ 
            // if(sv.getExpression() instanceof SubqueryOperator) {
            //     ((SubqueryOperator) sv.getExpression()).setSubqueryPlan(
            //         makePlan(((SubqueryOperator) sv.getExpression()).getSubquery(), null)
            //         );
            // }

        }


        // Subqueries in WHERE clause
        Expression whereExpr = selClause.getWhereExpr();
        SubqueryPlannerExpressionProcessor processor = new SubqueryPlannerExpressionProcessor();
        if (whereExpr != null) {
            whereExpr.traverse(processor);

            System.out.println("Create plan nodes in whereExpr");

            for(int i = 0; i < expressions_to_plan.size(); i++) {
                System.out.println(expressions_to_plan.get(i));

                planSubqueryOperatorExpression(expressions_to_plan.get(i), null, null);

            }
            expressions_to_plan.clear();
        }


        // Subqueries in HAVING clause
        Expression havingExpr = selClause.getHavingExpr();
        if (havingExpr != null) {
            havingExpr.traverse(processor);

            System.out.println("Create plan nodes in havingExpr");

            for(int i = 0; i < expressions_to_plan.size(); i++) {
                System.out.println(expressions_to_plan.get(i));

                planSubqueryOperatorExpression(expressions_to_plan.get(i), null, null);

            }
            expressions_to_plan.clear();
        }


    }
*/
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

        System.out.println("Create plan nodes in this expression");

        for(int i = 0; i < expressions_to_plan.size(); i++) {
            System.out.println(expressions_to_plan.get(i));

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
