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
        // private int errorCheck;

        public void enter(Expression e){};

        // public void setErrorCheck (int err){
        //     errorCheck = err;
        // }

        public Expression leave(Expression e) {
            // This function never changes the node that is traversed.

            if(e instanceof SubqueryOperator) {
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
     * subqueryPlan.
     *
     * @param e an expression
     *
     * @return Nothing
     *
     * @throws 
     */    

    // TODO make private after testing
    public void planSubqueryOperatorExpression(Expression e, List<SelectClause> enclosingSelects, PlanNode parentNode)
            throws IOException{
        if (e instanceof ScalarSubquery) {
            ScalarSubquery sub = (ScalarSubquery) e;
            PlanNode subqueryNode = parentPlanner.makePlan(sub.getSubquery(), enclosingSelects);
            subqueryNode.addParentEnvironmentToPlanTree(parentNode.getEnvironment());
            subqueryNode.addParentEnvironmentToPlanTree(subqueryEnvironment);
            subqueryNode.prepare();
            sub.setSubqueryPlan(subqueryNode);
            //sub.setSubqueryPlan(parentPlanner.makePlan(sub.getSubquery(), null));

        }
        if (e instanceof InSubqueryOperator){
            InSubqueryOperator sub = (InSubqueryOperator) e;
            PlanNode subqueryNode = parentPlanner.makePlan(sub.getSubquery(), enclosingSelects);
            subqueryNode.addParentEnvironmentToPlanTree(parentNode.getEnvironment());
            subqueryNode.addParentEnvironmentToPlanTree(subqueryEnvironment);
            subqueryNode.prepare();
            sub.setSubqueryPlan(subqueryNode);
            //sub.setSubqueryPlan(parentPlanner.makePlan(sub.getSubquery(), null));

            // TODO
            // ALso might have to plan the expr in InSubqueryOperator, but i think the 
            // traverse in planAllSubqueriesInSelectClause does that already

        }
        if (e instanceof ExistsOperator){
            ExistsOperator sub = (ExistsOperator) e;
            PlanNode subqueryNode = parentPlanner.makePlan(sub.getSubquery(), enclosingSelects);
            subqueryNode.addParentEnvironmentToPlanTree(parentNode.getEnvironment());
            subqueryNode.addParentEnvironmentToPlanTree(subqueryEnvironment);
            subqueryNode.prepare();
            sub.setSubqueryPlan(subqueryNode);
            //sub.setSubqueryPlan(parentPlanner.makePlan(sub.getSubquery(), null));

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


    // private Expression planSubquery(Expression e) throws IOException {
    //     if (e instanceof ScalarSubquery) {
    //         ScalarSubquery sub = (ScalarSubquery) e;
    //         sub.setSubqueryPlan(makePlan(sub.getSubquery(), null));
    //         return sub;
    //     }
    //     if (e instanceof InSubqueryOperator){
    //         InSubqueryOperator sub = (InSubqueryOperator) e;
    //         sub.setSubqueryPlan(makePlan(sub.getSubquery(), null));
    //         return sub;
    //     }
    //     if (e instanceof ExistsOperator){
    //         ExistsOperator sub = (ExistsOperator) e;
    //         sub.setSubqueryPlan(makePlan(sub.getSubquery(), null));
    //         return sub;
    //     }
    //     return e;
    // }

    private void findErrorSubquery(Expression e) throws IOException {
        if (e instanceof ScalarSubquery || e instanceof InSubqueryOperator || e instanceof ExistsOperator) {
            throw new IllegalArgumentException("Cannot contain subqueries in ORDER BY/GROUP BY clauses");
        }
    }

    // public PlanNode makePlan(SelectClause selClause,
    //                            List<SelectClause> enclosingSelects) throws IOException {

    //     // Errors for ORDER BY and GROUP BY
    //     for (OrderByExpression e : selClause.getOrderByExprs()){
    //         findErrorSubquery(e.getExpression());
    //     }
    //     for (Expression e : selClause.getGroupByExprs()){
    //         findErrorSubquery(e);
    //     }

    //     // Process where expression
    //     Expression whereExpr = selClause.getWhereExpr();
    //     if (whereExpr != null) {
    //         Expression new_exp = planSubquery(whereExpr);
    //         selClause.setWhereExpr(new_exp);
    //     }

    //     for (SelectValue sv : selClause.getSelectValues()) {
    //         // Skip select-values that aren't expressions
    //         if (!sv.isExpression())
    //             continue;
    //         Expression e = sv.getExpression();
    //         Expression new_exp = planSubquery(e);
    //         sv.setExpression(new_exp);

    //     }

    //     Expression havingExpr = selClause.getHavingExpr();
    //     if (havingExpr != null) {
    //         Expression new_exp = planSubquery(havingExpr);
    //         selClause.setHavingExpr(new_exp);
    //     }

    //     Planner p = new CostBasedJoinPlanner();
    //     return p.makePlan(selClause, enclosingSelects);
    // }


}
