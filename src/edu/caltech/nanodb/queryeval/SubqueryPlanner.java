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


public class SubqueryPlanner {

    private Expression planSubquery(Expression e) throws IOException {
        if (e instanceof ScalarSubquery) {
            ScalarSubquery sub = (ScalarSubquery) e;
            sub.setSubqueryPlan(makePlan(sub.getSubquery(), null));
            return sub;
        }
        if (e instanceof InSubqueryOperator){
            InSubqueryOperator sub = (InSubqueryOperator) e;
            sub.setSubqueryPlan(makePlan(sub.getSubquery(), null));
            return sub;
        }
        if (e instanceof ExistsOperator){
            ExistsOperator sub = (ExistsOperator) e;
            sub.setSubqueryPlan(makePlan(sub.getSubquery(), null));
            return sub;
        }
        return e;
    }

    private void findErrorSubquery(Expression e) throws IOException {
        if (e instanceof ScalarSubquery || e instanceof InSubqueryOperator || e instanceof ExistsOperator) {
            throw new IllegalArgumentException("Cannot contain subqueries in ORDER BY/GROUP BY clauses");
        }
    }

    public PlanNode makePlan(SelectClause selClause,
                               List<SelectClause> enclosingSelects) throws IOException {

        // Errors for ORDER BY and GROUP BY
        for (OrderByExpression e : selClause.getOrderByExprs()){
            findErrorSubquery(e.getExpression());
        }
        for (Expression e : selClause.getGroupByExprs()){
            findErrorSubquery(e);
        }

        // Process where expression
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            Expression new_exp = planSubquery(whereExpr);
            selClause.setWhereExpr(new_exp);
        }

        for (SelectValue sv : selClause.getSelectValues()) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;
            Expression e = sv.getExpression();
            Expression new_exp = planSubquery(e);
            sv.setExpression(new_exp);

        }

        Expression havingExpr = selClause.getHavingExpr();
        if (havingExpr != null) {
            Expression new_exp = planSubquery(havingExpr);
            selClause.setHavingExpr(new_exp);
        }

        Planner p = new CostBasedJoinPlanner();
        return p.makePlan(selClause, enclosingSelects);
    }
}
