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

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        //
        // This is a very rough sketch of how this function will work,
        // focusing mainly on join planning:
        //
        // 1)  Pull out the top-level conjuncts from the WHERE and HAVING
        //     clauses on the query, since we will handle them in special ways
        //     if we have outer joins.
        //
        // 2)  Create an optimal join plan from the top-level from-clause and
        //     the top-level conjuncts.
        //
        // 3)  If there are any unused conjuncts, determine how to handle them.
        //
        // 4)  Create a project plan-node if necessary.
        //
        // 5)  Handle other clauses such as ORDER BY, LIMIT/OFFSET, etc.
        //
        // Supporting other query features, such as grouping/aggregation,
        // various kinds of subqueries, queries without a FROM clause, etc.,
        // can all be incorporated into this sketch relatively easily.

        System.out.println("-----------\nmakePlan for:");
        System.out.println(selClause.toString());
        System.out.println("");

    
        // First, check 
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(this);
        //subqueryPlanner.planAllSubqueriesInSelectClause(selClause, enclosingSelects);

        List <SelectClause> enclosingSelectsIncludingThis;
        if (enclosingSelects != null && !enclosingSelects.isEmpty()){
            enclosingSelectsIncludingThis = new ArrayList<SelectClause>(enclosingSelects);
        } else {
            enclosingSelectsIncludingThis = new ArrayList<SelectClause>();
        }
        enclosingSelectsIncludingThis.add(selClause);

        /*if (enclosingSelects != null && !enclosingSelects.isEmpty()) {

            System.out.println("This is a correlated subquery (inside).");

            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }*/

        // Processing GROUP BY expressions and Aggregates
        AggregateExpressionProcessor processor = new AggregateExpressionProcessor();
        processor.setErrorCheck(1);
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            Expression new_exp = whereExpr.traverse(processor);
        }
        if (selClause.getFromClause() != null && selClause.getFromClause().isJoinExpr()) {
            Expression onExpr = selClause.getFromClause().getOnExpression();
            if (onExpr != null){
                Expression new_exp = onExpr.traverse(processor);
            }
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

        // Handle NULL From Clause case
        FromClause fromClause = selClause.getFromClause();
        if (fromClause == null) {
            ProjectNode projNode = new ProjectNode(selClause.getSelectValues());
            projNode.prepare();
            for (SelectValue sv : selClause.getSelectValues()) {
                subqueryPlanner.planSubqueryInExpression(sv.getExpression(), enclosingSelectsIncludingThis,
                        projNode);
            }
            return projNode;
        }

        HashSet<Expression> whereConjuncts = new HashSet<Expression>();
        // Add WHERE conjunct(s) for special handling
        if (whereExpr instanceof BooleanOperator) {
            // A Boolean AND, OR, or NOT operation.
            BooleanOperator bool = (BooleanOperator) whereExpr;
            // Split up Conjunct if this is an AND operation
            if (bool.getType() == BooleanOperator.Type.AND_EXPR){
                for (int i = 0; i < bool.getNumTerms(); i++) {
                    PredicateUtils.collectConjuncts(bool.getTerm(i), whereConjuncts);
                }
            } else {
                PredicateUtils.collectConjuncts(whereExpr, whereConjuncts);
            }
        } else {
            PredicateUtils.collectConjuncts(whereExpr, whereConjuncts);
        }

        HashSet<Expression> havingConjuncts = new HashSet<Expression>();
        // Add HAVING conjunct(s) for special handling
        if (havingExpr instanceof BooleanOperator) {
            // A Boolean AND, OR, or NOT operation.
            BooleanOperator bool = (BooleanOperator) havingExpr;
            // Split up Conjunct if this is an AND operation
            if (bool.getType() == BooleanOperator.Type.AND_EXPR){
                for (int i = 0; i < bool.getNumTerms(); i++) {
                    PredicateUtils.collectConjuncts(bool.getTerm(i), havingConjuncts);
                }
            } else {
                PredicateUtils.collectConjuncts(havingExpr, havingConjuncts);
            }
        } else {
            PredicateUtils.collectConjuncts(havingExpr, havingConjuncts);
        }

        // Create optimal node for FROM clause
        HashSet<Expression> finalConjuncts = new HashSet<Expression>(havingConjuncts);
        finalConjuncts.addAll(whereConjuncts);
        JoinComponent optimal = makeJoinPlan(fromClause, finalConjuncts, enclosingSelectsIncludingThis);

        whereConjuncts.removeAll(optimal.conjunctsUsed);
        havingConjuncts.removeAll(optimal.conjunctsUsed);
        finalConjuncts.removeAll(optimal.conjunctsUsed);
        PlanNode curNode = optimal.joinPlan;


        if (finalConjuncts.size() > 0) {
            if (whereConjuncts.size() > 0){
                Expression wherePred = PredicateUtils.makePredicate(whereConjuncts);
                curNode = new SimpleFilterNode(optimal.joinPlan, wherePred);
                curNode.prepare();
                subqueryPlanner.planSubqueryInExpression(wherePred, enclosingSelectsIncludingThis,
                        curNode);
            }

            PlanNode finalNode;
            if (processor.getAggFunct() != null || !selClause.getGroupByExprs().isEmpty()) {
                HashedGroupAggregateNode aggregateNode;
                if (processor.getAggFunct() == null) {
                    HashMap<String, FunctionCall> empty_agg = new HashMap<String, FunctionCall>();
                    aggregateNode = new HashedGroupAggregateNode(curNode,
                            selClause.getGroupByExprs(), empty_agg);
                } else {
                    aggregateNode = new HashedGroupAggregateNode(curNode,
                            selClause.getGroupByExprs(), processor.getAggFunct());
                }
                aggregateNode.prepare();
                if (havingConjuncts.size() > 0) {
                    Expression havingPred = PredicateUtils.makePredicate(havingConjuncts);
                    SimpleFilterNode havingNode = new SimpleFilterNode(aggregateNode, havingPred);
                    havingNode.prepare();
                    subqueryPlanner.planSubqueryInExpression(havingPred, enclosingSelectsIncludingThis,
                            havingNode);
                    finalNode = havingNode;
                } else {
                    finalNode = aggregateNode;
                }
            } else {
                finalNode = curNode;
            }
            if (!selClause.isTrivialProject()) {
                System.out.println("Info on ProjectNode in Subquery");
                System.out.println(selClause.getSelectValues().toString());
                System.out.println(finalNode.toString());
                ProjectNode projNode = new ProjectNode(finalNode, selClause.getSelectValues());
                projNode.prepare();
                for (SelectValue sv : selClause.getSelectValues()) {
                    subqueryPlanner.planSubqueryInExpression(sv.getExpression(), enclosingSelectsIncludingThis,
                            projNode);
                }
                if (!selClause.getOrderByExprs().isEmpty()) {
                    SortNode orderByNode = new SortNode(projNode, selClause.getOrderByExprs());
                    orderByNode.prepare();
                    return orderByNode;
                }
                return projNode;
            } else {
                if (!selClause.getOrderByExprs().isEmpty()) {
                    SortNode orderByNode = new SortNode(finalNode, selClause.getOrderByExprs());
                    orderByNode.prepare();
                    return orderByNode;
                }
                return finalNode;
            }
        } else { // No predicates to apply in this node, all were previously applied
            PlanNode finalNode;
            if (processor.getAggFunct() != null || !selClause.getGroupByExprs().isEmpty()) {
                HashedGroupAggregateNode aggregateNode;
                if (processor.getAggFunct() == null) {
                    HashMap<String, FunctionCall> empty_agg = new HashMap<String, FunctionCall>();
                    aggregateNode = new HashedGroupAggregateNode(curNode,
                            selClause.getGroupByExprs(), empty_agg);
                } else {
                    aggregateNode = new HashedGroupAggregateNode(curNode,
                            selClause.getGroupByExprs(), processor.getAggFunct());
                }
                aggregateNode.prepare();
                if (havingConjuncts.size() > 0) {
                    Expression havingPred = PredicateUtils.makePredicate(havingConjuncts);
                    SimpleFilterNode havingNode = new SimpleFilterNode(aggregateNode, havingPred);
                    havingNode.prepare();
                    subqueryPlanner.planSubqueryInExpression(havingPred, enclosingSelectsIncludingThis,
                            havingNode);
                    finalNode = havingNode;
                } else {
                    finalNode = aggregateNode;
                }
            } else {
                finalNode = curNode;
            }
            if (!selClause.isTrivialProject()) {
                ProjectNode projNode = new ProjectNode(finalNode, selClause.getSelectValues());
                for (SelectValue sv : selClause.getSelectValues()) {
                    subqueryPlanner.planSubqueryInExpression(sv.getExpression(), enclosingSelectsIncludingThis,
                            projNode);
                }
                projNode.prepare();
                if (!selClause.getOrderByExprs().isEmpty()) {
                    SortNode orderByNode = new SortNode(projNode, selClause.getOrderByExprs());
                    orderByNode.prepare();
                    return orderByNode;
                }
                return projNode;
            } else {
                if (!selClause.getOrderByExprs().isEmpty()) {
                    SortNode orderByNode = new SortNode(finalNode, selClause.getOrderByExprs());
                    orderByNode.prepare();
                    return orderByNode;
                }
                return finalNode;
            }
        }
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts, List<SelectClause> enclosingSelects) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts, enclosingSelects);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:\n" +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts, enclosingSelects);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * This method generates a collection of all of the conjuncts from non-leaf
     * <tt>FROM</tt> clauses and an arrayList of all of the leaf <tt>FROM</tt>
     * clauses. All base table, derived table, and outer join <tt>FROM</tt>
     * clauses are considered leaf clauses, and are simply added to the result
     * arrayList, which is passed in as a parameter. For non-leaf <tt>FROM</tt>
     * clauses, such as inner join clauses, we add the conjunct to the collection,
     * separating the conjunct in to subconjuncts when an AND boolean operator is
     * involved.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        // Base Table and Derived Table are leaf clauses
        if (fromClause.isBaseTable() || fromClause.isDerivedTable()){
            leafFromClauses.add(fromClause);
        }
        if (fromClause.isJoinExpr()) {
            // Outer Join is also considered to be a leaf clause
            if (fromClause.isOuterJoin()) {
                leafFromClauses.add(fromClause);
            } else {
                // Handle the non-leaf clause Inner Join case
                Expression onExp = fromClause.getComputedJoinExpr();
                if (onExp instanceof BooleanOperator) {
                    // A Boolean AND, OR, or NOT operation.
                    BooleanOperator bool = (BooleanOperator) onExp;
                    // Split up Conjunct if this is an AND operation
                    if (bool.getType() == BooleanOperator.Type.AND_EXPR){
                        for (int i = 0; i < bool.getNumTerms(); i++) {
                            PredicateUtils.collectConjuncts(bool.getTerm(i), conjuncts);
                        }
                    } else {
                        PredicateUtils.collectConjuncts(onExp, conjuncts);
                    }
                } else {
                    PredicateUtils.collectConjuncts(onExp, conjuncts);
                }
                collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
                collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
            }
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts,
        List<SelectClause> enclosingSelects)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts, enclosingSelects);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * If the given from clause is a base table, we return a FileScanNode,
     * while if it is a derived table (subquery in the from clause), we
     * recursively generate the plan of the subquery itself, with a
     * RenameNode at the root of the tree. In the case that the from
     * clause is an outer join, we check to see if we can apply any of the
     * overall conjuncts to the child nodes of the join, provided that
     * such changes do not change the results of the query. Then, after
     * making any necessary changes to improve efficiency, we add the
     * two recursively generated child nodes into a NestedLoopJoinNode
     * of the correct type.
     *
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts
     *        applied in this plan from the <tt>conjuncts</tt> collection
     *        should be added to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts,
                                  List<SelectClause> enclosingSelects)
        throws IOException {

        //        If you apply any conjuncts then make sure to add them to the
        //        leafConjuncts collection.
        //
        //        Don't forget that all from-clauses can specify an alias.
        //
        //        Concentrate on properly handling cases other than outer
        //        joins first, then focus on outer joins once you have the
        //        typical cases supported.

        PlanNode finalNode = null;
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(this);
        // Simple FileScanNode for the Base Table case
        if (fromClause.isBaseTable()){
            if (fromClause.getResultName() != fromClause.getTableName()){
                PlanNode initNode = makeSimpleSelect(fromClause.getTableName(), null, null);
                PlanNode tempNode = new RenameNode(
                        makeSimpleSelect(fromClause.getTableName(), null, null),
                        fromClause.getResultName());
                tempNode.prepare();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, leafConjuncts, tempNode.getSchema());
                Expression leafPred = PredicateUtils.makePredicate(leafConjuncts);
                System.out.println("LeafPred might be NULL:");
                if (leafPred != null){
                    System.out.println("LeafPred:");
                    System.out.println(leafPred.toString());
                    finalNode = new SimpleFilterNode(tempNode, leafPred);
                    finalNode.prepare();
                    subqueryPlanner.planSubqueryInExpression(leafPred, enclosingSelects,
                            finalNode);
                    //finalNode = PlanUtils.addPredicateToPlan(finalNode, leafPred);
                    finalNode.prepare();
                } else {
                    finalNode = tempNode;
                }
            } else {
                finalNode = makeSimpleSelect(fromClause.getTableName(), null, null);
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, leafConjuncts, finalNode.getSchema());
                Expression leafPred = PredicateUtils.makePredicate(leafConjuncts);
                if (leafPred != null) {
                    subqueryPlanner.planSubqueryInExpression(leafPred, enclosingSelects,
                            finalNode);
                    finalNode = PlanUtils.addPredicateToPlan(finalNode, leafPred);
                    finalNode.prepare();
                }
            }
        }
        // Recursively call makePlan, with RenameNode for the Derived Table case
        if (fromClause.isDerivedTable()){
            SelectClause fromSelClause = fromClause.getSelectClause();
            finalNode = new RenameNode(makePlan(fromSelClause, null),
                    fromClause.getResultName());
            finalNode.prepare();
            PredicateUtils.findExprsUsingSchemas(conjuncts, false, leafConjuncts, finalNode.getSchema());
            Expression leafPred = PredicateUtils.makePredicate(leafConjuncts);
            if (leafPred != null){
                subqueryPlanner.planSubqueryInExpression(leafPred, enclosingSelects,
                        finalNode);
                finalNode = PlanUtils.addPredicateToPlan(finalNode, leafPred);
                finalNode.prepare();
            }
        }
        if (fromClause.isJoinExpr() && fromClause.isOuterJoin()){
            PlanNode leftNode = makeJoinPlan(fromClause.getLeftChild(), null,
                    null).joinPlan;
            PlanNode rightNode = makeJoinPlan(fromClause.getRightChild(), null,
                    null).joinPlan;
            // If this is left outer join, we check to see if we can push any conjuncts down to the
            // left plan node.
            if (fromClause.hasOuterJoinOnLeft() && !fromClause.hasOuterJoinOnRight()){
                leftNode.prepare();
                rightNode.prepare();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, leafConjuncts, leftNode.getSchema());
                Expression leftPred = PredicateUtils.makePredicate(leafConjuncts);
                // Only call prepare() if necessary
                if (leftPred != null){
                    subqueryPlanner.planSubqueryInExpression(leftPred, enclosingSelects,
                            leftNode);
                    leftNode = PlanUtils.addPredicateToPlan(leftNode, leftPred);
                    leftNode.prepare();
                }
            }
            // If this is right outer join, we check to see if we can push any conjuncts down to the
            // right plan node.
            if (fromClause.hasOuterJoinOnRight() && !fromClause.hasOuterJoinOnLeft()){
                leftNode.prepare();
                rightNode.prepare();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, leafConjuncts, rightNode.getSchema());
                Expression rightPred = PredicateUtils.makePredicate(leafConjuncts);
                // Only call prepare() if necessary
                if (rightPred != null){
                    subqueryPlanner.planSubqueryInExpression(rightPred, enclosingSelects,
                            leftNode);
                    rightNode = PlanUtils.addPredicateToPlan(rightNode, rightPred);
                    rightNode.prepare();
                }
            }
            finalNode = new NestedLoopJoinNode(leftNode, rightNode,
                    fromClause.getJoinType(), fromClause.getComputedJoinExpr());
            finalNode.prepare();
            }
            if (finalNode == null){
                throw new IllegalArgumentException("From clause has unrecognized type");
            }
        return finalNode;
        }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts,
        List<SelectClause> enclosingSelects) throws IOException {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans = new HashMap<>();


        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(this);
        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                    new HashMap<>();
            for (JoinComponent planN : joinPlans.values()) {
                for (JoinComponent leaf : leafComponents) {

                    if (planN.leavesUsed.contains(leaf.joinPlan)) {
                        continue;
                    }

                    HashSet<PlanNode> nextLeavesUsed = new HashSet<PlanNode> (planN.leavesUsed);
                    nextLeavesUsed.addAll(leaf.leavesUsed);
                    PlanNode nextJoinPlan = new NestedLoopJoinNode(planN.joinPlan, leaf.joinPlan,
                            JoinType.INNER, null);

                    HashSet<Expression> conjunctsUnion = new HashSet<Expression> (planN.conjunctsUsed);
                    conjunctsUnion.addAll(leaf.conjunctsUsed);
                    HashSet<Expression> unusedConjuncts = new HashSet<Expression> (conjuncts);
                    unusedConjuncts.removeAll(conjunctsUnion);
                    HashSet<Expression> nextConjunctsUsed = new HashSet<Expression>();

                    nextJoinPlan.prepare();
                    PredicateUtils.findExprsUsingSchemas(unusedConjuncts, false, nextConjunctsUsed,
                            nextJoinPlan.getSchema());
                    Expression predicate = PredicateUtils.makePredicate(nextConjunctsUsed);
                    // Only call prepare() if necessary
                    if (predicate != null){
                        subqueryPlanner.planSubqueryInExpression(predicate, enclosingSelects,
                                nextJoinPlan);
                        /*nextJoinPlan = new NestedLoopJoinNode(planN.joinPlan, leaf.joinPlan,
                                JoinType.INNER, predicate);*/
                        nextJoinPlan = PlanUtils.addPredicateToPlan(nextJoinPlan, predicate);
                        nextJoinPlan.prepare();
                    }

                    nextConjunctsUsed.addAll(conjunctsUnion);
                    JoinComponent planNext = new JoinComponent(nextJoinPlan, nextLeavesUsed, nextConjunctsUsed);
                    float newCost = planNext.joinPlan.getCost().cpuCost;
                    if (nextJoinPlans.keySet().contains(planNext.leavesUsed)) {
                        if (newCost < nextJoinPlans.get(planNext.leavesUsed).joinPlan.getCost().cpuCost)
                            nextJoinPlans.put(planNext.leavesUsed, planNext);
                    } else {
                        nextJoinPlans.put(planNext.leavesUsed, planNext);
                    }
                }
            }
            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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
