package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.ColumnName;
import java.util.Collection;

public class AggregateExpressionProcessor implements ExpressionProcessor {

    public void enter(Expression e){};

    public Expression leave(Expression e) {
        // This function never changes the node that is traversed.
        FunctionCall call = (FunctionCall) e;
        Expression exp = new ColumnValue(new ColumnName(call.toString()));
        return exp;
    }
}

