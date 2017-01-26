package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.ColumnName;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AggregateExpressionProcessor implements ExpressionProcessor {
    private HashMap<String, FunctionCall> agg_funct;
    private int errorCheck;

    public void enter(Expression e){};

    public Map<String, FunctionCall> getAggFunct (){
        return agg_funct;
    }
    public void setErrorCheck (int err){
        errorCheck = err;
    }
    public Expression leave(Expression e) {
        // This function never changes the node that is traversed.
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (errorCheck == 1) {
                    throw new IllegalArgumentException("Cannot contain aggregates in where/on clauses");
                }
                else {
                    for (Expression arg : call.getArguments()) {
                        if (arg instanceof FunctionCall) {
                            FunctionCall argCall = (FunctionCall) arg;
                            Function argFunction = argCall.getFunction();
                            if (argFunction instanceof AggregateFunction) {
                                throw new IllegalArgumentException("Cannot contain aggregates in aggregates");
                            }
                        }
                    }
                    String key = call.toString();
                    if (agg_funct == null){
                        agg_funct = new HashMap<String, FunctionCall>();
                    }

                    agg_funct.put(key, call);
                    Expression exp = new ColumnValue(new ColumnName(call.toString()));
                    return exp;
                }
            }
        }
        return e;
    }
}

