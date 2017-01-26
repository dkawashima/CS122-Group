package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * HW2
 * This class exercises the database with some simple <tt>JOIN</tt>
 * statements against multiple tables.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }

    /* Tests to implement:
        JOIN TYPES
    	- left join (left outer join)
    	- right join
    	- inner join 

        SCENARIOS
        - both tables non empty
        - right table empty and left not
        - left table empty and right isnt
        - both tables empty
        - one row joins to multiple rows, vice versa
        - 3 tables
        - joining select statements together

        Optional
        - anti join
        - semi



    */

    // Do a simple select to make sure the class is working
        // delete before submission
    public void testJoinSanity() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_one",
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(0,null),
            new TupleLiteral(1,10),
            new TupleLiteral(2,20),
            new TupleLiteral(22,20),
            new TupleLiteral(null,20),
            new TupleLiteral(3,30),
            new TupleLiteral(4,null)
        };
        assert checkUnorderedResults(expected1, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one",
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(0,null),
            new TupleLiteral(1,10),
            new TupleLiteral(3,20),
            new TupleLiteral(22,4),
            new TupleLiteral(3,20),
            new TupleLiteral(3,12),
            new TupleLiteral(4,null)
        };
        assert checkUnorderedResults(expected2, result) == false;

    }


    /**
     * This test performs a inner join where Both tables are empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinBothTblEmpty() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one inner join test_simple_joins_table_empty_two on test_simple_joins_table_empty_one.b = test_simple_joins_table_empty_two.a",
            true);
        TupleLiteral[] expected = {}; 

        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs a right join where Both tables are empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightJoinBothTblEmpty() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one right join test_simple_joins_table_empty_two on test_simple_joins_table_empty_one.b = test_simple_joins_table_empty_two.a",
            true);
        TupleLiteral[] expected = {}; 

        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs a left join where Both tables are empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftJoinBothTblEmpty() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one left join test_simple_joins_table_empty_two on test_simple_joins_table_empty_one.b = test_simple_joins_table_empty_two.a",
            true);
        TupleLiteral[] expected = {}; 

        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs a inner join where Left table is empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinLeftTblEmpty() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one inner join test_simple_joins_table_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.a",
            true);
        TupleLiteral[] expected = {}; 
        assert checkUnorderedResults(expected, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one inner join test_simple_joins_table_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.b",
            true);
        assert checkUnorderedResults(expected, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one inner join test_simple_joins_table_two on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.a",
            true);
        assert checkUnorderedResults(expected, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one inner join test_simple_joins_table_two on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.b",
            true);
        assert checkUnorderedResults(expected, result);

    }


    /**
     * This test performs a right join where Left table is empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightJoinLeftTblEmpty() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one right join test_simple_joins_table_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.a",
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(null, null, 0, null),
            new TupleLiteral(null, null, 1, 10),
            new TupleLiteral(null, null, 2, 20),
            new TupleLiteral(null, null, 3, 30),
            new TupleLiteral(null, null, 4, null),
            new TupleLiteral(null, null, 22, 20),
            new TupleLiteral(null, null, null, 20)
        }; 
        assert checkUnorderedResults(expected1, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one right join test_simple_joins_table_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.b",
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(null, null, 1, 10),
            new TupleLiteral(null, null, 2, 20),
            new TupleLiteral(null, null, 22, 20),
            new TupleLiteral(null, null, null, 20),
            new TupleLiteral(null, null, 3, 30),
            new TupleLiteral(null, null, 4, null),
            new TupleLiteral(null, null, 0, null)
        }; 
        assert checkUnorderedResults(expected2, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one right join test_simple_joins_table_two on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.a",
            true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(null, null, 0, null),
            new TupleLiteral(null, null, 1, 10),
            new TupleLiteral(null, null, 2, 20),
            new TupleLiteral(null, null, 3, 30),
            new TupleLiteral(null, null, 4, null),
            new TupleLiteral(null, null, 33, 30),
            new TupleLiteral(null, null, 333, 30),
            new TupleLiteral(null, null, null, 30),
        }; 
        assert checkUnorderedResults(expected3, result);

    }


    /**
     * This test performs a left join where Left table is empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftJoinLeftTblEmpty() throws Throwable {
        CommandResult result;
        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one left join test_simple_joins_table_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.a",
            true);
        TupleLiteral[] expected1 = {}; 
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_empty_one left join test_simple_joins_table_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.b",
            true);
        assert checkUnorderedResults(expected1, result);

    }


    /**
     * This test performs a inner join where right table is empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinRightTblEmpty() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.a",
            true);

        TupleLiteral[] expected = {}; 
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.b",
            true);
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_two inner join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.a",
            true);
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_two inner join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.b",
            true);
        assert checkUnorderedResults(expected, result);

    }


    /**
     * This test performs a right join where right table is empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightJoinRightTblEmpty() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.a", 
            true);
        TupleLiteral[] expected = {};
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.b", 
            true);    
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_two right join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.b", 
            true);    
        assert checkUnorderedResults(expected, result);

    }


    /**
     * This test performs a left join where right table is empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftJoinRightTblEmpty() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.a", 
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(0, null, null, null),
            new TupleLiteral(1, 10, null, null),
            new TupleLiteral(2, 20, null, null),
            new TupleLiteral(3, 30, null, null),
            new TupleLiteral(4, null, null, null),
            new TupleLiteral(22, 20, null, null),
            new TupleLiteral(null, 20, null, null)
        };
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_one.b", 
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1, 10, null, null),
            new TupleLiteral(2, 20, null, null),
            new TupleLiteral(22, 20, null, null),
            new TupleLiteral(null, 20, null, null),
            new TupleLiteral(3, 30, null, null),
            new TupleLiteral(4, null, null, null),
            new TupleLiteral(0, null, null, null)
        };
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_two left join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.a", 
            true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(0, null, null, null),
            new TupleLiteral(1, 10, null, null),
            new TupleLiteral(2, 20, null, null),
            new TupleLiteral(3, 30, null, null),
            new TupleLiteral(4, null, null, null),
            new TupleLiteral(33, 30, null, null),
            new TupleLiteral(333, 30, null, null),
            new TupleLiteral(null, 30, null, null)
        };
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
            "select * from test_simple_joins_table_two left join test_simple_joins_table_empty_one on test_simple_joins_table_empty_one.b = test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected4 = {
            new TupleLiteral(1, 10, null, null),
            new TupleLiteral(2, 20, null, null),
            new TupleLiteral(null, 30, null, null),
            new TupleLiteral(3, 30, null, null),
            new TupleLiteral(33, 30, null, null),
            new TupleLiteral(333, 30, null, null),
            new TupleLiteral(4, null, null, null),
            new TupleLiteral(0, null, null, null)
        };
        assert checkUnorderedResults(expected4, result);
    }


    /**
     * This test performs a inner join where both tables are non empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinStandard() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b = test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30)
        };
        assert checkUnorderedResults(expected1, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_three on test_simple_joins_table_one.b = test_simple_joins_table_three.b", 
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,2,30),
            new TupleLiteral(3,30,6,30),
            new TupleLiteral(3,30,4,30),
            new TupleLiteral(3,30,5,30)
        };
        assert checkUnorderedResults(expected2, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b != test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20)
        };
        assert checkUnorderedResults(expected3, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b <> test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected4 = {
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20)
        };
        assert checkUnorderedResults(expected4, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b > test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected5 = {
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20)
        };
        assert checkUnorderedResults(expected5, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b < test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected6 = {
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30)
        };
        assert checkUnorderedResults(expected6, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b >= test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected7 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(3,30,null,30)
        };
        assert checkUnorderedResults(expected7, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b <= test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected8 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(3,30,null,30)
        };
        assert checkUnorderedResults(expected8, result);
    }


    /**
     * This test performs a right join where both tables are non empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightJoinStandard() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b = test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(null,null,4,null),
            new TupleLiteral(null,null,0,null)
        };
        assert checkUnorderedResults(expected1, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_three on test_simple_joins_table_one.b = test_simple_joins_table_three.b", 
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,2,30),
            new TupleLiteral(3,30,6,30),
            new TupleLiteral(3,30,4,30),
            new TupleLiteral(3,30,5,30),
            new TupleLiteral(null,null,null,50),
            new TupleLiteral(null,null,null,null),
            new TupleLiteral(null,null,7,null),
            new TupleLiteral(null,null,0,null)
        };
        assert checkUnorderedResults(expected2, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b != test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(null,null,0,null),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(null,null,4,null)
        };
        assert checkUnorderedResults(expected3, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b <> test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected4 = {
            new TupleLiteral(null,null,0,null),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(null,null,4,null)
        };
        assert checkUnorderedResults(expected4, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b > test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected5 = {
            new TupleLiteral(null,null,0,null),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(null,null,3,30),
            new TupleLiteral(null,null,33,30),
            new TupleLiteral(null,null,333,30),
            new TupleLiteral(null,null,null,30),
            new TupleLiteral(null,null,4,null)
        };
        assert checkUnorderedResults(expected5, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b < test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected6 = {
            new TupleLiteral(null,null,0,null),
            new TupleLiteral(null,null,1,10),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(null,null,4,null)
        };
        assert checkUnorderedResults(expected6, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b >= test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected7 = {
            new TupleLiteral(null,null,0,null),
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(null,null,4,null)
        };
        assert checkUnorderedResults(expected7, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one right join test_simple_joins_table_two on test_simple_joins_table_one.b <= test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected8 = {
            new TupleLiteral(null,null,0,null),
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(null,null,4,null)
        };
        assert checkUnorderedResults(expected8, result);

    }


    /**
     * This test performs a left join where both tables are non empty,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftJoinStandard() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b = test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(4,null,null,null),
            new TupleLiteral(0,null,null,null)
        };
        assert checkUnorderedResults(expected1, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_three on test_simple_joins_table_one.b = test_simple_joins_table_three.b", 
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,2,30),
            new TupleLiteral(3,30,6,30),
            new TupleLiteral(3,30,4,30),
            new TupleLiteral(3,30,5,30),
            new TupleLiteral(4,null,null,null),
            new TupleLiteral(0,null,null,null)
        };
        assert checkUnorderedResults(expected2, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b != test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(0,null,null,null),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(4,null,null,null)
        };
        assert checkUnorderedResults(expected3, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b <> test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected4 = {
            new TupleLiteral(0,null,null,null),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(4,null,null,null)
        };
        assert checkUnorderedResults(expected4, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b > test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected5 = {
            new TupleLiteral(0,null,null,null),
            new TupleLiteral(1,10,null,null),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(4,null,null,null)
        };
        assert checkUnorderedResults(expected5, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b < test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected6 = {
            new TupleLiteral(0,null,null,null),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,null,null),
            new TupleLiteral(4,null,null,null)
        };
        assert checkUnorderedResults(expected6, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b >= test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected7 = {
            new TupleLiteral(0,null,null,null),
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(2,20,1,10),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,1,10),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,1,10),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,1,10),
            new TupleLiteral(3,30,2,20),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(4,null,null,null)
        };
        assert checkUnorderedResults(expected7, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one left join test_simple_joins_table_two on test_simple_joins_table_one.b <= test_simple_joins_table_two.b", 
            true);
        TupleLiteral[] expected8 = {
            new TupleLiteral(0,null,null,null),
            new TupleLiteral(1,10,1,10),
            new TupleLiteral(1,10,2,20),
            new TupleLiteral(1,10,3,30),
            new TupleLiteral(1,10,33,30),
            new TupleLiteral(1,10,333,30),
            new TupleLiteral(1,10,null,30),
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(2,20,3,30),
            new TupleLiteral(2,20,33,30),
            new TupleLiteral(2,20,333,30),
            new TupleLiteral(2,20,null,30),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(22,20,3,30),
            new TupleLiteral(22,20,33,30),
            new TupleLiteral(22,20,333,30),
            new TupleLiteral(22,20,null,30),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(null,20,3,30),
            new TupleLiteral(null,20,33,30),
            new TupleLiteral(null,20,333,30),
            new TupleLiteral(null,20,null,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(4,null,null,null)
        };
        assert checkUnorderedResults(expected8, result);

    }


    /**
     * This test performs other miscelaneous joins,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testMiscJoin() throws Throwable {
        /*
        - join select statements
        - join on multiple conditions
        - join multiple tables
        */
        CommandResult result;
        result = server.doCommand(
            "select * from (select * from test_simple_joins_table_one where b > 10) as ttt inner join test_simple_joins_table_two on ttt.b = test_simple_joins_table_two.b",
            true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2,20,2,20),
            new TupleLiteral(22,20,2,20),
            new TupleLiteral(null,20,2,20),
            new TupleLiteral(3,30,null,30),
            new TupleLiteral(3,30,3,30),
            new TupleLiteral(3,30,33,30),
            new TupleLiteral(3,30,333,30)
        };
        assert checkUnorderedResults(expected1, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b = test_simple_joins_table_two.b and test_simple_joins_table_one.a > test_simple_joins_table_two.a", 
            true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(22,20,2,20)
        };
        assert checkUnorderedResults(expected2, result);


        result = server.doCommand(
            "select * from test_simple_joins_table_one inner join test_simple_joins_table_two on test_simple_joins_table_one.b = test_simple_joins_table_two.b inner join test_simple_joins_table_three on test_simple_joins_table_two.b = test_simple_joins_table_three.b", 
            true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(1,10,1,10,1,10),
            new TupleLiteral(2,20,2,20,2,20),
            new TupleLiteral(22,20,2,20,2,20),
            new TupleLiteral(null,20,2,20,2,20),
            new TupleLiteral(3,30,null,30,2,30),
            new TupleLiteral(3,30,3,30,2,30),
            new TupleLiteral(3,30,33,30,2,30),
            new TupleLiteral(3,30,333,30,2,30),
            new TupleLiteral(3,30,null,30,6,30),
            new TupleLiteral(3,30,3,30,6,30),
            new TupleLiteral(3,30,33,30,6,30),
            new TupleLiteral(3,30,333,30,6,30),
            new TupleLiteral(3,30,null,30,4,30),
            new TupleLiteral(3,30,3,30,4,30),
            new TupleLiteral(3,30,33,30,4,30),
            new TupleLiteral(3,30,333,30,4,30),
            new TupleLiteral(3,30,null,30,5,30),
            new TupleLiteral(3,30,3,30,5,30),
            new TupleLiteral(3,30,33,30,5,30),
            new TupleLiteral(3,30,333,30,5,30)
        };
        assert checkUnorderedResults(expected3, result);

    }


}
