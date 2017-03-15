// copy of testAggregation
// TransactionsTest


package edu.caltech.test.nanodb.transactions;

import org.testng.annotations.Test;

import java.util.List;

import edu.caltech.test.nanodb.sql.SqlTestCase;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
* This class exercises the database with some general SQL statements to test 
* the transaction processing capabilities of the database
**/
@Test
public class TransactionsTest extends SqlTestCase {
    public TransactionsTest() {
        super();
    }


    /**
     * This is a basic test to see if this test class is being called 
     * correctly. If this one does not pass, the test is messed up somehow.
     *
     * This test is useful because it can gauge whether or not the tests 
     * are functioning or not.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testOfTest() throws Throwable {
        CommandResult result;
        result = server.doCommand("create table t_trans (a int)", false);
        if(result.failed()) {
            assert false;
        }
        result = server.doCommand("insert into t_trans values (1)", false);
        if(result.failed()) {
            assert false;
        }
        result = server.doCommand("select * from t_trans", true);
        TupleLiteral[] expected = {createTupleFromNum(1)};

        assert checkUnorderedResults(expected, result);

        result = server.doCommand("drop table t_trans", false);
        if(result.failed()) {
            assert false;
        }        
    }


    /*
    * Basic Tests
    */


    /**
     * This test starts a transaction, inserts/updates/deletes data,
     * and commits. Verifies data is still present.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void basicCommit() throws Throwable {
        CommandResult result;

        // Create table, insert(before transaction start), start transaction
        result = server.doCommand("create table basicCommitTransactionsTest (id varchar(5), name varchar(40))", false);
        result = server.doCommand("insert into basicCommitTransactionsTest values ('-1', 'father joseph')", false);
        result = server.doCommand("begin transaction", false);

        // Insert Rows
        result = server.doCommand("insert into basicCommitTransactionsTest values ('0', 'bob')", false);
        result = server.doCommand("insert into basicCommitTransactionsTest values ('1', 'rob')", false);
        result = server.doCommand("insert into basicCommitTransactionsTest values ('2', 'job')", false);
        result = server.doCommand("insert into basicCommitTransactionsTest values ('3', 'nob')", false);

        // Update
        result = server.doCommand("update basicCommitTransactionsTest set name='slob' where id='0'", false);

        // Delete
        result = server.doCommand("delete from basicCommitTransactionsTest where id='3'", false);

        // Commit
        result = server.doCommand("commit", false);

        // Check results
        result = server.doCommand("select * from basicCommitTransactionsTest", true);
        TupleLiteral[] expected = {
            new TupleLiteral(  "-1" ,"father joseph" ),
            new TupleLiteral(  "1"  ,"rob" ),
            new TupleLiteral(  "2"  ,"job" ),
            new TupleLiteral(  "0"  ,"slob" )
        };
        assert checkUnorderedResults(expected, result);

    }

    /**
     * This test starts a transaction, inserts/updates/deletes data,
     * and rolls back. Verifies changes are gone
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void basicRollBack() throws Throwable {
        CommandResult result;

        // Create table, insert(before transaction start), start transaction
        result = server.doCommand("create table basicRollBackTransactionsTest (id varchar(5), name varchar(40))", false);
        result = server.doCommand("insert into basicRollBackTransactionsTest values ('-1', 'father joseph')", false);
        result = server.doCommand("begin transaction", false);

        // Insert Rows
        result = server.doCommand("insert into basicRollBackTransactionsTest values ('0', 'bob')", false);
        result = server.doCommand("insert into basicRollBackTransactionsTest values ('1', 'rob')", false);
        result = server.doCommand("insert into basicRollBackTransactionsTest values ('2', 'job')", false);
        result = server.doCommand("insert into basicRollBackTransactionsTest values ('3', 'nob')", false);

        // Update
        result = server.doCommand("update basicRollBackTransactionsTest set name='slob' where id='0'", false);

        // Delete
        result = server.doCommand("delete from basicRollBackTransactionsTest where id='3'", false);

        // Rollback
        result = server.doCommand("rollback", false);

        // Check results
        result = server.doCommand("select * from basicRollBackTransactionsTest", true);
        TupleLiteral[] expected = {
            new TupleLiteral(  "-1" ,"father joseph" )
        };
        assert checkUnorderedResults(expected, result);

    }

    /**
     * This tests runs several transactions back to back, and checks if
     * the results are correct. 
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void multiTransaction() throws Throwable {

        // TODO: 3
        // TransactionsTest
        /* 
        commit-commit
        commit-rollback
        rollback-commit
        rollback-rollback
        */
        CommandResult result;

        // Insert rows
        // result = server.doCommand();

        assert false;

    }



    /**
    * This test performs summing, to see if the query produces the expected
    * results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testSum() throws Throwable {
        // CommandResult result;

        // result = server.doCommand(
        //     "SELECT SUM(balance) FROM test_aggregate", true);
        // TupleLiteral[] expected1 = {
        //     createTupleFromNum( 36700480 )
        // };

        // assert checkSizeResults(expected1, result);
        // assert checkOrderedResults(expected1, result);

        // result = server.doCommand(
        //     "SELECT SUM(balance) FROM test_aggregate WHERE branch_name = 'North Town'", true);
        // TupleLiteral[] expected2 = {
        //     createTupleFromNum( 3700000 )
        // };
        // assert checkSizeResults(expected2, result);
        // assert checkOrderedResults(expected2, result);


        assert false;
    }

    /*
    * More Advanced Tests
    */

    // TransactionsTest

    /*
    * Recovery Processing Tests
    */

    // todo:
    /*
    first run crash, to see what happens to the 'server' object. see if 
    it can still run commands - should not be able to. Then, can probably
    spawn a new server object. 

    */

}
