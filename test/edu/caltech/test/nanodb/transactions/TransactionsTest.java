// TransactionsTest


package edu.caltech.test.nanodb.transactions;

import org.testng.annotations.Test;

import java.util.List;

import java.lang.Thread;
import java.lang.Runtime;

import edu.caltech.test.nanodb.sql.SqlTestCase;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

import edu.caltech.nanodb.transactions.TransactionManager;


/**
* This class exercises the database with some general SQL statements to test 
* the transaction processing capabilities of the database
**/
@Test(sequential=true)
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

        System.setProperty(TransactionManager.PROP_TXNS, "on"); // Hope this does not mess anything up...


        NanoDBServer server1 = new NanoDBServer();
        server1.startup(this.getTestBaseDir());

        CommandResult result;

        // Create table, insert(before transaction start), start transaction
        result = server1.doCommand("create table basicCommitTransactionsTest (id varchar(5), name varchar(40))", false);
        result = server1.doCommand("insert into basicCommitTransactionsTest values ('-1', 'father joseph')", false);
        result = server1.doCommand("start transaction", false);

        // Insert Rows
        result = server1.doCommand("insert into basicCommitTransactionsTest values ('0', 'bob')", false);
        result = server1.doCommand("insert into basicCommitTransactionsTest values ('1', 'rob')", false);
        result = server1.doCommand("insert into basicCommitTransactionsTest values ('2', 'job')", false);
        result = server1.doCommand("insert into basicCommitTransactionsTest values ('3', 'nob')", false);

        // Update
        result = server1.doCommand("update basicCommitTransactionsTest set name='slob' where id='0'", false);

        // Delete
        result = server1.doCommand("delete from basicCommitTransactionsTest where id='3'", false);

        // Commit
        result = server1.doCommand("commit", false);

        // Check results
        result = server1.doCommand("select * from basicCommitTransactionsTest", true);
        TupleLiteral[] expected = {
            new TupleLiteral(  "-1" ,"father joseph" ),
            new TupleLiteral(  "1"  ,"rob" ),
            new TupleLiteral(  "2"  ,"job" ),
            new TupleLiteral(  "0"  ,"slob" )
        };

        server1.shutdown();

        assert checkUnorderedResults(expected, result);

    }

    /**
     * This test starts a transaction, inserts/updates/deletes data,
     * and rolls back. Verifies changes are gone
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void basicRollBack() throws Throwable {

        /*
         * Implemented for CS122 HW7. I was having a problem where the test
         * was writing to the default folder (datafiles) and not the test 
         * folder (test_datafiles). Trying to fix this by instantiating 
         * a new server instance with the correct datfile directory
         */
        System.setProperty(TransactionManager.PROP_TXNS, "on");

        NanoDBServer server1 = new NanoDBServer();
        server1.startup(this.getTestBaseDir());

        CommandResult result;

        // Create table, insert(before transaction start), start transaction
        result = server1.doCommand("create table basicRollBackTransactionsTest (id varchar(5), name varchar(40))", false);
        result = server1.doCommand("insert into basicRollBackTransactionsTest values ('-1', 'father joseph')", false);
        result = server1.doCommand("start transaction", false);

        // Insert Rows
        result = server1.doCommand("insert into basicRollBackTransactionsTest values ('0', 'bob')", false);
        result = server1.doCommand("insert into basicRollBackTransactionsTest values ('1', 'rob')", false);
        result = server1.doCommand("insert into basicRollBackTransactionsTest values ('2', 'job')", false);
        result = server1.doCommand("insert into basicRollBackTransactionsTest values ('3', 'nob')", false);

        // Update
        result = server1.doCommand("update basicRollBackTransactionsTest set name='slob' where id='0'", false);

        // Delete
        result = server1.doCommand("delete from basicRollBackTransactionsTest where id='3'", false);

        // Rollback
        result = server1.doCommand("rollback", false);

        // Check results
        result = server1.doCommand("select * from basicRollBackTransactionsTest", true);
        TupleLiteral[] expected = {
            new TupleLiteral(  "-1" ,"father joseph" )
        };
        server1.shutdown();
        assert checkUnorderedResults(expected, result);

    }

    /**
     * This tests runs several transactions back to back, and checks if
     * the results are correct. 
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void multiTransaction() throws Throwable {
        /*
         * Implemented for CS122 HW7. I was having a problem where the test
         * was writing to the default folder (datafiles) and not the test 
         * folder (test_datafiles). Trying to fix this by instantiating 
         * a new server instance with the correct datfile directory
         */
        System.setProperty(TransactionManager.PROP_TXNS, "on");

        NanoDBServer server1 = new NanoDBServer();
        server1.startup(this.getTestBaseDir());

        CommandResult result;


        // Commit followed by commit
        result = server1.doCommand("create table multiTransactionTransactionsTest (a int, b int)", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (0,0)", false);
        result = server1.doCommand("commit", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (1,1)", false);
        result = server1.doCommand("commit", false);

        result = server1.doCommand("select * from multiTransactionTransactionsTest", true);
        TupleLiteral[] expected = {
            new TupleLiteral(0, 0),
            new TupleLiteral(1, 1)
        };
        assert checkUnorderedResults(expected, result);

        result = server1.doCommand("drop table if exists multiTransactionTransactionsTest", false);

        // Commit followed by rollback
        result = server1.doCommand("create table multiTransactionTransactionsTest (a int, b int)", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (0,0)", false);
        result = server1.doCommand("commit", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (1,1)", false);
        result = server1.doCommand("rollback", false);

        result = server1.doCommand("select * from multiTransactionTransactionsTest", true);
        TupleLiteral[] expected0 = {
            new TupleLiteral(0, 0)
        };
        assert checkUnorderedResults(expected0, result);

        result = server1.doCommand("drop table if exists multiTransactionTransactionsTest", false);

        // Rollback followed by commit
        result = server1.doCommand("create table multiTransactionTransactionsTest (a int, b int)", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (0,0)", false);
        result = server1.doCommand("rollback", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (1,1)", false);
        result = server1.doCommand("commit", false);

        result = server1.doCommand("select * from multiTransactionTransactionsTest", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(1, 1)
        };
        assert checkUnorderedResults(expected1, result);

        result = server1.doCommand("drop table if exists multiTransactionTransactionsTest", false);       


        // Rollback followed by rollback
        result = server1.doCommand("create table multiTransactionTransactionsTest (a int, b int)", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (0,0)", false);
        result = server1.doCommand("rollback", false);
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("insert into multiTransactionTransactionsTest values (1,1)", false);
        result = server1.doCommand("rollback", false);

        result = server1.doCommand("select * from multiTransactionTransactionsTest", true);
        TupleLiteral[] expected2 = {};
        server1.shutdown();
        assert checkUnorderedResults(expected2, result);

    }


    /*
    * More Advanced Tests
    */


    /**
    * This test involves operations that will modify multiple data pages.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void multiPage() throws Throwable {
        /*
        The default page size is 8K bytes. So we just have to have a table 
        bigger than 8000 bytes. 
        */

        String string7k = "";
        for(int i = 0; i < 7000; i++) {
            string7k.concat("x");
        }
        String cmdString = "";

        System.setProperty(TransactionManager.PROP_TXNS, "on");

        NanoDBServer server1 = new NanoDBServer();
        server1.startup(this.getTestBaseDir());

        CommandResult result;


        result = server1.doCommand("create table multiPageTransactionsTest (a int, b varchar(7500))", false);

        cmdString = "insert into multiPageTransactionsTest values (0, '" + string7k + "')";
        result = server1.doCommand(cmdString, false);

        cmdString = "insert into multiPageTransactionsTest values (1, '" + string7k + "')";
        result = server1.doCommand(cmdString, false);

        cmdString = "insert into multiPageTransactionsTest values (2, '" + string7k + "')";
        result = server1.doCommand(cmdString, false);

        cmdString = "insert into multiPageTransactionsTest values (3, '" + string7k + "')";
        result = server1.doCommand(cmdString, false);

        cmdString = "insert into multiPageTransactionsTest values (4, '" + string7k + "')";
        result = server1.doCommand(cmdString, false);

        cmdString = "insert into multiPageTransactionsTest values (5, '" + string7k + "')";
        result = server1.doCommand(cmdString, false);

        // Commit
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("update multiPageTransactionsTest set b='hi' where a=0", false);
        result = server1.doCommand("update multiPageTransactionsTest set b='bye' where a=5", false);
        result = server1.doCommand("commit", false);

        result = server1.doCommand("select * from multiPageTransactionsTest where (a=0) or (a=5)", true);
        TupleLiteral[] expected = {
            new TupleLiteral(0, "hi"),
            new TupleLiteral(5, "bye")
        };

        assert checkUnorderedResults(expected, result);

        // Rollback
        result = server1.doCommand("start transaction", false);
        result = server1.doCommand("update multiPageTransactionsTest set b='blah blah' where a=0", false);
        result = server1.doCommand("update multiPageTransactionsTest set b='hahaaha' where a=5", false);
        result = server1.doCommand("rollback", false);

        result = server1.doCommand("select * from multiPageTransactionsTest where (a=0) or (a=5)", true);
        TupleLiteral[] expectedd = {
            new TupleLiteral(0, "hi"),
            new TupleLiteral(5, "bye")
        };
        server1.shutdown();
        assert checkUnorderedResults(expectedd, result);
    }



}
