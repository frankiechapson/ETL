# Summary of the solution

This is an ETL (Extract Transform Load) package which collects the data from source databases via database links and loads them into the target tables.

# Requirements and Preparations

## Target schema

Create a schema for target database.

For example, if the schema name was DWH, these are the minimum roles and privileges for this user:

ROLES

    GRANT CONNECT TO DWH;  
    GRANT RESOURCE TO DWH;  
    ALTER USER DWH DEFAULT ROLE CONNECT, RESOURCE;  
    

SYSTEM PRIVILEGES

    GRANT CREATE JOB                TO DWH;  
    GRANT CREATE SESSION            TO DWH;  
    GRANT CREATE TABLE              TO DWH;  
    GRANT CREATE TYPE               TO DWH;  
    GRANT CREATE SYNONYM            TO DWH;  
    GRANT CREATE DATABASE LINK      TO DWH;  
    GRANT UNLIMITED TABLESPACE      TO DWH;  
    GRANT CREATE PROCEDURE          TO DWH;  

and must have execute right to  

    sys.dbms_lock.sleep(  );  

    or  

    sys.dbms_session.sleep(  );  


## Database links

Create database links to access the source schemas from target database.

for example:

    CREATE DATABASE LINK MASTER  
    CONNECT TO MASTER_READONLY IDENTIFIED BY MASTER_READONLY_PWD  
        USING '100.100.100.100:1521/test19c';  

Test the database links for example with these queries:

    select * from ITEMS@MASTER;


# Installation

## ETL package

There is a file with name: 

**Install\_ETL.sql**

Execute this file or the script in it on the target schema! 

1. This script is going to create a table named **ETL\_TABLES**. This table administrate the ETL tables and processes. See more below!
1. After that it creates a type: T\_STRING\_LIST which is used by the PKG\_ETL package.
1. Finally, the script deploys the PKG\_ETL PL/SQL package.

## Target Tables

Create the target tables according to the structure of source tables and views.

**DO NOT USE CONSTRAINTS ON TARGET TABLES EXCEPT FOR PRIMARY KEY!**

The Primary key is mandatory on target table if we used different TRANSFER\_TYPE than FULL. See it later!

# ETL\_TABLES table

This table is used to administrate the ETL process. The structure of this table is the following:

The **LOCAL\_NAME** must contain an existing local table name in the target schema.

The **REMOTE\_NAME** must contain an existing remote table or view name in source schemas.

The ETL process is going to copy rows from a remote table or view into a local table.

The **TRANSFER\_TYPE** can contain

- **FULL**, use this, if the physical delete is allowed on the source table (or view). Also if there was not any column in the source what contains the last modification time but the update is allowed.
  The “FULL” means the target table will be truncated, then every rows from the source will be inserted into.
- **MERGE**, like “FULL”, but this merge (insert or update by primary key) every rows from the source table into the target. So this keeps the rows in the target that will be deleted later from the source table.
- The **name of a date/timestamp column** in the source, what contains the last modification (including insert) time in case: if there was such a column, and the physical delete is not allowed for the source. In this case the ETL will copy only those rows from the source which were created/modified after the latest created/modified row in the target.
  This is the **differential** copy, base on creation/modification time.
- The **name of an “ID” type sequence number column**. We can use it, if neither delete nor update are allowed on the source, but only the insert.
  This is the **incremental** copy, based on ID sequence.

The remaining columns are used to monitor the ETL process.

# Checking 

There are two ways to check the system consistency:

1. Checking the tables existence and comparing their structures
2. Comparing the data rows in the tables

## Table structures

There are two functions in the PKG\_ETL package what can do this:

    function ONE_TAB_DEF_DIFF ( I_TARGET_TABLE_NAME in varchar2 ) return T_STRING_LIST PIPELINED;  

    function ALL_TAB_DEF_DIFF return T_STRING_LIST PIPELINED;  

The ALL\_TAB\_DEF\_DIFF calls the ONE\_TAB\_DEF\_DIFF for every LOCAL\_NAME from the ETL\_TABLES, so only the ONE\_TAB\_DEF\_DIFF will be described here.

Use the ONE\_TAB\_DEF\_DIFF function this way: (example only)

    select * from table( PKG_ETL.ONE_TAB_DEF_DIFF( 'ITEMS' ) );  

At the first step the function reads the row from ETL\_TABLES where the LOCAL\_NAME equals with the name in the parameter. If it did not exist, then the function returns with null.

In the second step the function checks the existence of the local table. If it did not exist, then returns with the following lines:

    Source : ITEMS@MASTER

    Target : ITEMS

    ! Table (target) does not exist locally.

    -----------------------------------------------------------

In the 3rd step the function checks the existence of the remote table. If it did not exist, then returns with the following lines:

    Source : ITEMS@MASTER

    Target : ITEMS

    ! Table (source) does not exist remotely.  

    -----------------------------------------------------------

Then the function compares the columns one by one in both local table and remote table (or view) and lists the differences. It is checking:

- existences of columns by name
- any differences in type, length, precision and scale.

The function lists these or just write this

    Source : ITEMS@MASTER

    Target : ITEMS

    = The definition of tables are identical.  

    -----------------------------------------------------------

if the structure of remote and local tables are the same.

In case there is any difference in columns, the message will be one of the followings: (example)

    ! Column : BINARY_VALUE is missing locally

      Type   : BLOB, Length: 4000, Precision: , Scale:  

or

    ! Column : BINARY_VALUE is missing remotely

      Type   : BLOB, Length: 4000, Precision: , Scale:   

or

    ! Column : BINARY_VALUE definitions are differ:

      Source : Type   : CLOB, Length: 4000, Precision: , Scale: 

      Target : Type   : BLOB, Length: 4000, Precision: , Scale: 

(for each different columns)


To check the whole structure, run the following query:

    select * from table( PKG_ETL.ALL_TAB_DEF_DIFF());




## Table content

There are two functions in the PKG\_ETL package what can do this:

    function ONE_TAB_DAT_DIFF ( I_TARGET_TABLE_NAME in varchar2 )  return T_STRING_LIST PIPELINED;

    function ALL_TAB_DAT_DIFF return T_STRING_LIST PIPELINED;

The ALL\_TAB\_DAT\_DIFF calls the ONE\_TAB\_DAT\_DIFF for every LOCAL\_NAME from the ETL\_TABLES, so only the ONE\_TAB\_DAT\_DIFF will be described here.

Use the ONE\_TAB\_DAT\_DIFF function this way: (example only)

    select * from table( PKG_ETL.ONE_TAB_DAT_DIFF( 'ITEMS' ));

At the first step the function reads the row from ETL\_TABLES where the LOCAL\_NAME equals with the name in the parameter. If it did not exist, then the function returns with null.

In the second step the function runs a “local table” minus “remote table/view” query with the columns of “local table”. This lists the rows what exist in “local table”, but does not exist in the remote table. (because they were physically deleted from the source).

Finally, the function runs a “remote table/view” minus “local table” query with the columns of “local table”. This lists the rows what exist in the “remote table/view”, but does not exist in the “local table”. 

The function separates the column names and values by horizontal tab character (ascii 9). 


To check every table content, run the following query:

    select * from table( PKG_ETL.ALL_TAB_DAT_DIFF());


# Operation of ETL

## Initializing

After the installation, start the first ETL process manually:

    begin  
        PKG_ETL.START_ETL;  
    end;  
    /

This fills up every target tables to the current state of sources.

When it finished, check the ETL\_TABLES and fix the problems if they were. See “Monitoring”!

## Running the ETL Process

The ETL process can be run by starting the PKG\_ETL.START\_ETL procedure for every table:

    begin  
        PKG_ETL.START_ETL;  
    end;  
    /

But we can start it only one table by this way: (for example only for the ITEMS table)

    begin  
        PKG_ETL.START_ETL( 'ITEMS' );  
    end;  
    /

The PKG\_ETL.START\_ETL procedure creates individual jobs for each table in the ETL\_TABLES.  
So, the ETL processes for each table are running parallelly. The START\_ETL waits 1 second before starting a new job, avoiding to run too many jobs in the same time.  
To run the ETL process regularly, create an Oracle job that will start it! You can create one or more jobs for all tables or just for one table if you want to refresh some tables more frequently than others.  
Here is an example job, which starts ETL for every table on every day at 1:00 AM:

    BEGIN  
        DBMS_SCHEDULER.CREATE_JOB (  
                job_name            => 'REPORTING_ETL',  
                job_type            => 'PLSQL_BLOCK',  
                job_action          => 'PKG_ETL.START_ETL;',  
                number_of_arguments => 0,  
                start_date          => NULL,  
                repeat_interval     => 'FREQ=DAILY;BYTIME=010000',  
                end_date            => NULL,  
                enabled             => FALSE,  
                auto_drop           => FALSE,  
                comments            => 'ETL for every table');  
        DBMS_SCHEDULER.SET_ATTRIBUTE(   
                name       => 'REPORTING_ETL',   
                attribute  => 'restartable'  , value => TRUE);  
        DBMS_SCHEDULER.SET_ATTRIBUTE(   
                name       => 'REPORTING_ETL',   
                attribute  => 'logging_level', value => DBMS_SCHEDULER.LOGGING_FULL);  
  
        DBMS_SCHEDULER.enable(  
                name       => 'REPORTING_ETL');  
  
        COMMIT;           
    END;  
    /  


**When an ETL job starts for a certain table the ID of this job is updating into the ETL\_TABLES row belongs to this table. So, it is not possible to start an ETL process for a table if there was a job running with this job ID!**



## Monitoring

In the ETL\_TABLES table the following columns are available for monitoring

    JOB_STARTED         TIMESTAMP(6)          Start time of last ETL Job 
    JOB_ID              NUMBER                ID of last ETL Job  
    JOB_FINISHED        TIMESTAMP(6)          End time of last ETL Job  
    JOB_RESULT          VARCHAR2(4000)        Result of last ETL Job run  
    NOF_INSERTED_ROWS   NUMBER                Number of inserted rows in the last Job run  
    NOF_UPDATED_ROWS    NUMBER                Number of updated rows in the last Job run  
    NOF_DELETED_ROWS    NUMBER                Number of deleted rows in the last Job run  

The purpose and content of each column could be clear except perhaps the JOB\_RESULT.  
This field shows the result of the latest ETL job run for the table.   
It could be two kind of value:

1. SUCCESS = the job finished successfully
2. an Oracle error code and message

In the 2<sup>nd</sup> case we have to fix the problem as soon as possible and start the ETL for that table again.


## Configuring

If the structure of a source table changes, it must be examined whether or not the target table needs to be adjusted.

If we do not need a target table anymore, first we must delete its row from the ETL\_TABLES, then drop the target table from the target schema.

If we want to add a new table to the ETL, we must create the target table under the target schema, then insert its new row into the ETL\_TABLES table.

This table must have a PRIMARY\_KEY! It could be compound.

Do not use foreign key or any other constraints on target table (except for PK)

We can use less column in the target table, than the source has, but their name and type must be the same.

We can use views as a source too.


