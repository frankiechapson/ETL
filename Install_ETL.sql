/*********************************************************************
    Author  :   TothF  
    Remark  :   Objects for ETL
    Date    :   2022.01.05
*********************************************************************/


Prompt *****************************************************************
Prompt **       I N S T A L L I N G    E T L _ T A B L E S            **
Prompt *****************************************************************


/*============================================================================================*/
create table ETL_TABLES (
/*============================================================================================*/
  LOCAL_NAME                varchar2(  500 )                not null,
  REMOTE_NAME               varchar2(  500 )                not null,
  TRANSFER_TYPE             varchar2(  500 )                not null,
  JOB_STARTED               timestamp                       null,
  JOB_ID                    number                          null,
  JOB_FINISHED              timestamp                       null,
  JOB_RESULT                varchar2( 4000 )                null,
  NOF_INSERTED_ROWS         number                          null,
  NOF_UPDATED_ROWS          number                          null,
  NOF_DELETED_ROWS          number                          null
  );

comment on table  ETL_TABLES                      is 'This table configures and administrates the ETL Tables';
comment on column ETL_TABLES.LOCAL_NAME           is 'The name of the target table (with schema name if necessary) eg: ITEMS';
comment on column ETL_TABLES.REMOTE_NAME          is 'The name of the source data source whith db_link name (table or view) eg: ITEMS@MASTER';
comment on column ETL_TABLES.TRANSFER_TYPE        is '"FULL" = truncate target then insert every rows from source, "MERGE" = merge/upsert based on PK, or a column name such: ID = incremental based on ID, MODIF_TIME = incremental based on MODIF_TIME etc.';
comment on column ETL_TABLES.JOB_STARTED          is 'Start time of last ETL Job';
comment on column ETL_TABLES.JOB_ID               is 'ID of last ETL Job';
comment on column ETL_TABLES.JOB_FINISHED         is 'End time of last ETL Job';
comment on column ETL_TABLES.JOB_RESULT           is 'Result of last ETL Job run';
comment on column ETL_TABLES.NOF_INSERTED_ROWS    is 'Number of inserted rows in the last Job run';
comment on column ETL_TABLES.NOF_UPDATED_ROWS     is 'Number of updated rows in the last Job run';
comment on column ETL_TABLES.NOF_DELETED_ROWS     is 'Number of deleted rows in the last Job run';

alter table ETL_TABLES add ( constraint ETL_TABLES_PK   primary key ( LOCAL_NAME ) );
  


Prompt *****************************************************************
Prompt **       F I L L I N G   U P    E T L _ T A B L E S            **
Prompt *****************************************************************

SET DEFINE OFF;
/*
Insert into ETL_TABLES (LOCAL_NAME,REMOTE_NAME,TRANSFER_TYPE) values ('ITEMS','ITEMS@MASTER','FULL');
*/



Prompt *****************************************************************
Prompt **               C R E A T I N G   T Y P E                     **
Prompt *****************************************************************


create or replace type T_STRING_LIST as table of varchar2( 32000 );
/



Prompt *****************************************************************
Prompt **          I N S T A L L I N G    E T L _ P K G               **
Prompt *****************************************************************


/*============================================================================================*/
create or replace package PKG_ETL as
   
    procedure ETL_TABLE ( I_TARGET_TABLE_NAME in varchar2 );

    procedure START_ETL ( I_TARGET_TABLE_NAME in varchar2 := null );

    function  ONE_TAB_DEF_DIFF ( I_TARGET_TABLE_NAME in varchar2 )  return T_STRING_LIST PIPELINED;

    function  ALL_TAB_DEF_DIFF return T_STRING_LIST PIPELINED;

    function  EXEC_SQL ( I_SQL  in varchar2 )  return T_STRING_LIST PIPELINED;

    function  ONE_TAB_DAT_DIFF ( I_TARGET_TABLE_NAME in varchar2 )  return T_STRING_LIST PIPELINED;

    function  ALL_TAB_DAT_DIFF return T_STRING_LIST PIPELINED;
    
end;
/

/*============================================================================================*/

create or replace package body PKG_ETL as

--******************************************************************************
--
--  DESCRIPTION:
--      This package can manage ETL process
--
--  AUTHORS:
--
--      FTH: Ferenc Toth
--
--  MODIFICATION HISTORY:
--
-- YYYY.MM.DD | GGGGNN   | DESCRIPTION
--------------+----------+-----------------------------------------------------------
-- 2022.01.05 | 1.0  FTH | Created
--------------+----------+-----------------------------------------------------------
--
--******************************************************************************

    ----------------------------------------
    function GET_PK ( I_TABLE_NAME  in varchar2
                    , I_PREFIX      in varchar2 := null
                    ) return varchar2 is
    ----------------------------------------
        L_PK   varchar2(4000);
        L_SEP  varchar2(40) := ',';
    begin
        for L_A_PK in ( select column_name
                          from user_constraints uc, user_cons_columns dbc
                         where uc.constraint_type  = 'P'
                           and dbc.constraint_name = uc.constraint_name
                           and dbc.table_name      = I_TABLE_NAME 
                           order by position 
                      ) 
        loop
            if nvl(length(L_PK),0)+length(L_A_PK.COLUMN_NAME) < 4000 then
                L_PK:=L_PK||I_PREFIX||L_A_PK.column_name||L_SEP;
            end if;
        end loop;
        L_PK := substr(L_PK,1,length(L_PK)-(length(L_SEP)));
        return L_PK;
    end;
    


    ----------------------------------------
    function GET_COLUMN_LIST ( I_TABLE_NAME  in varchar2 ) return varchar2 is
    ----------------------------------------
        L_CL   varchar2(4000);
        L_SEP  varchar2(40) := ',';
    begin
        for L_C in ( select column_name
                       from user_tab_columns
                      where table_name = I_TABLE_NAME 
                      order by column_id 
                   ) 
        loop
            L_CL:=L_CL||L_C.COLUMN_NAME||L_SEP;
        end loop;
        L_CL := substr(L_CL,1,length(L_CL)-(length(L_SEP)));
        return L_CL;
    end;


    ---------------------------------------------------------------------------
    procedure ETL_TABLE ( I_TARGET_TABLE_NAME in varchar2 ) as
    ---------------------------------------------------------------------------
        V_ETL_TABLE             ETL_TABLES%rowtype;
        V_JOB_RESULT            ETL_TABLES.JOB_RESULT%type := 'SUCCESS';
        V_NOF_INSERTED_ROWS     number := 0;
        V_NOF_UPDATED_ROWS      number := 0;
        V_NOF_DELETED_ROWS      number := 0;
        V_SQL                   varchar2(  2000 );
        V_BLOCK                 varchar2( 32000 );
        V_WHERE                 varchar2(  4000 );
        V_SQC                   number;
        V_SQE                   varchar2(2000);
        V_CNT                   number;
        
        ----------------------------------------
        procedure GENERATE_SQL as
        ----------------------------------------
            L_NUMBER        number;
            L_TIMESTAMP     timestamp;
            L_DATE          date;
            L_DATA_TYPE     varchar2( 200 );
        begin
            V_SQL := 'select '|| GET_COLUMN_LIST( I_TARGET_TABLE_NAME ) ||' from '||V_ETL_TABLE.REMOTE_NAME;

            if upper( V_ETL_TABLE.TRANSFER_TYPE ) not in ( 'FULL', 'MERGE' ) then
                
                select min( data_type ) 
                  into L_DATA_TYPE
                  from user_tab_columns 
                 where table_name  = I_TARGET_TABLE_NAME
                   and column_name = V_ETL_TABLE.TRANSFER_TYPE;

                if L_DATA_TYPE is null then

                    V_ETL_TABLE.TRANSFER_TYPE := 'FULL'; 

                elsif L_DATA_TYPE = 'DATE' then

                    execute immediate 'select max('||V_ETL_TABLE.TRANSFER_TYPE||') from '||I_TARGET_TABLE_NAME into L_DATE;
                    L_DATE := nvl(  L_DATE, sysdate - 10000 );
                    V_SQL := V_SQL || ' where '||V_ETL_TABLE.TRANSFER_TYPE||' > to_date( '''''|| to_char( L_DATE, 'YYYY.MM.DD HH24:MI:SS' ) ||''''' , ''''YYYY.MM.DD HH24:MI:SS'''' ) order by '||V_ETL_TABLE.TRANSFER_TYPE||' asc';

                elsif substr( L_DATA_TYPE, 1, 9 ) = 'TIMESTAMP' then

                    execute immediate 'select max('||V_ETL_TABLE.TRANSFER_TYPE||') from '||I_TARGET_TABLE_NAME into L_TIMESTAMP;
                    L_TIMESTAMP := nvl(  L_TIMESTAMP, systimestamp - 10000 );
                    V_SQL := V_SQL || ' where '||V_ETL_TABLE.TRANSFER_TYPE||' > to_timestamp( '''''|| to_char( L_TIMESTAMP, 'YYYY.MM.DD HH24:MI:SS.FF' ) ||''''' , ''''YYYY.MM.DD HH24:MI:SS.FF'''' ) order by '||V_ETL_TABLE.TRANSFER_TYPE||' asc';

                elsif L_DATA_TYPE = 'NUMBER' then

                    execute immediate 'select max('||V_ETL_TABLE.TRANSFER_TYPE||') from '||I_TARGET_TABLE_NAME into L_NUMBER;
                    L_NUMBER := nvl( L_NUMBER, 0 );
                    V_SQL := V_SQL || ' where '||V_ETL_TABLE.TRANSFER_TYPE||' > '|| to_char( L_NUMBER ) ||' order by '||V_ETL_TABLE.TRANSFER_TYPE||' asc';

                else

                    V_ETL_TABLE.TRANSFER_TYPE := 'FULL'; 

                end if;    

            end if;           
        end;


        ----------------------------------------
        procedure GENERATE_WHERE as
        ----------------------------------------
            L_NUMBER        number;
            L_TIMESTAMP     timestamp;
        begin
            V_WHERE := '( '||GET_PK( I_TARGET_TABLE_NAME )||' ) = ( select '||GET_PK( I_TARGET_TABLE_NAME, 'V_ROW.' )||' from dual )' ;
        end;


        ----------------------------------------
        procedure GENERATE_BLOCK as
        ----------------------------------------
            L_CRLF     varchar(2)  := chr(13)||chr(10);  -- $0D0A  CrLf
        begin
            if V_ETL_TABLE.TRANSFER_TYPE = 'FULL' then

                V_BLOCK :=            'declare'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_INS   number := 0;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_UPD   number := 0;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_DEL   number := 0;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_RES   ETL_TABLES.JOB_RESULT%type := ''SUCCESS'';'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_SQC   number;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_SQE   varchar2(2000);'||L_CRLF;  
                V_BLOCK := V_BLOCK || 'begin'||L_CRLF;
                V_BLOCK := V_BLOCK || '    begin'||L_CRLF;
                V_BLOCK := V_BLOCK || '        select count(*) into V_DEL from '||I_TARGET_TABLE_NAME||';'||L_CRLF;
                V_BLOCK := V_BLOCK || '        execute immediate ''truncate table '||I_TARGET_TABLE_NAME||''';'||L_CRLF;
                V_BLOCK := V_BLOCK || '        insert into '||I_TARGET_TABLE_NAME||' '||V_SQL||';'||L_CRLF;
                V_BLOCK := V_BLOCK || '        select count(*) into V_INS from '||I_TARGET_TABLE_NAME||';'||L_CRLF;
                V_BLOCK := V_BLOCK || '        commit;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    exception when others then'||L_CRLF;
                V_BLOCK := V_BLOCK || '        V_SQC := sys.standard.sqlcode;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        V_SQE := sys.standard.sqlerrm;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        V_RES := V_SQC||'' ''||V_SQE;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    end;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :INS := V_INS;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :UPD := V_UPD;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :DEL := V_DEL;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :RES := V_RES;'||L_CRLF;
                V_BLOCK := V_BLOCK || 'end;'||L_CRLF;

            else  
           
                V_BLOCK :=            'declare'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_DS    sys_refcursor;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_SQL   varchar(3000) := '''||V_SQL||''';'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_ROW   '||I_TARGET_TABLE_NAME||'%rowtype;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_INS   number := 0;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_UPD   number := 0;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_DEL   number := 0;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_RES   ETL_TABLES.JOB_RESULT%type := ''SUCCESS'';'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_SQC   number;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    V_SQE   varchar2(2000);'||L_CRLF;  
                V_BLOCK := V_BLOCK || 'begin'||L_CRLF;
                V_BLOCK := V_BLOCK || '    open V_DS for V_SQL;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    loop'||L_CRLF;
                V_BLOCK := V_BLOCK || '        fetch V_DS into V_ROW;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        exit when V_DS%notfound;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        begin'||L_CRLF;
                V_BLOCK := V_BLOCK || '            insert into '||I_TARGET_TABLE_NAME||' values V_ROW;'||L_CRLF;
                V_BLOCK := V_BLOCK || '            V_INS := V_INS + 1;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        exception '||L_CRLF;
                V_BLOCK := V_BLOCK || '            when dup_val_on_index then'||L_CRLF;
                V_BLOCK := V_BLOCK || '                update '||I_TARGET_TABLE_NAME||' set row = V_ROW where '||V_WHERE||';'||L_CRLF;
                V_BLOCK := V_BLOCK || '                V_UPD := V_UPD + 1;'||L_CRLF;
                V_BLOCK := V_BLOCK || '            when others then'||L_CRLF;
                V_BLOCK := V_BLOCK || '                V_SQC := sys.standard.sqlcode;'||L_CRLF;
                V_BLOCK := V_BLOCK || '                V_SQE := sys.standard.sqlerrm;'||L_CRLF;
                V_BLOCK := V_BLOCK || '                V_RES := V_SQC||'' ''||V_SQE;'||L_CRLF;
                V_BLOCK := V_BLOCK || '                exit;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        end;'||L_CRLF;
                V_BLOCK := V_BLOCK || '        commit;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    end loop;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    close V_DS;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :INS := V_INS;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :UPD := V_UPD;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :DEL := V_DEL;'||L_CRLF;
                V_BLOCK := V_BLOCK || '    :RES := V_RES;'||L_CRLF;
                V_BLOCK := V_BLOCK || 'end;'||L_CRLF;
                
            end if;
            
        end;
        
    begin
        select * into V_ETL_TABLE from ETL_TABLES where LOCAL_NAME = I_TARGET_TABLE_NAME;

        select count(*) into V_CNT from all_jobs where job = V_ETL_TABLE.JOB_ID and broken='N';
        
        if V_CNT = 1 and V_ETL_TABLE.JOB_STARTED is null then
        
            update ETL_TABLES
               set JOB_STARTED = systimestamp
             where LOCAL_NAME  = I_TARGET_TABLE_NAME;
            commit;
          
            GENERATE_SQL;
            
            GENERATE_WHERE;

            GENERATE_BLOCK;
            
            execute immediate V_BLOCK using out V_NOF_INSERTED_ROWS, out V_NOF_UPDATED_ROWS, out V_NOF_DELETED_ROWS, out V_JOB_RESULT;

            update ETL_TABLES
               set JOB_FINISHED      = systimestamp
                 , JOB_RESULT        = V_JOB_RESULT       
                 , NOF_INSERTED_ROWS = V_NOF_INSERTED_ROWS
                 , NOF_UPDATED_ROWS  = V_NOF_UPDATED_ROWS 
                 , NOF_DELETED_ROWS  = V_NOF_DELETED_ROWS               
             where LOCAL_NAME = I_TARGET_TABLE_NAME;
             commit;
             
        end if;

    exception when others then

        V_SQC := sys.standard.sqlcode;
        V_SQE := sys.standard.sqlerrm;

        update ETL_TABLES
           set JOB_FINISHED      = systimestamp
             , JOB_RESULT        = to_char( V_SQC )||' - '||V_SQE
             , NOF_INSERTED_ROWS = V_NOF_INSERTED_ROWS
             , NOF_UPDATED_ROWS  = V_NOF_UPDATED_ROWS 
             , NOF_DELETED_ROWS  = V_NOF_DELETED_ROWS               
         where LOCAL_NAME = I_TARGET_TABLE_NAME;
         commit;       
    end;


    ---------------------------------------------------------------------------
    procedure START_ETL ( I_TARGET_TABLE_NAME in varchar2 := null ) as
    ---------------------------------------------------------------------------
        V_JOB_ID    number;
        V_WHAT      varchar2( 2000 );
        V_SLEEP     number := 1;   --  wait some seconds between two job starting
        V_CNT       number;
    begin     
        for L_R in ( select * from ETL_TABLES where LOCAL_NAME = nvl( I_TARGET_TABLE_NAME, LOCAL_NAME ) )
        loop
        
            V_WHAT := 'PKG_ETL.ETL_TABLE( '''||L_R.LOCAL_NAME||''' );'; 
        
            select count(*) into V_CNT from all_jobs where job = L_R.JOB_ID and broken='N';
        
            if V_CNT = 0 then
            
                dbms_job.submit( job       => V_JOB_ID
                               , what      => V_WHAT
                               , next_date => sysdate - 1
                               , interval  => null
                               );                                    
            
                update ETL_TABLES
                   set JOB_STARTED       = null
                     , JOB_ID            = V_JOB_ID
                     , JOB_FINISHED      = null
                     , JOB_RESULT        = null
                     , NOF_INSERTED_ROWS = null
                     , NOF_UPDATED_ROWS  = null
                     , NOF_DELETED_ROWS  = null              
                 where LOCAL_NAME = L_R.LOCAL_NAME;
                 
                commit;

                $IF DBMS_DB_VERSION.VERSION < 18 $THEN 
                    sys.dbms_lock.sleep( V_SLEEP );
                $ELSE
                    sys.dbms_session.sleep( V_SLEEP );
                $END                 
        
            end if;    
        
            
        end loop;             
    end;


    ---------------------------------------------------------------------------
    function ONE_TAB_DEF_DIFF ( I_TARGET_TABLE_NAME in varchar2 )  return T_STRING_LIST PIPELINED as
    ---------------------------------------------------------------------------
    -- use :
    -- select * from PKG_ETL.ONE_TAB_DEF_DIFF( 'INVOICE_FILE');
    ---------------------------------------------------------------------------
        V_ETL_TABLE             ETL_TABLES%rowtype;
        V_R_TAB_NAME            varchar2( 300 );
        V_MS_POS                number;
        V_DB_LINK               varchar2( 300 );
        V_DS_L                  sys_refcursor;
        V_DS_R                  sys_refcursor;
        V_N                     number := 0;
        V_L_COLUMN_NAME         varchar2( 300 );
        V_L_DATA_TYPE           varchar2( 300 );
        V_L_DATA_LENGTH         number;
        V_L_DATA_PRECISION      number;
        V_L_DATA_SCALE          number;
        V_R_COLUMN_NAME         varchar2( 300 );
        V_R_DATA_TYPE           varchar2( 300 );
        V_R_DATA_LENGTH         number;
        V_R_DATA_PRECISION      number;
        V_R_DATA_SCALE          number;
    begin
        select * into V_ETL_TABLE from ETL_TABLES where LOCAL_NAME = I_TARGET_TABLE_NAME;

        V_MS_POS     := instr( V_ETL_TABLE.REMOTE_NAME, '@' );
        if V_MS_POS = 0 then
            V_R_TAB_NAME := V_ETL_TABLE.REMOTE_NAME;
            V_DB_LINK    := null;           
        else    
            V_R_TAB_NAME := substr( V_ETL_TABLE.REMOTE_NAME, 1       , V_MS_POS - 1 );
            V_DB_LINK    := substr( V_ETL_TABLE.REMOTE_NAME, V_MS_POS, 300          );
        end if;    

        PIPE ROW( 'Source : '||V_ETL_TABLE.REMOTE_NAME );
        PIPE ROW( 'Target : '||V_ETL_TABLE.LOCAL_NAME  );

        open V_DS_L for 'select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE from user_tab_columns where table_name = '''||V_ETL_TABLE.LOCAL_NAME||''' order by column_name';
        
        fetch V_DS_L into V_L_COLUMN_NAME, V_L_DATA_TYPE, V_L_DATA_LENGTH, V_L_DATA_PRECISION, V_L_DATA_SCALE;
        if V_DS_L%notfound then
            PIPE ROW( ' ' );            
            PIPE ROW( ' ! Table (target) does not exist locally.' );
            PIPE ROW( '-----------------------------------------------------------' );
            close V_DS_L;
            return;
        end if;

        open V_DS_R for 'select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE from user_tab_columns'||V_DB_LINK||' where table_name = '''||V_R_TAB_NAME||''' order by column_name';

        fetch V_DS_R into V_R_COLUMN_NAME, V_R_DATA_TYPE, V_R_DATA_LENGTH, V_R_DATA_PRECISION, V_R_DATA_SCALE;
        if V_DS_R%notfound then
            PIPE ROW( ' ' );            
            PIPE ROW( ' ! Table (source) does not exist remotely.' );
            PIPE ROW( '-----------------------------------------------------------' );
            close V_DS_R;
            return;
        end if;

        loop

            if V_L_COLUMN_NAME = V_R_COLUMN_NAME then

                if   V_L_DATA_TYPE      != V_R_DATA_TYPE 
                  or V_L_DATA_LENGTH    != V_R_DATA_LENGTH 
                  or V_L_DATA_PRECISION != V_R_DATA_PRECISION
                  or V_L_DATA_SCALE     != V_L_DATA_SCALE then

                    PIPE ROW( ' ' );
                    PIPE ROW( ' ! Column '||V_L_COLUMN_NAME|| ' definitions are different:' );
                    PIPE ROW( '   Source : Type: '||V_R_DATA_TYPE||', Length: '||to_char( V_R_DATA_LENGTH )||', Precision: '||to_char( V_R_DATA_PRECISION )||', Scale: '||to_char( V_R_DATA_SCALE ) );
                    PIPE ROW( '   Target : Type: '||V_L_DATA_TYPE||', Length: '||to_char( V_L_DATA_LENGTH )||', Precision: '||to_char( V_L_DATA_PRECISION )||', Scale: '||to_char( V_L_DATA_SCALE ) );                    

                    V_N := V_N + 1;

                end if; 

                if V_DS_L%isopen then       
                    fetch V_DS_L into V_L_COLUMN_NAME, V_L_DATA_TYPE, V_L_DATA_LENGTH, V_L_DATA_PRECISION, V_L_DATA_SCALE;
                    if V_DS_L%notfound then
                        close V_DS_L;
                    end if;
                end if;    

                if V_DS_R%isopen then       
                    fetch V_DS_R into V_R_COLUMN_NAME, V_R_DATA_TYPE, V_R_DATA_LENGTH, V_R_DATA_PRECISION, V_R_DATA_SCALE;
                    if V_DS_R%notfound then
                        close V_DS_R;
                    end if;
                end if;    

            elsif V_L_COLUMN_NAME > V_R_COLUMN_NAME or not V_DS_L%isopen then

                PIPE ROW( ' ' );
                PIPE ROW( ' ! Column : '||V_R_COLUMN_NAME|| ' is missing locally' );
                PIPE ROW( '   Type   : '||V_R_DATA_TYPE||', Length: '||to_char( V_R_DATA_LENGTH )||', Precision: '||to_char( V_R_DATA_PRECISION )||', Scale: '||to_char( V_R_DATA_SCALE ) );

                V_N := V_N + 1;

                if V_DS_R%isopen then       
                    fetch V_DS_R into V_R_COLUMN_NAME, V_R_DATA_TYPE, V_R_DATA_LENGTH, V_R_DATA_PRECISION, V_R_DATA_SCALE;
                    if V_DS_R%notfound then
                        close V_DS_R;
                    end if;
                end if;    

            elsif V_L_COLUMN_NAME < V_R_COLUMN_NAME or not V_DS_R%isopen then

                PIPE ROW( ' ' );
                PIPE ROW( ' ! Column : '||V_L_COLUMN_NAME|| ' is missing remotely' );
                PIPE ROW( '   Type   : '||V_L_DATA_TYPE||', Length: '||to_char( V_L_DATA_LENGTH )||', Precision: '||to_char( V_L_DATA_PRECISION )||', Scale: '||to_char( V_L_DATA_SCALE ) );                    

                V_N := V_N + 1;

                if V_DS_L%isopen then       
                    fetch V_DS_L into V_L_COLUMN_NAME, V_L_DATA_TYPE, V_L_DATA_LENGTH, V_L_DATA_PRECISION, V_L_DATA_SCALE;
                    if V_DS_L%notfound then
                        close V_DS_L;
                    end if;
                end if;    

            end if;

            exit when not V_DS_L%isopen and not V_DS_R%isopen;

        end loop;
        
        if V_N = 0 then
            PIPE ROW( ' = The definition of tables are identical.' );
        end if;

        PIPE ROW( '-----------------------------------------------------------' );

        return;
    end;


    ---------------------------------------------------------------------------
    function ALL_TAB_DEF_DIFF return T_STRING_LIST PIPELINED as
    ---------------------------------------------------------------------------
    -- use :
    -- select * from PKG_ETL.ALL_TAB_DEF_DIFF();
    ---------------------------------------------------------------------------
    begin
        PIPE ROW( '-----------------------------------------------------------' );

        for L_R in ( select LOCAL_NAME from ETL_TABLES order by LOCAL_NAME )
        loop

            for L_C in ( select * from table( PKG_ETL.ONE_TAB_DEF_DIFF( L_R.LOCAL_NAME ) ) )
            loop

                PIPE ROW( L_C.column_value );

            end loop;

        end loop;

        return;
    end;


    ---------------------------------------------------------------------------
    function EXEC_SQL ( I_SQL  in varchar2 )  return T_STRING_LIST PIPELINED as
    ---------------------------------------------------------------------------
        V_DATA              sys_refcursor;
        V_CURSOR            integer;
        V_COLUMNS           integer;
        V_DESC              dbms_sql.desc_tab2;
        V_STR               varchar2( 4000 );
        V_CV                varchar2( 4000 );
        V_NUM               number;
        V_SEP               varchar2(10) := chr( 9 );  -- horizontal tab
    begin
        open V_DATA for I_SQL;

        V_CURSOR := dbms_sql.to_cursor_number( V_DATA );

        dbms_sql.describe_columns2( V_CURSOR, V_COLUMNS, V_DESC );

        for V_I in 1..V_COLUMNS 
        loop
            if length( V_STR ) < 4000 or V_STR is null then
                V_STR := substr( V_STR || V_DESC( V_I ).col_name || V_SEP, 1, 4000 );
            end if;
        end loop;

        PIPE ROW( V_STR );
        V_STR := '';
        
        for V_I in 1..V_COLUMNS 
        loop
            dbms_sql.define_column( V_CURSOR, V_I, V_STR, 32000 );
        end loop;

        V_STR := '';
        while dbms_sql.fetch_rows( V_CURSOR ) > 0 
        loop
            for V_I in 1..V_COLUMNS 
            loop
                dbms_sql.column_value( V_CURSOR, V_I, V_CV );
                if length( V_STR ) < 4000 or V_STR is null then
                    V_STR := substr( V_STR || V_CV || V_SEP, 1, 4000 );
                end if;
            end loop;
            PIPE ROW( V_STR );
            V_STR := '';
        end loop;

        dbms_sql.close_cursor( V_CURSOR );
        
        return;
    end;

    ---------------------------------------------------------------------------
    function ONE_TAB_DAT_DIFF ( I_TARGET_TABLE_NAME in varchar2 )  return T_STRING_LIST PIPELINED as
    ---------------------------------------------------------------------------
    -- use :
    -- select * from PKG_ETL.ONE_TAB_DAT_DIFF( 'INVOICE_FILE');
    ---------------------------------------------------------------------------
        V_ETL_TABLE             ETL_TABLES%rowtype;
        V_CL                    varchar2( 4000 );
    begin
        select * into V_ETL_TABLE from ETL_TABLES where LOCAL_NAME = I_TARGET_TABLE_NAME;

        V_CL := GET_COLUMN_LIST( V_ETL_TABLE.LOCAL_NAME );
        
        PIPE ROW( ' ' );
        PIPE ROW( V_ETL_TABLE.LOCAL_NAME||' minus '||V_ETL_TABLE.REMOTE_NAME );
        PIPE ROW( ' ' );
        
        for L_C in ( select * from table( PKG_ETL.EXEC_SQL ( 'select '|| V_CL ||' from '||V_ETL_TABLE.LOCAL_NAME|| ' minus select '|| V_CL ||' from '||V_ETL_TABLE.REMOTE_NAME ) ) )
        loop

            PIPE ROW( L_C.column_value );

        end loop;

        PIPE ROW( '-----------------------------------------------------------' );
        PIPE ROW( ' ' );
        PIPE ROW( V_ETL_TABLE.REMOTE_NAME||' minus '||V_ETL_TABLE.LOCAL_NAME );
        PIPE ROW( ' ' );

        for L_C in ( select * from table( PKG_ETL.EXEC_SQL ( 'select '|| V_CL ||' from '||V_ETL_TABLE.REMOTE_NAME|| ' minus select '|| V_CL ||' from '||V_ETL_TABLE.LOCAL_NAME ) ) )
        loop

            PIPE ROW( L_C.column_value );

        end loop;

        PIPE ROW( '=============================================================================' );

        return;
    end;



    ---------------------------------------------------------------------------
    function ALL_TAB_DAT_DIFF return T_STRING_LIST PIPELINED as
    ---------------------------------------------------------------------------
    -- use :
    -- select * from PKG_ETL.ALL_TAB_DAT_DIFF();
    ---------------------------------------------------------------------------
    begin
        PIPE ROW( '=============================================================================' );

        for L_R in ( select LOCAL_NAME from ETL_TABLES order by LOCAL_NAME )
        loop

            for L_C in ( select * from table( PKG_ETL.ONE_TAB_DAT_DIFF( L_R.LOCAL_NAME ) ) )
            loop

                PIPE ROW( L_C.column_value );

            end loop;

        end loop;

        return;
    end;

end;
/


