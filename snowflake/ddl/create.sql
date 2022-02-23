////// create table

//// variants

-- create table ... as select()     --> creates CTA (populated table)

-- create table ... using template  --> creates a table with the column definitions from a set template

-- create table ... like            --> creates an empty copy of an existing table

-- create table ... clone           --> creates a clone of an existing table

----------------------------------------------------------------------------------------------------

////Syntax

CREATE [ OR REPLACE ]
    [ { [ LOCAL | GLOBAL ] TEMPORARY | VOLATILE } | TRANSIENT ]
    TABLE [ IF NOT EXISTS ] <table_name>
    ( <col_name> <col_type>
                             [ COLLATE '<collation_specification>' ]
                                /* COLLATE is supported only for text data types (VARCHAR and synonyms) */
                             [ { DEFAULT <expr>
                               | { AUTOINCREMENT | IDENTITY } [ ( <start_num> , <step_num> ) | START <num> INCREMENT <num> ] } ]
                                /* AUTOINCREMENT (or IDENTITY) is supported only for numeric data types (NUMBER, INT, FLOAT, etc.) */
                             [ NOT NULL ]
                             [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ]
                             [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
                             [ inlineConstraint ]
      [ , <col_name> <col_type> [ ... ] ]
      [ , outoflineConstraint ]
      [ , ... ] )
  [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
  [ STAGE_FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
                           | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
  [ STAGE_COPY_OPTIONS = ( copyOptions ) ]
  [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
  [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
  [ CHANGE_TRACKING = { TRUE | FALSE } ]
  [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
  [ COPY GRANTS ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
  [ COMMENT = '<string_literal>' ]


//// variants

-- create table ... as select()     --> creates CTA (populated table)

-- create table ... using template  --> creates a table with the column definitions from a set template

-- create table ... like            --> creates an empty copy of an existing table

-- create table ... clone           --> creates a clone of an existing table

----------------------------------------------------------------------------------------------------

//// types

-- temporary --> A temporary table persists only for the duration of the user session in which it was created and is not visible to other users.

-- transient --> A transient table exists until explicitly dropped and is visible to any user with the appropriate privileges.
