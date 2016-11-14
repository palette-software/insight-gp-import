-- Create dummy table
CREATE READABLE EXTERNAL TABLE palette.dummy_to_create_ext_error_table
(
    dummy int
)
LOCATION (
    'gpfdist://localhost:18001/*/dummy-*.csv.gz'
)
FORMAT 'TEXT' (delimiter ';' null '\\N' escape '\\' header)
ENCODING 'UTF8'
LOG ERRORS INTO "palette"."ext_error_table" SEGMENT REJECT LIMIT 1000 ROWS;

-- Drop the table
drop external table palette.dummy_to_create_ext_error_table;
