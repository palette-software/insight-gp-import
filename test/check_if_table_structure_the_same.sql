-- With superuser: 
-- set search_path = 'palette'; set role = palette;
drop schema py_load_tables_test cascade;

create schema py_load_tables_test;
grant all on schema py_load_tables_test to palette_etl_user;
grant all on schema py_load_tables_test to palette_palette_updater;
;

-- With palette_palette_updater: 
-- set search_path = 'py_load_tables_test'; set role = palette_palette_updater;

CREATE TABLE "p_threadinfo"(
 "p_id" BigSerial NOT NULL,
 "threadinfo_id" Bigint,
 "host_name" Character varying(255),
 "process_name" Character varying(255),
 "ts" Timestamp,
 "ts_rounded_15_secs" Timestamp,
 "ts_date" Date,
 "process_id" Bigint,
 "thread_id" Bigint,
 "start_ts" Timestamp,
 "cpu_time_ticks" Bigint,
 "cpu_time_delta_ticks" Bigint,
 "ts_interval_ticks" Bigint,
 "cpu_core_consumption" Double precision,
 "memory_usage_bytes" Bigint,
 "memory_usage_delta_bytes" Bigint,
 "is_thread_level" Character varying(1)
)
WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN, COMPRESSTYPE=QUICKLZ)
DISTRIBUTED BY (host_name, process_id, thread_id)
PARTITION BY RANGE (ts_rounded_15_secs)
SUBPARTITION BY LIST (host_name)
SUBPARTITION TEMPLATE (SUBPARTITION init VALUES ('init')
WITH (appendonly=true, orientation=column, compresstype=quicklz))
(PARTITION "10010101" START (date '1001-01-01') INCLUSIVE
	END (date '1001-01-02') EXCLUSIVE 
WITH (appendonly=true, orientation=column, compresstype=quicklz)
)

;

-- source /usr/local/greenplum-db/greenplum_path.sh
--/usr/local/greenplum-db/bin/gpfdist -p 18011 -v -m 268435456


-- manage partitions...
-- get max ts ...
CREATE OR REPLACE FUNCTION get_max_ts(p_schema_name text, p_table_name text) RETURNS timestamp 
AS $$
declare
	v_sql text;	
	v_max_timestamp timestamp;
	v_column_name varchar;
	rec record;
begin

		v_max_timestamp := null;
		v_column_name := case 	when p_table_name = 'p_cpu_usage_report' then 'cpu_usage_ts_rounded_15_secs'
						when p_table_name in ('p_threadinfo', 'p_cpu_usage') then 'ts_rounded_15_secs'
						when p_table_name = 'p_cpu_usage_agg_report' then 'timestamp_utc'
						when p_table_name = 'p_interactor_session_agg_cpu_usage' then 'session_start_ts'
						else 'ts'
					end;		
		for rec in (select
						a.e
					from
						(select 'select max(' || v_column_name || ') from ' || min(schemaname) || '."' || parentpartitiontablename || '"' as e
						from pg_partitions
						where tablename = p_table_name and
								schemaname = p_schema_name
						group by parentpartitiontablename
						) a
						where
							a.e is not null
						order by e desc
					)
					
		loop
			execute rec.e into v_max_timestamp;
			exit when v_max_timestamp is not null;
		end loop;					
	
		if (v_max_timestamp is null)
			then execute 'select coalesce(max(' || v_column_name || '), date''1001-01-01'') from ' || p_schema_name || '.'  || p_table_name into v_max_timestamp;
		end if;

		return v_max_timestamp;		
end;
$$ LANGUAGE plpgsql;

create or replace function get_max_ts_date(p_schema_name text, p_table_name text) returns date
as $$
begin
	execute 'set local search_path = ' || p_schema_name;
	return get_max_ts(p_schema_name, p_table_name)::date;				
end;
$$ language plpgsql;


CREATE or replace function manage_single_range_partitions(p_schema_name text, p_table_name text) returns int
AS $$
declare
	v_sql_cur text;
	c refcursor;
	rec record;
	v_sql text;
	v_max_ts_date_p_threadinfo text;
	v_max_ts_date_p_serverlogs text;
	v_subpart_cols text;
    v_table_name text;    
BEGIN

		v_subpart_cols := '';
		execute 'set local search_path = ' || p_schema_name;
		
        v_table_name := lower(p_table_name);
        
		v_sql_cur := '';
		v_sql := '';
		
		if v_table_name in ('p_cpu_usage_agg_report') then --year
			v_sql_cur := 'select distinct timestamp_utc::date d from ' || 's' || ltrim(v_table_name, 'p');
		elseif v_table_name in ('p_interactor_session') then --year
			v_sql_cur := 'select distinct session_start_ts::date d from ' || 's' || ltrim(v_table_name, 'p');
		elseif v_table_name in ('p_process_class_agg_report') then --month
			v_sql_cur := 'select distinct ts_rounded_15_secs::date d from '|| 's' || ltrim(v_table_name, 'p');
		elseif v_table_name in ('p_cpu_usage_bootstrap_rpt') then --month
			v_sql_cur := 'select distinct cpu_usage_ts_rounded_15_secs::date d from '|| 's' || ltrim(v_table_name, 'p');
		elseif v_table_name in ('p_serverlogs_bootstrap_rpt') then --day
			v_sql_cur := 'select distinct start_ts::date d from '|| 's' || ltrim(v_table_name, 'p');
        elseif v_table_name in ('plainlogs') then --day
			v_sql_cur := 'select distinct ts::date d from ' || 'ext_plainlogs';
		end if;
		
		
		open c for execute (v_sql_cur);
			loop
				  fetch c into rec;
				  exit when not found;
				  
				  v_sql := 'ALTER TABLE #schema_name#.#table_name# 
				  		         ADD PARTITION "#partition_name#" START (date''#start_date#'') INCLUSIVE END (date''#end_date#'') EXCLUSIVE WITH (appendonly=true, orientation=column, compresstype=quicklz)';

				  v_sql := replace(v_sql, '#schema_name#', p_schema_name);
				  v_sql := replace(v_sql, '#table_name#', v_table_name);
				if v_table_name in ('p_cpu_usage_agg_report', 'p_interactor_session') then --year
					v_sql := replace(v_sql, '#partition_name#', to_char(rec.d, 'yyyy'));
					v_sql := replace(v_sql, '#start_date#', to_char(rec.d, 'yyyy') || '-01-01');
					v_sql := replace(v_sql, '#end_date#', to_char(rec.d + interval'1 year', 'yyyy') || '-01-01');
				elseif v_table_name in ('p_process_class_agg_report', 'p_cpu_usage_bootstrap_rpt') then --month
					v_sql := replace(v_sql, '#partition_name#', to_char(rec.d, 'yyyymm'));
					v_sql := replace(v_sql, '#start_date#', to_char(rec.d, 'yyyy-mm') || '-01');
					v_sql := replace(v_sql, '#end_date#', to_char(rec.d + interval'1 month', 'yyyy-mm') || '-01');
				elseif v_table_name in ('p_serverlogs_bootstrap_rpt', 'plainlogs') then --day
					v_sql := replace(v_sql, '#partition_name#', to_char(rec.d, 'yyyymmdd'));
					v_sql := replace(v_sql, '#start_date#', to_char(rec.d, 'yyyy-mm-dd'));
					v_sql := replace(v_sql, '#end_date#', to_char(rec.d + 1, 'yyyy-mm-dd'));
				end if;
				
				begin
					if v_table_name in ('p_cpu_usage_agg_report', 'p_interactor_session') then --year
						if (not does_part_exist(p_schema_name, v_table_name, to_char(rec.d, 'yyyy'))) then
					  		execute v_sql;
						end if;
						--exception when duplicate_object
						--	then null;
					elseif v_table_name in ('p_process_class_agg_report', 'p_cpu_usage_bootstrap_rpt') then --month
						if (not does_part_exist(p_schema_name, v_table_name, to_char(rec.d, 'yyyymm'))) then
					  		execute v_sql;
						end if;
						--exception when duplicate_object
						--	then null;
					elseif v_table_name in ('p_serverlogs_bootstrap_rpt', 'plainlogs') then --day
						if (not does_part_exist(p_schema_name, v_table_name, to_char(rec.d, 'yyyymmdd'))) then
					  		execute v_sql;
						end if;
						--exception when duplicate_object
						--	then null;
					end if;
				end;
				
				 raise notice 'I: %', v_sql;
			end loop;
			close c;
	
	return 0;

END;
$$ LANGUAGE plpgsql;

CREATE or replace function manage_multi_range_partitions(p_schema_name text, p_table_name text) returns int
AS $$
declare
	v_sql_cur text;
	c refcursor;
	rec record;
	v_sql text;
	v_max_ts_date_p_threadinfo text;
	v_max_ts_date_p_serverlogs text;
	v_subpart_cols text;
    v_table_name text;
BEGIN

		v_subpart_cols := '';
		execute 'set local search_path = ' || p_schema_name;				

        v_table_name := lower(p_table_name);        
        
		v_sql_cur := 'select to_char((select #schema_name#.get_max_ts_date(''#schema_name#'', ''p_threadinfo'')), ''yyyy-mm-dd'')';
		v_sql_cur := replace(v_sql_cur, '#schema_name#', p_schema_name);			
		execute v_sql_cur into v_max_ts_date_p_threadinfo;
		v_max_ts_date_p_threadinfo := 'date''' || v_max_ts_date_p_threadinfo || '''';
						
		v_sql_cur := '';
		v_sql := '';
		
		if v_table_name in ('threadinfo') then
			v_sql_cur := 'select distinct host_name::text as host_name from #schema_name#.ext_threadinfo';
		elseif v_table_name in ('serverlogs') then
			v_sql_cur := 'select distinct host_name::text as host_name from #schema_name#.ext_serverlogs';
		elseif v_table_name in ('p_serverlogs') then
			v_sql_cur := 'select distinct host_name::text as host_name from #schema_name#.s_serverlogs						   
			';
		elseif v_table_name in ('p_threadinfo') then
			v_sql_cur := 'select distinct host_name::text as host_name from #schema_name#.threadinfo 
							where ts >= #max_ts_date_p_threadinfo# - interval ''1 hour''
						';
		elseif v_table_name in ('p_cpu_usage') then
			v_sql_cur := 'select distinct host_name::text as host_name from #schema_name#.s_cpu_usage';
			
		elseif v_table_name in ('p_cpu_usage_report') then
					v_sql_cur := 'select distinct cpu_usage_host_name::text as host_name from #schema_name#.s_cpu_usage_report';						
		end if;
		
		v_sql_cur := v_sql_cur || 
					' union
						select distinct partitionname 
						from pg_partitions
						where 
							schemaname = ''#schema_name#'' and
							tablename = ''#table_name#'' and
							partitionlevel = 1 and
							partitionname not in (''init'', ''new_host'')
					order by 1';
					
		v_sql_cur := replace(v_sql_cur, '#schema_name#', p_schema_name);
		v_sql_cur := replace(v_sql_cur, '#table_name#', v_table_name);
		v_sql_cur := replace(v_sql_cur, '#max_ts_date_p_threadinfo#', v_max_ts_date_p_threadinfo);		
		
		v_sql := 'ALTER TABLE #schema_name#.#table_name# SET SUBPARTITION TEMPLATE (';		

		open c for execute (v_sql_cur);
		loop
			  fetch c into rec;
			  exit when not found;
			  
			  v_sql := v_sql || ' SUBPARTITION "#host_name#" VALUES (''#host_name#'') WITH (appendonly=true, orientation=column, compresstype=quicklz),';
			  v_sql := replace(v_sql, '#host_name#', rec.host_name);
			  v_subpart_cols := v_subpart_cols || ',' || lower(rec.host_name);
		end loop;
		close c;
		
		v_sql := replace(v_sql, '#schema_name#', p_schema_name);
		v_sql := replace(v_sql, '#table_name#', v_table_name);
		v_sql := v_sql || ' DEFAULT SUBPARTITION new_host WITH (appendonly=true, orientation=column, compresstype=quicklz))';
		
		v_subpart_cols := ltrim(v_subpart_cols, ',');
		
		raise notice 'I: %', v_sql;
				
		if (v_sql like '%SUBPARTITION%VALUES%' and not is_subpart_template_same(p_schema_name, v_table_name, v_subpart_cols))
			then
				execute v_sql;
		end if;
		
		v_sql_cur := '';
		v_sql := '';
		
		if v_table_name in ('threadinfo') then
			v_sql_cur := 'select distinct ts::date d from #schema_name#.ext_threadinfo
							 order by 1';
		elseif v_table_name in ('serverlogs') then
			v_sql_cur := 'select distinct ts::date d from #schema_name#.ext_serverlogs';
		elseif v_table_name in ('p_serverlogs') then
			v_sql_cur := 'select distinct ts::date d from #schema_name#.s_serverlogs						  
					      order by 1';
		elseif v_table_name in ('p_threadinfo') then
			v_sql_cur := 'select distinct poll_cycle_ts::date d from #schema_name#.threadinfo
						   where ts >= #max_ts_date_p_threadinfo# - interval''1 hour''
						   order by 1';
		elseif v_table_name in ('p_cpu_usage') then
			v_sql_cur := 'select distinct ts_rounded_15_secs::date d from #schema_name#.s_cpu_usage							
							order by 1';			
		elseif v_table_name in ('p_cpu_usage_report') then
					v_sql_cur := 'select distinct cpu_usage_ts_rounded_15_secs::date d from #schema_name#.s_cpu_usage_report							
							order by 1';						
		end if;
		
		v_sql_cur := replace(v_sql_cur, '#schema_name#', p_schema_name);
		v_sql_cur := replace(v_sql_cur, '#table_name#',  v_table_name);
		v_sql_cur := replace(v_sql_cur, '#max_ts_date_p_threadinfo#', v_max_ts_date_p_threadinfo);		

		
		open c for execute (v_sql_cur);
			loop
				  fetch c into rec;
				  exit when not found;
				  
				  v_sql := 'ALTER TABLE #schema_name#.#table_name# 
				  		         ADD PARTITION "#partition_name#" START (date''#start_date#'') INCLUSIVE END (date''#end_date#'') EXCLUSIVE WITH (appendonly=true, orientation=column, compresstype=quicklz)';
						
				  v_sql := replace(v_sql, '#schema_name#', p_schema_name);
				  v_sql := replace(v_sql, '#table_name#', v_table_name);		
				  v_sql := replace(v_sql, '#partition_name#', to_char(rec.d, 'yyyymmdd'));
				  v_sql := replace(v_sql, '#start_date#', to_char(rec.d, 'yyyy-mm-dd'));
				  v_sql := replace(v_sql, '#end_date#', to_char(rec.d + 1, 'yyyy-mm-dd'));				  			  			  				  
				  
				begin
					if (not does_part_exist(p_schema_name, v_table_name, to_char(rec.d, 'yyyymmdd'))) then
				  		execute v_sql;
					end if;
				exception when duplicate_object
						then null;
				end;
				  				  
				 raise notice 'I: %', v_sql;
			end loop;
			close c;
	
	return 0;

END;
$$ LANGUAGE plpgsql;


CREATE or replace function manage_partitions(p_schema_name text, p_table_name text) returns int
AS $$
BEGIN
		
		execute 'set search_path = ' || p_schema_name;
		
		if lower(p_table_name) in ('threadinfo', 'serverlogs', 'p_serverlogs', 'p_threadinfo', 'p_cpu_usage', 'p_cpu_usage_report') then
			perform manage_multi_range_partitions(p_schema_name, p_table_name);
		elsif lower(p_table_name) in ('plainlogs', 'p_cpu_usage_agg_report', 'p_interactor_session', 'p_process_class_agg_report', 'p_cpu_usage_bootstrap_rpt', 'p_serverlogs_bootstrap_rpt') then
			perform manage_single_range_partitions(p_schema_name, p_table_name);
		else raise notice '--WARNING: No partition management happened for % - wrong table specified?--', p_table_name;
		end if;
		
	return 0;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION py_load_tables_test.is_subpart_template_same(p_schema_name text, p_table_name text, p_subpart_cols text)
RETURNS boolean AS
$BODY$
declare
	v_subpart_cols text;
	
BEGIN

	v_subpart_cols := '';
	
	select lower(string_agg(partitionname, ',')) into v_subpart_cols
	from pg_partition_templates
	where 
		partitionname <> 'new_host' and
		schemaname = p_schema_name and
		tablename = p_table_name;
		
	if lower(p_subpart_cols) = v_subpart_cols then
		return true;
	else
		return false;
	end if;
	
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY INVOKER;


-- not needed for the first loadtable run, but this is a function too
CREATE OR REPLACE FUNCTION py_load_tables_test.does_part_exist(p_schema_name text, p_table_name text, p_part_name text)
RETURNS boolean AS
$BODY$
declare
	v_cnt int;
BEGIN

	v_cnt := 0;
	select count(1) into v_cnt
	from 
		pg_partitions 
	where 
		schemaname = p_schema_name and
		tablename = p_table_name and 
		partitionname = p_part_name;
		
	if v_cnt > 0 then
		return true;
	else
		return false;
	end if;
	
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY INVOKER;




----- after the first loadtables, run this

-- Recreate agent tables
alter table threadinfo rename to threadinfo_old;

CREATE TABLE threadinfo
(LIKE threadinfo_old INCLUDING DEFAULTS)
WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN, COMPRESSTYPE=QUICKLZ)
DISTRIBUTED BY (p_id)
PARTITION BY RANGE (ts)
SUBPARTITION BY LIST (host_name)
SUBPARTITION TEMPLATE (SUBPARTITION init VALUES ('init')
WITH (appendonly=true, orientation=column, compresstype=quicklz))
(PARTITION "10010101" START (date '1001-01-01') INCLUSIVE
        END (date '1001-01-02') EXCLUSIVE
WITH (appendonly=true, orientation=column, compresstype=quicklz)
);

alter sequence threadinfo_p_id_seq owned by threadinfo.p_id;
drop table threadinfo_old;


alter table serverlogs rename to serverlogs_old;

CREATE TABLE serverlogs
(LIKE serverlogs_old INCLUDING DEFAULTS)
WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN, COMPRESSTYPE=QUICKLZ)
DISTRIBUTED BY (p_id)
PARTITION BY RANGE (ts)
SUBPARTITION BY LIST (host_name)
SUBPARTITION TEMPLATE (SUBPARTITION init VALUES ('init')
WITH (appendonly=true, orientation=column, compresstype=quicklz))
(PARTITION "10010101" START (date '1001-01-01') INCLUSIVE
        END (date '1001-01-02') EXCLUSIVE
WITH (appendonly=true, orientation=column, compresstype=quicklz)
);

alter sequence serverlogs_p_id_seq owned by serverlogs.p_id;
drop table serverlogs_old;


alter table plainlogs rename to plainlogs_old;

CREATE TABLE plainlogs (LIKE plainlogs_old INCLUDING DEFAULTS)
WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN, COMPRESSTYPE=QUICKLZ)
DISTRIBUTED BY (p_id)
PARTITION BY RANGE (ts)
(PARTITION "10010101"
    START (date '1001-01-01') INCLUSIVE
        END (date '1001-01-02') EXCLUSIVE
WITH (appendonly=true, orientation=column, compresstype=quicklz)
);

alter sequence plainlogs_p_id_seq owned by plainlogs.p_id;
drop table plainlogs_old;




------------------
-----------






CREATE OR REPLACE FUNCTION py_load_tables_test.get_struct_diff_for_table(p_tablename text, p_schema_a text, p_schema_b text) RETURNS
setof record AS $$
declare

    c cursor is     
    
with t_new as (
    select *
    from 
        information_schema.columns c
    where  
        c.table_name = p_tablename and
        c.table_schema = p_schema_a
    order by table_schema, column_name),
t_orig as (
    select *
    from 
        information_schema.columns c
    where  
        c.table_name = p_tablename and
        c.table_schema = p_schema_b
    order by table_schema, column_name)
    
 select 
 		table_name::character varying,
		ntable_name::character varying,
        column_name::character varying,
        data_type::character varying ,
        ordinal_position::character varying,
        column_default::character varying,
        is_nullable::character varying, 
        character_maximum_length::character varying,
        numeric_precision::character varying,
        numeric_scale::character varying,
        diffcount::character varying
from (
    select 
		t_orig.table_name,
		t_new.table_name as ntable_name,
        coalesce(t_orig.column_name, t_new.column_name) as column_name,
        case when t_orig.data_type = t_new.data_type then null else coalesce(t_orig.data_type, '') || ',' || coalesce(t_new.data_type, '') end as data_type,
        case when t_orig.ordinal_position = t_new.ordinal_position then null else coalesce(t_orig.ordinal_position::text, '') || ',' || coalesce(t_new.ordinal_position::text, '') end as ordinal_position ,
        case when t_orig.column_default = t_new.column_default then null else coalesce(t_orig.column_default, '') || ',' || coalesce(t_new.column_default, '') end as column_default ,
        case when t_orig.is_nullable = t_new.is_nullable then null else coalesce(t_orig.is_nullable, '') || ',' || coalesce(t_new.is_nullable, '') end as is_nullable ,
        case when t_orig.character_maximum_length = t_new.character_maximum_length then null else coalesce(t_orig.character_maximum_length::text, '') || ',' || coalesce(t_new.character_maximum_length::text, '') end as character_maximum_length ,
        case when t_orig.numeric_precision = t_new.numeric_precision then null else coalesce(t_orig.numeric_precision::text, '') || ',' || coalesce(t_new.numeric_precision::text, '') end as numeric_precision ,
        case when t_orig.numeric_scale = t_new.numeric_scale then null else coalesce(t_orig.numeric_scale::text, '') || ',' || coalesce(t_new.numeric_scale::text, '') end as numeric_scale ,
        decode(t_orig.column_name, t_new.column_name, 0, 1) +
            decode(t_orig.data_type, t_new.data_type, 0, 1) +
            --decode(t_orig.ordinal_position, t_new.ordinal_position, 0, 1) +        
            decode(t_orig.column_default, t_new.column_default, 0, 1) + 
            decode(t_orig.is_nullable, t_new.is_nullable, 0, 1) + 
            decode(t_orig.character_maximum_length, t_new.character_maximum_length, 0, 1) + 
            decode(t_orig.numeric_precision, t_new.numeric_precision, 0, 1) + 
            decode(t_orig.numeric_scale, t_new.numeric_scale, 0, 1) as diffcount
    from
        t_orig 
        full outer join t_new on (t_orig.column_name = t_new.column_name) ) diff
where
       diff.diffcount > 0
       
            ;

    r_return record;    
begin
 
 
 open c;
 loop
  fetch c into r_return;
  exit when not found;
  --raise notice 'I: %', v_i;
  return next r_return;
 end loop;
 close c;
 return;
end;
$$ LANGUAGE plpgsql;






select *
from
    py_load_tables_test.get_struct_diff_for_table('p_threadinfo', 'palette', 'py_load_tables_test') a
        (
		table_name character varying, 
		ntable_name character varying, 
        column_name character varying, 
        data_type character varying ,
        ordinal_position character varying,
        column_default character varying,
        is_nullable character varying, 
        character_maximum_length character varying,
        numeric_precision character varying,
        numeric_scale character varying,
        diffcount character varying
        )
;

select 'select * from py_load_tables_test.get_struct_diff_for_table(''' ||  tablename || ''', ''palette'', ''py_load_tables_test'') a
        (
        table_name character varying, 
        ntable_name character varying, 
        column_name character varying, 
        data_type character varying ,
        ordinal_position character varying,
        column_default character varying,
        is_nullable character varying, 
        character_maximum_length character varying,
        numeric_precision character varying,
        numeric_scale character varying,
        diffcount character varying
        ) union all'
from
    pg_tables
where
  schemaname = 'py_load_tables_test' and
  tablename not like '%prt%' and
  tablename like 'ext_n%' 
  ;
  
  
  ----------------------------------
  ----------------------------------

select py_load_tables_test.get_data_diff_for_table('palette', 'py_load_tables_test');

CREATE OR REPLACE FUNCTION py_load_tables_test.get_data_diff_for_table(p_schema_a text, p_schema_b text) RETURNS
--setof record AS $$
text AS $$
declare 

    v_sql text;
	v_sql_1 text;
    v_sql_2 text;
    v_sql_cnt text;
    rec_table record;
	rec record;
	v_col_list text;
    r_return record;
   -- c cursor is
   -- ;
begin           		
    
    v_sql := '';
    v_sql_cnt := '';
    
    for rec_table in (select tablename 
                      from 
                            pg_tables 
                       where 
                           schemaname = p_schema_a and
                           tablename not like 'ext#_%' escape '#' and
                           tablename not like '%#_prt#_%' escape '#' and
                           tablename not like 's#_%' escape '#' and
                           tablename not like 'p#_%' escape '#' and
                           tablename not in ('ptalend_flows', 'ptalend_logs', 'ptalend_stats', 'db_version_meta')
                       )
    loop
    
        v_col_list := '';
    	for rec in (select
                        c.column_name || ',' as col_name
                    from
                        information_schema.columns c
                    where
                        c.table_name = rec_table.tablename and
                        c.table_schema = 'palette' and
                        c.column_name not in ('p_id', 'p_filepath', 'p_cre_date')
                    order by column_name
    				)
    	loop
        
    		v_col_list := v_col_list || ' ' || rec.col_name;
    		
    	end loop;
        
        v_col_list := rtrim(v_col_list, ',');
     
        v_sql_1 := '(select ' || v_col_list || ' from ' ||  p_schema_a || '.' ||  rec_table.tablename ||
                    ' except ' ||
                    ' select ' || v_col_list || ' from ' ||  p_schema_b || '.' ||  rec_table.tablename || ')';

        v_sql_2 := '(select ' || v_col_list || ' from ' ||  p_schema_b || '.' ||  rec_table.tablename ||
                    ' except ' ||
                    ' select ' || v_col_list || ' from ' ||  p_schema_a || '.' ||  rec_table.tablename || ')';
         
                             
         v_sql_cnt := v_sql_cnt || '\n select ''' || rec_table.tablename  || ''', (select count(1) from ' || p_schema_a || '.' ||  rec_table.tablename || ')  as cnt_schema_a , (select count(1) from ' || p_schema_b || '.' ||  rec_table.tablename || ') as cnt_schema_b union all \n';
         v_sql := v_sql || '\n select ''' || rec_table.tablename  || ''', count(1) data_diff_cnt from (' || v_sql_1 || '\n union all \n' || v_sql_2 || ') a union all \n';
         
    end loop;
 
    return rtrim(v_sql_cnt, ' union all \n') || ' \n\n\n ' || rtrim(v_sql, ' union all \n');
    
end;
$$ LANGUAGE plpgsql;




