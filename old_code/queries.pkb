CREATE OR REPLACE PACKAGE BODY queries AS

	PROCEDURE get_json_config (
		tq VARCHAR2 DEFAULT NULL,
        tqx VARCHAR2 DEFAULT NULL,
		tqcustom VARCHAR2 DEFAULT NULL
	) IS
		
		page_c_contents clob;
		output varchar2(100) := 'html';
		v_first boolean;
		
	BEGIN
		
		-- return the json object with the proper response Handler given
		if output = 'html' then
	    	owa_util.mime_header('text/x-json', FALSE, NULL);
			htp.p('Pragma: no-cache');
			htp.p('Expires: Thu, 01 Jan 1970 12:00:00 GMT');
			owa_util.http_header_close;
		end if;
		
        helper.output := output;
		
		helper.p(nvl(helper.get_tqx_attr('responseHandler',tqx),'google.visualization.Query.setResponse')||'(');
		helper.p('{');
		helper.p(' version: "0.6",');
		helper.p(' status: "ok",');
		helper.p(' reqId: '||nvl(helper.get_tqx_attr('reqId',tqx),0)||',');
		-- signature ??
		-- helper.p(' signature: "928347923874923874",');
		
		if tqcustom is not null then
			helper.p(' requestID: '||nvl(helper.get_tqx_attr('requestID',tqcustom),0)||',');
		end if;
		
		helper.p(' table: {');
		
		-- define cols
		helper.p('  cols: [');
        	helper.prn('    {id: "conf", ');
	    	helper.prn('type: "string"}');
	    	helper.nl;
	    helper.p('  ],');
		
		-- rows!
		helper.p('  rows: [');
        
        select contents
		into page_c_contents
		from static_cfiles
		where file_name = 'js/datasources.json';
		
		declare
			v_count binary_integer;
			v_read binary_integer;
			v_text_buffer varchar2(2000);
		begin
			v_count := 1;
			v_read := 1000;
			v_first := true;
			loop
				
				DBMS_LOB.READ (
        			page_c_contents, v_read , v_count, v_text_buffer
				);
				
				if v_first then
					helper.p('   {c: [ ');
					v_first := false;
				else
					helper.p('   ,{c: [ ');
				end if;
				
				helper.prn('    {');
                	helper.prn('v: "'||utl_url.escape(v_text_buffer)||'"');
	            helper.prn('}');
	            helper.nl;
				
        		helper.p('   ]}');
								
				v_count := v_count  + v_read;
			end loop;
		exception
			WHEN NO_DATA_FOUND THEN
				null;
		end;
		
		helper.p('  ]');
	    helper.p(' }');
	    helper.p('}');
	    
	    -- finish!
	    helper.p(')');
    
	EXCEPTION
		WHEN OTHERS THEN
			DECLARE
				v_errors json_error_obj_table;
			BEGIN
				v_errors(1) := json_error_obj('internal_error',sqlerrm,null);
				send_json_error(v_errors,tqx,tqcustom);				
			END;			
	END get_json_config;
	
	PROCEDURE get_json (
            p_datasource_id VARCHAR2,
            tq VARCHAR2 DEFAULT NULL,
            tqx VARCHAR2 DEFAULT NULL,
			tqcustom VARCHAR2 DEFAULT NULL) 
    IS
        
		-- to store query details
        v_query VARCHAR2(32767) := '';
		v_query_description varchar2(32767) := '';
        v_cursor NUMBER;
        v_cursor_output NUMBER;
        v_col_cnt PLS_INTEGER;
        
        -- to store output values of the query
        v_col_char VARCHAR2(32767);
        v_col_number NUMBER;
		v_col_date DATE;
        
        -- description table
        record_desc_table dbms_sql.desc_tab;
		
		-- formats
		type type_formats is table of varchar2(100) index by binary_integer;
		v_format type_formats;
		v_format_tmp varchar2(100);
		
		-- dbms_profiler
		l_result pls_integer;
		
		-- logic helper vars
		v_first boolean;
		
	BEGIN
	
		-- output format
		helper.output := 'html';
		
		
		--l_result := DBMS_PROFILER.start_profiler(run_comment => 'Getting JSON: ' || to_char(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
		--execute immediate 'alter session set tracefile_identifier = ''JOSE'''; 
 		--dbms_support.start_trace(true,true);
		
		-- find the datasource details to execute
		find_datasource(
	        p_datasource_id,
	        v_query_description,
			v_query
		);
		
		if tq is null or length(trim(tq)) = 0 then
			raise_application_error(-20011,'No datasource query language detected');
		end if;
		
		-- open cursor
        v_cursor := dbms_sql.open_cursor;
		
		-- create google datasource object
		gdatasource.init;
		
		-- add the datasource info
		gdatasource.parse(v_query);
		
		-- parse the datasource query filter the user sent
		gdatasource.filter(tq);
		
        -- validate and prepare the query
		gdatasource.prepareCursor(v_cursor);
				
		-- execute the cursor
        v_cursor_output := dbms_sql.execute(v_cursor); 
		
		-- get columns of the cursor
        dbms_sql.describe_columns(v_cursor, v_col_cnt, record_desc_table);
		
		-- return the json object with the proper response Handler given
		if helper.output = 'html' then
	    	owa_util.mime_header('text/x-json', FALSE, NULL);
			htp.p('Pragma: no-cache');
			htp.p('Expires: Thu, 01 Jan 1970 12:00:00 GMT');
			owa_util.http_header_close;
		end if;
		
        helper.p(nvl(helper.get_tqx_attr('responseHandler',tqx),'google.visualization.Query.setResponse')||'(');
		helper.p('{');
		helper.p(' version: "0.6",');
		helper.p(' status: "ok",');
		helper.p(' reqId: '||nvl(helper.get_tqx_attr('reqId',tqx),0)||',');
		-- signature ??
		-- helper.p(' signature: "928347923874923874",');
		
		if tqcustom is not null then
			helper.p(' requestID: '||nvl(helper.get_tqx_attr('requestID',tqcustom),0)||',');
		end if;
		
		helper.p(' table: {');
		
		-- define cols
		helper.p('  cols: [');
	    
	    FOR col IN 1..v_col_cnt
	    LOOP
	        
			if record_desc_table(col).col_name <> 'FOURTEAM_ROWNUM' then
			
	            -- create column details
	            helper.prn('   {');
	            
	            helper.prn('id: "'||record_desc_table(col).col_name||'", ');
	            helper.prn('label: "'||record_desc_table(col).col_name||'", ');
	            
	            IF record_desc_table(col).col_type IN (1,96,112)
	            THEN
	                -- varchar, char and CLOB
	                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_char, 32767);
	                helper.prn('type: "string"');
	            ELSIF record_desc_table(col).col_type IN (2)
	            THEN
	                -- number
	                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_number);
	                helper.prn('type: "number"');
				ELSIF record_desc_table(col).col_type IN (12)
	            THEN
	                -- number
	                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_date);
	                helper.prn('type: "datetime"');
	            ELSE 
	                raise_application_error(-20001,'Not expected datatype');
	            END IF;
	            
	            if col < v_col_cnt then
	            	helper.prn('},');
	            else
	            	helper.prn('}');
	            end if;
	            
	            helper.nl;
					  
			end if;
	        
	    END LOOP;
		
		helper.p('  ],');
		
		-- rows!
		helper.p('  rows: [');
		
		v_first := true;
		
		LOOP
			
			-- Fetch a row from the source table
	        EXIT WHEN dbms_sql.fetch_rows(v_cursor) = 0;
	        
	        -- create row details
			
			-- Add the col and rows objects to the table json
			if v_first then
				helper.p('   {c: [ ');
				v_first := false;
			else
				helper.p('   ,{c: [ ');
			end if;
	        
	        FOR col IN 1..v_col_cnt
	        LOOP
	            
				if record_desc_table(col).col_name <> 'FOURTEAM_ROWNUM' then
					
	            	helper.prn('    {');
	                    
	    	        -- varchar, char and clob
	            	IF record_desc_table(col).col_type IN (1,96,112)
	            	THEN
	                    dbms_sql.column_value(v_cursor, col, v_col_char);
	                    helper.prn('v: "'||v_col_char||'"');
	            	ELSIF record_desc_table(col).col_type IN (2)
	            	THEN
	                    dbms_sql.column_value(v_cursor, col, v_col_number);
	                    helper.prn('v: '||to_char(v_col_number));
					ELSIF record_desc_table(col).col_type IN (12)
	            	THEN
	                    dbms_sql.column_value(v_cursor, col, v_col_date);
	                    helper.prn('v: new Date('||                    
	    								nvl(trim(leading '0' from to_char(v_col_date,'yyyy')),'0')	||','||
	    								nvl(trim(leading '0' from to_char(v_col_date,'mm')),'0')		||','||
	    								nvl(trim(leading '0' from to_char(v_col_date,'dd')),'0')		||','||
	    								nvl(trim(leading '0' from to_char(v_col_date,'hh24')),'0')	||','||
	    								nvl(trim(leading '0' from to_char(v_col_date,'mi')),'0')		||','||
	    								nvl(trim(leading '0' from to_char(v_col_date,'ss')),'0')		||')');
	                    helper.prn(', f: "'||to_char(v_col_date,'yyyy-mm-dd hh24:mi:ss')||'"');
	            	ELSE
	                    raise_application_error(-20001,'Not expected datatype');
	            	END IF;
	                    
	            	if col < v_col_cnt then
		            	helper.prn('},');
		            else
		            	helper.prn('}');
		            end if;
		            
					helper.nl;
					
				end if;
				                
	        END LOOP;
	        
	        helper.p('   ]}');
	    
		END LOOP;
	    
	    helper.p('  ]');
	    helper.p(' }');
	    helper.p('}');
	    
	    -- finish!
	    helper.p(')');
	     
	    dbms_sql.close_cursor(v_cursor);
        
       	--l_result := DBMS_PROFILER.stop_profiler;
    	--dbms_support.stop_trace;

	EXCEPTION
		WHEN OTHERS THEN
			DECLARE
				v_errors json_error_obj_table;
			BEGIN
				v_errors(1) := json_error_obj('internal_error',sqlerrm,null);
				send_json_error(
					v_errors,
					tqx,
					tqcustom
				);				
			END;
			--l_result := DBMS_PROFILER.stop_profiler;
			--dbms_support.start_trace;
    END get_json; 
        
	PROCEDURE find_datasource(
        p_datasource_id IN VARCHAR2, -- query identifier
        p_query_description OUT NOCOPY varchar2,
        p_query OUT NOCOPY varchar2
	) IS
	BEGIN
   		-- initialize
		p_query_description := null;
		p_query := '';
		
		-- get the query details
		case p_datasource_id
			when 'dashboard_sessions' then
				p_query_description := 'Database sessions overview';
				p_query := 'select 
	 a.sid as SID,
	 a.serial# as Serial#,
	 a.username as UserName,
	 a.status as status,
	 a.osuser||''@''||a.machine as client_info,
     decode(a.username,null,e.name,a.username) as UserName_incl_bgprocess,
     d.spid as OS_Process_ID,
     a.machine as Machine,
     logon_time as Logon_Time,
    (sum(decode(c.name,''physical reads'',value,0)) +
     sum(decode(c.name,''physical writes'',value,0)) +
     sum(decode(c.name,''physical writes direct'',value,0)) +
     sum(decode(c.name,''physical writes direct (lob)'',value,0))+
     sum(decode(c.name,''physical reads direct (lob)'',value,0)) +
     sum(decode(c.name,''physical reads direct'',value,0)))
     as total_physical_io,
    (sum(decode(c.name,''db block gets'',value,0)) +
     sum(decode(c.name,''db block changes'',value,0)) +
     sum(decode(c.name,''consistent changes'',value,0)) +
     sum(decode(c.name,''consistent gets'',value,0)) )
     as total_logical_io,
    (sum(decode(c.name,''session pga memory'',value,0))+
     sum(decode(c.name,''session uga memory'',value,0)) )
     as total_memory_usage,
     sum(decode(c.name,''parse count (total)'',value,0)) as parses,
     sum(decode(c.name,''CPU used by this session'',value,0))
     as total_cpu,
     sum(decode(c.name,''parse time cpu'',value,0)) as parse_cpu,
     sum(decode(c.name,''recursive cpu usage'',value,0))
     as recursive_cpu,
     sum(decode(c.name,''CPU used by this session'',value,0)) -
     sum(decode(c.name,''parse time cpu'',value,0)) -
     sum(decode(c.name,''recursive cpu usage'',value,0))
     as other_cpu,
     sum(decode(c.name,''sorts (disk)'',value,0)) as disk_sorts,
     sum(decode(c.name,''sorts (memory)'',value,0)) as memory_sorts,
     sum(decode(c.name,''sorts (rows)'',value,0)) as rows_sorted,
     sum(decode(c.name,''user commits'',value,0)) as commits,
     sum(decode(c.name,''user rollbacks'',value,0)) as rollbacks,
     sum(decode(c.name,''execute count'',value,0)) as executions
from sys.v_$session a
left join 		sys.v_$sesstat b 	on b.sid = a.sid
left join 		sys.v_$statname c 	on c.statistic# = b.statistic#
left outer join sys.v_$process d 	on d.addr = a.paddr
left outer join sys.v_$bgprocess e 	on e.paddr = a.paddr 
where c.NAME in (''physical reads'',''physical writes'',''physical writes direct'',''physical reads direct'',
                 ''physical writes direct (lob)'',''physical reads direct (lob)'',''db block gets'',
                 ''db block changes'',''consistent changes'',''consistent gets'',''session pga memory'',
                 ''session uga memory'',''parse count (total)'',''CPU used by this session'',
                 ''parse time cpu'',''recursive cpu usage'',''sorts (disk)'',''sorts (memory)'',
                 ''sorts (rows)'',''user commits'',''user rollbacks'',''execute count'')
group by a.sid,
	 a.serial#,
	 a.username,
	 a.status,
	 a.osuser||''@''||a.machine,
     decode(a.username,null,e.name,a.username),
     d.spid,
     a.machine,
     logon_time';
	 		when 'dashboard_version' then
				p_query_description := 'Database version';
				p_query := 'select banner from sys.v_$version';
			when 'dashboard_timemodel' then
				p_query_description := 'Database Time Model';
				p_query := 'select  
		WAIT_CLASS, 
		WAIT_COUNT,
        round(100 * (WAIT_COUNT / SUM_WAITS),2) as PCT_WAITS,
        ROUND((TIME_WAITED / 100),2) as TIME_WAITED_SECS,
        round(100 * (TIME_WAITED / SUM_TIME),2) as PCT_TIME
from (select wc.WAIT_CLASS as wait_class,
        sum(wh.WAIT_COUNT) as wait_count,
        sum(wh.TIME_WAITED) as time_waited
      from    sys.v_$waitclassmetric_history wh,  sys.v_$system_wait_class wc
      where   WAIT_CLASS != ''Idle'' 
        and wc.WAIT_CLASS# = wh.WAIT_CLASS#
      group by wc.wait_class),
     (select  sum(wh.WAIT_COUNT) as SUM_WAITS,
        sum(wh.TIME_WAITED) as SUM_TIME
      from    sys.v_$waitclassmetric_history wh,  sys.v_$system_wait_class wc
      where   WAIT_CLASS != ''Idle'' 
        and wc.WAIT_CLASS# = wh.WAIT_CLASS#)';
			when 'time_model_history' then
				p_query_description := 'Wait events by class over time.';
				p_query := 'select  end_time,
		sum(decode(b.wait_class,''Application'',round((a.time_waited / 100),2),0)) as APPLICATION,
		sum(decode(b.wait_class,''Network'',round((a.time_waited / 100),2),0)) as NETWORK,
		sum(decode(b.wait_class,''User I/O'',round((a.time_waited / 100),2),0)) as USER_IO,
		sum(decode(b.wait_class,''Configuration'',round((a.time_waited / 100),2),0)) as CONFIGURATION,
		sum(decode(b.wait_class,''Concurrency'',round((a.time_waited / 100),2),0)) as CONCURRENCY,
		sum(decode(b.wait_class,''Other'',round((a.time_waited / 100),2),0)) as OTHER,
		sum(decode(b.wait_class,''Commit'',round((a.time_waited / 100),2),0)) as COMMIT,
		sum(decode(b.wait_class,''System I/O'',round((a.time_waited / 100),2),0)) as SYSTEM_IO
from    sys.v_$waitclassmetric_history a,
        sys.v_$system_wait_class b
where   a.wait_class# = b.wait_class# and
        b.wait_class != ''Idle''
group by end_time
order by end_time';
			else
            	raise_application_error(-20002,'Query not found');				
		end case;
	END find_datasource;
	
	/**
	 * Send errors as json object
	 *
	 */
	procedure send_json_error(
		p_errors in json_error_obj_table,
		tqx VARCHAR2 DEFAULT NULL,
		tqcustom VARCHAR2 DEFAULT NULL
	) is
	begin
		
		-- return the json object with the proper response Handler given
		owa_util.mime_header('text/x-json', FALSE, NULL);
		htp.p('Pragma: no-cache');
		htp.p('Expires: Thu, 01 Jan 1970 12:00:00 GMT');
		owa_util.http_header_close;
		
		
		helper.p(nvl(helper.get_tqx_attr('responseHandler',tqx),'google.visualization.Query.setResponse')||'(');
				
		helper.p('{');
		helper.p(' version: "0.6",');
		helper.p(' status: "error",');
		helper.p(' reqId: '||nvl(helper.get_tqx_attr('reqId',tqx),0)||',');
		-- signature ??
		-- helper.p(' signature: "928347923874923874",');
		
		if tqcustom is not null then
			helper.p(' requestID: '||nvl(helper.get_tqx_attr('requestID',tqcustom),0)||',');
		end if;
		
		if p_errors.COUNT < 1 then
			raise_application_error(-20001,'Invalid JSON error object');
		end if;
		
		helper.p(' errors: [');
		
		for e in 1..p_errors.COUNT loop
			if e > 1 then
			    helper.p(',');
				helper.prn('   {');
			else
				helper.prn('   {');
			end if;
			helper.prn('reason: "'||p_errors(e).reason||'"');
			if p_errors(e).message is not null then
				helper.prn(', message: "'||p_errors(e).message||'"');
			end if;
			if p_errors(e).detailed_message is not null then
				helper.prn(', detailed_message: "'||p_errors(e).detailed_message||'"');
			end if;
			helper.p('}');		
		end loop;
		
		helper.p(' ]');
	    helper.p('}');
	    
	    -- finish!
	    helper.p(')');		
		
	end send_json_error;

END queries;
/
