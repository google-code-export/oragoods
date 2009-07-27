CREATE OR REPLACE PACKAGE BODY GDataSource
	/**
	 * 
	 * PL/SQL Implementation for Google Data Source objects 
	 * 
	 * Some support to the Query Language included 
	 * http://code.google.com/apis/visualization/documentation/querylanguage.html#Clauses
	 * 
	 * 2009-05-21: v0.1 by jose.canciani@4tm.com.ar
	 * 		Finish initial version.
	 * 	
	 */
AS
	
	PROCEDURE INIT
    IS
	BEGIN
		-- initialize object attributes
		g_google_query				:= '';
		g_datasource_query			:= '';
		g_needed_datasource_cols 	:= gdatasource_rows_obj_table();
		g_query_cols 				:= gdatasource_rows_obj_table();
		g_select_cols				:= gdatasource_select_obj_table();
		g_where_cols				:= gdatasource_where_obj_table();
		g_groupby_cols				:= gdatasource_groupby_obj_table();
		g_orderby_cols				:= gdatasource_orderby_obj_table();
		g_labels					:= gdatasource_rows_obj_table();
		g_limit						:= null;
		g_offset					:= null;
		
		g_init := true;
		
	END;
	
	procedure parse (
		p_datasource_query 			IN 			varchar2
	)
	is
		
		v_begin			pls_integer;
		v_end			pls_integer;
		v_select		varchar2(32676);
		
		v_col			varchar2(32676);
		v_alias			varchar2(32676);
		v_function_col	varchar2(32676);
		v_function_col_type	varchar2(32676);
		v_function		varchar2(32676);
		
		-- explode table
		v_table helper.t_varchar2;
		
	begin
	
		if not g_init then
			init;
		end if;
		
		g_datasource_query := p_datasource_query;
		
		v_begin	:= instr(lower(p_datasource_query),'select') + 6; -- 7 = length('select ')
		v_end := instr(lower(p_datasource_query),'from') - 1;
		
		if v_begin = 0 or v_end = 0 then
			raise_application_error(-20029,'Invalid DataSource query');
		end if;
		
		v_select := trim(replace(replace(substr(p_datasource_query,v_begin,v_end-v_begin),chr(10),' '),chr(9),' '));
		
		-- now get the columns
		helper.EXPLODE_SELECT_CLAUSE(v_select,v_table,true);
		
		for c in 1..v_table.count 
		loop
			
			g_query_cols.extend;
			
			-- find alias first
			findAlias(v_table(c),v_col,v_alias);
			
			if v_alias is not null then
				g_query_cols(c) := gdatasource_rows_obj(
												v_col, v_alias
											);
			elsif helper.isValidColumnName(v_col) then
				g_query_cols(c) := gdatasource_rows_obj(
												v_col, v_col
											);
			else
				raise_application_error(-20030,'Invalid column found in DataSource query: '||v_col);
			end if;
		end loop;
		
	end parse;
	
	/**
	 * filter: Filter the DataSource with a Google Query Language
	 * 
	 * @param p_query_string varchar2 Query string as presented by the client
	 * 
	 * Example:
	 *   select row1 [as "label 1"], .. ,  rown as "label n"
	 *   [where expr1 op [cond1] [and .. and exprn op [condn]]]
	 *   [group by row1, .. , rown]
	 *   [order by row1 [asc|desc] , row2 [asc|desc], rown [asc|desc]] 
	 *   [limit n]
	 *   [offset n]
	 *	 [label row1 'label1' [, rown 'label n'] ]
	 *	 [format row1 'format1' [, rown 'format n'] ]
	 * 
	 * Supported operatoros (op): < <= > >= = != <> "is null" "is not null" "and"
	 *     
	 * TODO: 1) ignore , and reserve words between quotes and double quotes when exploding
	 * TODO: 2) add "date" keyword for comparing literal dates with date columns
	 * TODO: 3) enable logic ("and" mixed with "or") in the where clause
	 * 
	 *   
	 */
	procedure filter (
		p_query_string	IN	varchar2
	) is
		v_tmp_query		varchar2(32767) := '';
		-- query strings and positions
		v_select 		varchar2(32767) := '';
		v_select_pos	pls_integer;
		v_where			varchar2(32767) := '';
		v_where_pos		pls_integer;
		v_group			varchar2(32767) := '';
		v_group_pos		pls_integer;
		v_order			varchar2(32767) := '';
		v_order_pos		pls_integer;
		v_order_tmp		varchar2(32767) := ''; -- temporary for looking asc/desc keywords
		v_order_tmp_order	varchar2(4) := null; -- temporary for looking asc/desc keywords
		v_limit			varchar2(100) := '';
		v_limit_pos		pls_integer;
		v_offset		varchar2(100) := '';
		v_offset_pos	pls_integer;
		v_label			varchar2(32767) := '';
		v_label_pos		pls_integer;
		v_format		varchar2(32767) := '';
		v_format_pos	pls_integer;
		-- two vars for getting begin/end parameters
		v_begin			pls_integer;
		v_end			pls_integer;
		-- helper vars
		v_col			varchar2(100);
		v_col_type		varchar2(30);
		v_right_col		varchar2(100);
		v_right_col_type varchar2(30);
		v_alias			varchar2(100);
		v_function_col	varchar2(30);
		v_function_col_type varchar2(30);
		v_function		varchar2(30);
		v_operator		varchar2(11);
		v_operator_pos	pls_integer;
		-- collect needed columns
		type mycols is table of varchar2(2000) index by varchar2(100);
		v_needed_mycols mycols;
		v_current varchar2(100);
		-- table for explode
		v_table helper.t_varchar2;
		v_table2 helper.t_varchar2;
	begin
	
		if not g_init then
			init;
		end if;
		
		g_google_query := p_query_string;
		
		-- extract the parts of the query
		v_tmp_query := lower(p_query_string);
		
		-- find keywords and validate order
		v_select_pos 	:= instr(v_tmp_query,'select ');
		v_where_pos 	:= instr(v_tmp_query,'where ');
		v_group_pos 	:= instr(v_tmp_query,'group by ');
		v_order_pos 	:= instr(v_tmp_query,'order by ');
		v_limit_pos 	:= instr(v_tmp_query,'limit ');
		v_offset_pos 	:= instr(v_tmp_query,'offset ');
		v_label_pos 	:= instr(v_tmp_query,'label ');
		v_format_pos 	:= instr(v_tmp_query,'format ');
		
		if 		instr(v_tmp_query,'select ',1,2) > 0
			or  instr(v_tmp_query,'where ',1,2) > 0
			or 	instr(v_tmp_query,'group by ',1,2) > 0
			or  instr(v_tmp_query,'order by ',1,2) > 0
			or  instr(v_tmp_query,'limit ',1,2) > 0
			or  instr(v_tmp_query,'offset ',1,2) > 0
			or  instr(v_tmp_query,'label ',1,2) > 0
			or  instr(v_tmp_query,'format ',1,2) > 0
		then
			raise_application_error(-20001,'Invalid query, only one keyword is allowed.');
		end if;
		
		if v_where_pos = 0 then
			v_where_pos := v_select_pos;
		end if;
		if v_group_pos = 0 then
			v_group_pos := v_where_pos;
		end if;
		if v_order_pos = 0 then
			v_order_pos := v_group_pos;
		end if;
		if v_limit_pos = 0 then
			v_limit_pos := v_order_pos;
		end if;
		if v_offset_pos = 0 then
			v_offset_pos := v_limit_pos;
		end if;
		if v_label_pos = 0 then
			v_label_pos := v_offset_pos;
		end if;
		if v_format_pos = 0 then
			v_format_pos := length(p_query_string)+1;
		end if;
		
		if 		v_select_pos > v_where_pos
			or 	v_where_pos > v_order_pos
			or 	v_order_pos > v_limit_pos
			or 	v_limit_pos > v_offset_pos
			or 	v_offset_pos > v_label_pos
			or 	v_label_pos > v_format_pos
		then
			raise_application_error(-20002,'Invalid query, the order of the keywords is invalid.');
		end if;
		
		-- find the rows and validate it's content
		-- first, get the correct position again
		v_select_pos 	:= instr(v_tmp_query,'select ');
		v_where_pos 	:= instr(v_tmp_query,'where ');
		v_group_pos 	:= instr(v_tmp_query,'group by ');
		v_order_pos 	:= instr(v_tmp_query,'order by ');
		v_limit_pos 	:= instr(v_tmp_query,'limit ');
		v_offset_pos 	:= instr(v_tmp_query,'offset ');
		v_label_pos 	:= instr(v_tmp_query,'label ');

		-- select
		if v_select_pos > 0 then
			v_begin := v_select_pos + 7; -- 7 = length('select ')
			select decode(v_where_pos,0,
						decode(v_group_pos,0,
							decode(v_order_pos,0,
								decode(v_limit_pos,0,
									decode(v_offset_pos,0,
										decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										,v_label_pos-1)
									,v_offset_pos-1)
							    ,v_limit_pos-1)
							,v_order_pos-1)
						,v_group_pos-1)
					,v_where_pos-1)
			into v_end
			from dual;
			
			v_select := trim(substr(p_query_string,v_begin,v_end-v_begin));
			
			-- now get the columns
			helper.explode(v_select,',',v_table,true);
			for c in 1..v_table.count
			loop
				g_select_cols.extend;
				-- determine type of column
				-- find alias first
				findAlias(v_table(c),v_col,v_alias);
				if helper.isStringWithQuotes(v_col) then
					-- string
					g_select_cols(c) := gdatasource_select_obj(
													v_table(c), null, v_alias, 'string', null
												);
				elsif helper.isNumeric(v_table(c)) then
					-- number
					g_select_cols(c) :=	gdatasource_select_obj(
													v_table(c), null, v_alias, 'number', null
												);
				else
				
					-- function
					findFunction(v_col,v_function_col,v_function_col_type,v_function);
					-- if not a function, findFunction will check for a valid column name
					g_select_cols(c) :=	gdatasource_select_obj(
													v_function_col, 
													v_function, 
													v_alias, 
													v_function_col_type, 
													null
												);
					if v_function_col_type = 'column' then
						v_needed_mycols(upper(v_function_col)):=v_function_col;
					end if;
				end if;
			end loop;
		end if;
		
		-- where
		if v_where_pos > 0 then
			v_begin := v_where_pos + 6; -- 6 = length('where ')
			select --decode(v_where_pos,0,
						decode(v_group_pos,0,
							decode(v_order_pos,0,
								decode(v_limit_pos,0,
									decode(v_offset_pos,0,
										decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										,v_label_pos-1)
									,v_offset_pos-1)
							    ,v_limit_pos-1)
							,v_order_pos-1)
						,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			v_where := trim(substr(p_query_string,v_begin,v_end-v_begin));
			-- now get the conditions
			helper.explode(replace(v_where,' and ',chr(13)),chr(13),v_table,true);
			helper.explode('<,<=,>,>=,=,!=,<>,is null,is not null',',',v_table2);			
			for c in 1..v_table.count
			loop
				-- search comparison operator
				v_operator_pos := 0;
				v_operator := null;
				for o in 1..v_table2.count
				loop
					v_operator_pos := instr(lower(v_table(c)),v_table2(o));
					if v_operator_pos > 0 then
						v_operator := v_table2(o);
						exit;					
					end if;
				end loop;
				if v_operator_pos = 0 then
					raise_application_error(-20003,'Operator not found in where clause');
				end if;
				v_col := null;
				v_col_type := null;
				v_right_col := null;
				v_right_col_type := null;
				-- separate left and right side of the condition
				helper.explode(replace(v_table(c),v_operator,chr(13)),chr(13),v_table2,true);
				for a in 1..v_table2.count
				loop
					if a = 1 then
						v_col := v_table2(a);		
					elsif a = 2 then
						v_right_col := v_table2(a);		
					else
						raise_application_error(-20004,'Invalid syntax in where clause');
					end if;
				end loop;
				-- check left side of the condition
				if helper.isStringWithQuotes(v_col) then
					v_col_type := 'string';
				elsif helper.isNumeric(v_col) then
					v_col_type := 'number';
				elsif helper.isValidColumnName(v_col) then
					v_col_type := 'column';
					v_needed_mycols(upper(v_col)):=v_col;
				else
					raise_application_error(-20005,'Invalid left column in where clause');					
				end if;
				-- now check right side
				if v_operator not in ('is null','is not null') then
					if helper.isStringWithQuotes(v_right_col) then
						v_right_col_type := 'string';
					elsif helper.isNumeric(v_right_col) then
						v_right_col_type := 'number';
					elsif helper.isValidColumnName(v_right_col) then
						v_right_col_type := 'column';
						v_needed_mycols(upper(v_right_col)):=v_right_col;
					else
						raise_application_error(-20006,'Invalid right column in where clause');					
					end if;
				end if;
				-- finally store the where condition
				g_where_cols.extend;
				g_where_cols(c) := 	gdatasource_where_obj(
												v_col,v_col_type,v_right_col,v_right_col_type,v_operator	
												);
			end loop;
		end if;
		
		-- group by
		if v_group_pos > 0 then
			v_begin := v_group_pos + 9; -- 9 = length('group by ')
			select --decode(v_where_pos,0,
						--decode(v_group_pos,0,
							decode(v_order_pos,0,
								decode(v_limit_pos,0,
									decode(v_offset_pos,0,
										decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										,v_label_pos-1)
									,v_offset_pos-1)
							    ,v_limit_pos-1)
							,v_order_pos-1)
						--,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			v_group := trim(substr(p_query_string,v_begin,v_end-v_begin));
			-- now get the columns
			helper.explode(v_group,',',v_table,true);
			for c in 1..v_table.count
			loop
				g_groupby_cols.extend;
				-- determine type of column
				if helper.isStringWithQuotes(v_table(c)) then
					-- string
					g_groupby_cols(c) := gdatasource_groupby_obj(
											v_table(c),null,'string'
										);
				elsif helper.isNumeric(v_table(c)) then
					-- number
					g_groupby_cols(c) := gdatasource_groupby_obj (
											v_table(c), null, 'number'
										);
				else
					-- column! also look for function?
					v_function := null;
					v_function_col := null;
					findFunction(v_table(c),v_function_col,v_function_col_type,v_function);
					-- if not a function, findFunction will check for a valid column name
					g_groupby_cols(c) := gdatasource_groupby_obj(
												v_function_col, v_function, v_function_col_type
											);
					if v_function_col_type = 'column' then
						v_needed_mycols(upper(v_function_col)):=v_function_col;
					end if;
				end if;
			end loop;
		end if;
		
		-- order by
		if v_order_pos > 0 then
			v_begin := v_order_pos + 9; -- 9 = length('order by ')
			select --decode(v_where_pos,0,
						--decode(v_group_pos,0,
							--decode(v_order_pos,0,
								decode(v_limit_pos,0,
									decode(v_offset_pos,0,
										decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										,v_label_pos-1)
									,v_offset_pos-1)
							    ,v_limit_pos-1)
							--,v_order_pos-1)
						--,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			v_order := trim(substr(p_query_string,v_begin,v_end-v_begin));
			-- now get the columns
			helper.explode(v_order,',',v_table,true);
			for c in 1..v_table.count
			loop
				-- look for asc or desc keywords
				v_order_tmp_order := null;
				if instr(lower(v_table(c)),' asc') > 0 then
					v_order_tmp := substr(v_table(c),1,instr(lower(v_table(c)),' asc')-1);
					v_order_tmp_order := 'asc';
				elsif instr(lower(v_table(c)),' desc') > 0 then
					v_order_tmp := substr(v_table(c),1,instr(lower(v_table(c)),' desc')-1);
					v_order_tmp_order := 'desc';
				else
					v_order_tmp := v_table(c);	
				end if;
				
				g_orderby_cols.extend;
				-- determine type of column
				if helper.isNumeric(v_order_tmp) then
					-- number
					g_orderby_cols(c) := gdatasource_orderby_obj (
											v_order_tmp, null, 'number', v_order_tmp_order
										);
				else
					-- column! also look for function?
					v_function := null;
					v_function_col := null;
					findFunction(v_order_tmp,v_function_col,v_function_col_type,v_function);
					-- if not a function, findFunction will check for a valid column name
					g_orderby_cols(c) := gdatasource_orderby_obj(
												v_function_col, v_function, v_function_col_type, v_order_tmp_order
											);
					if v_function_col_type = 'column' then
						v_needed_mycols(upper(v_function_col)):=v_function_col;
					end if;
				end if;
			end loop;
		end if;
		
		-- limit
		v_limit := null;
		if v_limit_pos > 0 then
			v_begin := v_limit_pos + 6; -- 6 = length('limit ')
			select --decode(v_where_pos,0,
						--decode(v_group_pos,0,
							--decode(v_order_pos,0,
								--decode(v_limit_pos,0,
									decode(v_offset_pos,0,
										decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										,v_label_pos-1)
									,v_offset_pos-1)
							    --,v_limit_pos-1)
							--,v_order_pos-1)
						--,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			
			v_limit := trim(substr(p_query_string,v_begin,v_end-v_begin));
			
			if not helper.isNumeric(v_limit) then
				raise_application_error(-20007,'Invalid limit value specified.');
			else
				g_limit := v_limit;
			end if;
		end if;
		
		-- offset
		v_offset := null;
		if v_offset_pos > 0 then
			v_begin := v_offset_pos + 7; -- 7 = length('offset ')
			select --decode(v_where_pos,0,
						--decode(v_group_pos,0,
							--decode(v_order_pos,0,
								--decode(v_limit_pos,0,
									--decode(v_offset_pos,0,
										decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										,v_label_pos-1)
									--,v_offset_pos-1)
							    --,v_limit_pos-1)
							--,v_order_pos-1)
						--,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			v_offset := trim(substr(p_query_string,v_begin,v_end-v_begin));
			if not helper.isNumeric(v_offset) then
				raise_application_error(-20008,'Invalid offset value specified.');
			else
				g_offset := v_offset;
			end if;
		end if;
		
		-- label
		-- p_labels := ??;
		if v_label_pos > 0 then
			v_begin := v_label_pos + 6; -- 6 = length('label ')
			select --decode(v_where_pos,0,
						--decode(v_group_pos,0,
							--decode(v_order_pos,0,
								--decode(v_limit_pos,0,
									--decode(v_offset_pos,0,
										--decode(v_label_pos,0,
											LENGTH(p_query_string)+1
										--,v_label_pos-1)
									--,v_offset_pos-1)
							    --,v_limit_pos-1)
							--,v_order_pos-1)
						--,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			v_label := trim(substr(p_query_string,v_begin,v_end-v_begin));
			raise_application_error(-20009,'Label is implemented by using the AS keyword in the SELECT clause.');
		end if;	
		
		-- group by
		if v_format_pos > 0 then
			v_begin := v_format_pos + 7; -- 7 = length('format ')
			select --decode(v_where_pos,0,
						--decode(v_group_pos,0,
							decode(v_order_pos,0,
								decode(v_limit_pos,0,
									decode(v_offset_pos,0,
										decode(v_label_pos,0,
											decode(v_format_pos,0,
												LENGTH(p_query_string)+1
											,v_format_pos-1)
										,v_label_pos-1)
									,v_offset_pos-1)
							    ,v_limit_pos-1)
							,v_order_pos-1)
						--,v_group_pos-1)
					--,v_where_pos-1)
			into v_end
			from dual;
			v_label := trim(substr(p_query_string,v_begin,v_end-v_begin));
			
			-- now get the columns
			helper.explode(v_label,',',v_table,true);
			for c in 1..v_table.count
			loop
				null;
				/*
				v_label_cols.extend;
				-- determine type of column
				v_label_cols(c) := gdatasource_label_obj(
										v_table(c),'format'
									);
				*/
			end loop;
		end if;		
		
		-- store my needed columns for the dataset
		v_current := v_needed_mycols.first;
		loop
			g_needed_datasource_cols.extend;
			g_needed_datasource_cols(g_needed_datasource_cols.count) :=	
											gdatasource_rows_obj(
												upper(v_current),
												v_needed_mycols(v_current)
											);		
			v_current := v_needed_mycols.next(v_current);
			exit when v_current is null;
		end loop;					
		
	end filter;
	
	-- find alias (col AS "alias")
	procedure findAlias(
		p_string 					IN 			varchar2, 
		p_col 						OUT NOCOPY 	varchar2, 
		p_alias 					OUT NOCOPY 	varchar2
	) is 
		v_string varchar2(32767);
		v_alias_pos pls_integer;
		v_table helper.t_varchar2;
	begin
		
		-- default value for alias
		p_alias := null;
		
		if instr(lower(p_string),' as ',1,2) > 0 then
			raise_application_error(-20010,'AS keyword found too many times.');
		end if;
		
		v_alias_pos := instr(lower(p_string),' as ');
		
		if v_alias_pos > 0 then
			
			-- replace " as " for an explode version
			v_string := substr(p_string,1,v_alias_pos-1)||chr(13)||substr(p_string,v_alias_pos+4);
				
			helper.EXPLODE(v_string,chr(13),v_table,true);
				 
			for i in 1..v_table.count
			loop
				if i = 1 then
					p_col := v_table(i);
				else
					-- validate alias
					if helper.isStringWithDoubleQuotes(v_table(i)) then
						p_alias := v_table(i);
					else
						-- verify that is a valid column name
						if helper.isValidColumnName(v_table(i)) then
							p_alias := v_table(i);
						else
							raise_application_error(-20011,'Invalid column name used as alias.');
						end if;
					end if;					
				end if;									
			end loop;
		else
			p_col := trim(p_string);
			p_alias := null;
		end if;
	end findAlias;
	
	-- find function ("func(col)")
	procedure findFunction(
		p_col 						in 			varchar2,
		p_function_col 				out nocopy 	varchar2,
		p_function_col_type 		out nocopy 	varchar2,
		p_function 					out nocopy 	varchar2,
		allow_schema_prefix			IN			boolean		default false
	) is
		v_begin pls_integer;
		v_end pls_integer;
	begin
		-- default function value
		p_function := null;
		
		v_begin := instr(p_col,'(');
		v_end := instr(p_col,')');
		
		if v_begin > 0 and v_end > 0 then
			
			-- found function!
			p_function_col := trim(substr(p_col,v_begin+1,v_end-v_begin-1));
			p_function := trim(substr(p_col,1,v_begin-1));
			
			if helper.isNumeric(p_function_col) then
				p_function_col_type := 'number';
			elsif helper.isStringWithQuotes(p_function_col) then
				p_function_col_type := 'string';
			elsif helper.isValidColumnName(p_function_col,allow_schema_prefix) then
				p_function_col_type := 'column';
			else
				raise_application_error(-20012,'Invalid column name or not supported data type.');
			end if;
			
			case lower(p_function)
				when 'sum' then p_function := 'sum';
				when 'max' then p_function := 'max';
				when 'min' then p_function := 'min';
				when 'avg' then p_function := 'avg';
				when 'count' then p_function := 'count';
			else
				raise_application_error(-20013,'Invalid column name or not supported data type.');
			end case;
						
		else
			if v_begin > 0 or v_end > 0 then
				raise_application_error(-20014,'Invalid syntax in function column.');
			else
				-- no function detected, validate column now
				p_function_col := p_col; 
				p_function := null;
				if helper.isNumeric(p_function_col) then
					p_function_col_type := 'number';
				elsif helper.isStringWithQuotes(p_function_col) then
					p_function_col_type := 'string';
				elsif helper.isValidColumnName(p_function_col,false,allow_schema_prefix) then
					p_function_col_type := 'column';
				else
					raise_application_error(-20015,'Invalid column name or not supported data type');
				end if;
			end if;
		end if;
		
	end findFunction;
	
	-- validate that query columns are indeed part of the datasource
	procedure validateQueryColumns
	is
		type mycols is table of varchar2(2000) index by varchar2(100);
		v_mycols mycols;
	begin
		-- fill the associative array
		for i in 1..g_query_cols.LAST loop
			v_mycols(upper(g_query_cols(i).column_alias)) := g_query_cols(i).column_name;
		end loop;
		
		-- loop in the data source query elements and validate that the column exists.
		for i in 1..g_select_cols.count loop
			if g_select_cols(i).column_type = 'column' then
				if not v_mycols.exists(upper(g_select_cols(i).column_name)) then
					raise_application_error(-20016,'Column does not exists in datasource: '||g_select_cols(i).column_name);										
				end if;
			end if;
		end loop;
		for i in 1..g_where_cols.count loop
			if g_where_cols(i).column_left_type = 'column' then
				if not v_mycols.exists(upper(g_where_cols(i).column_left)) then
					raise_application_error(-20017,'Column does not exists in datasource: '||g_where_cols(i).column_left);										
				end if;
			end if;
			if g_where_cols(i).column_right_type = 'column' then
				if not g_query_cols.exists(upper(g_where_cols(i).column_right)) then
					raise_application_error(-20018,'Column does not exists in datasource: '||g_where_cols(i).column_right);										
				end if;
			end if;
		end loop;
		for i in 1..g_groupby_cols.count loop
			if g_groupby_cols(i).column_type = 'column' then
				if not v_mycols.exists(upper(g_groupby_cols(i).column_name)) then
					raise_application_error(-20019,'Column does not exists in datasource: '||g_groupby_cols(i).column_name);										
				end if;
			end if;
		end loop;
		for i in 1..g_orderby_cols.count loop
			if g_orderby_cols(i).column_type = 'column' then
				if not v_mycols.exists(upper(g_orderby_cols(i).column_name)) then
					raise_application_error(-20020,'Column does not exists in datasource: '||g_orderby_cols(i).column_name);										
				end if;
			end if;
		end loop;
	end validateQueryColumns;
	
	-- build, parse and bind query based on datasource query and datasource cursor
	procedure prepareCursor(
		-- the cursor id
		p_cursor 					IN NUMBER
	)
	is
		v_query varchar2(32767);
		v_select varchar2(32767);
		v_col varchar2(32767);
		v_first boolean;
		v_bind pls_integer;
		type bind_table is table of gdatasource_bind_obj index by binary_integer;
		v_bind_table bind_table;
		-- current column
		v_current varchar2(32767);
		-- limit and offset vars
		v_from	pls_integer;
		v_to	pls_integer;
		
		type mycols is table of varchar2(2000) index by varchar2(200);
		v_mycols mycols;
		
	begin
	
		if not g_init then
			init;
		end if;
		
		-- first validate the column names
		validateQueryColumns();
		
		-- now start building the query
		v_query := 'select';
		
		-- first, build the datasource with the needed columns
		v_first := true;
		
		-- fill the associative array
		for i in 1 .. g_query_cols.LAST loop
			v_mycols(upper(g_query_cols(i).column_alias)) := g_query_cols(i).column_name;
		end loop;
		
		for i in 1..g_needed_datasource_cols.COUNT loop
			
			if not v_first then
				v_query := v_query || ', '||chr(10);
			else
				v_query := v_query || ' ';
				v_first := false;
			end if;
			
			-- determine if we need an alias or not
			if upper(v_mycols(g_needed_datasource_cols(i).column_name)) <> g_needed_datasource_cols(i).column_name then
				v_query := v_query || v_mycols(g_needed_datasource_cols(i).column_name) ||' as '|| g_needed_datasource_cols(i).column_name;				
			else
				v_query := v_query || g_needed_datasource_cols(i).column_name;
			end if;
		end loop;
		
		-- finish the data source		
		v_query := 	'from (' || chr(10) || 
						v_query || chr(10) || 
						substr(
							g_datasource_query,
							instr(lower(g_datasource_query),'from'),
							length(g_datasource_query)
						) || chr(10) || 
					')';
		
		--
		-- Now add the the data source query filters the user asked for
		--
		
		v_select := 'select ';
		v_bind := 1;
		
		if g_select_cols.count = 0 then
			raise_application_error(-20021,'At least one column needs to be selected from the datasource.');
		end if;
		
		-- select clause first
		v_first := true;
		for c in 1..g_select_cols.count loop
			if v_first then
				v_col := ' '; 
				v_first := false;	
			else
				v_col := ', ';
			end if;
				
			if g_select_cols(c).column_function is not null then
				v_col := v_col || g_select_cols(c).column_function || '(';
			end if;
			
			case g_select_cols(c).column_type
				when 'column' then 
					v_col := v_col || g_select_cols(c).column_name;
				when 'string' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'varchar2',
												helper.stripQuotes(g_select_cols(c).column_name));											  
					v_bind:=v_bind+1;
				when 'number' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'number',
												g_select_cols(c).column_name); 
					v_bind:=v_bind+1;
				else
					raise_application_error(-20022,'Invalid column_type in select clause detected while building the query.');
			end case;
			
			if g_select_cols(c).column_function is not null then
				v_col := v_col || ')';
			end if;
			
			if g_select_cols(c).column_alias is not null then
				v_col := v_col || ' as ' || g_select_cols(c).column_alias;
			end if;
			
			v_select := v_select || v_col;
			
		end loop;
		
		-- also add the row number if we need to
		if g_limit is not null or g_offset is not null then
			v_select := v_select || ', rownum as fourteam_rownum';
		end if; 
		
		-- add the select clause to our query
		v_query := v_select || chr(10) || v_query;
		
		-- continue with the where clause
		v_first := true;
		for c in 1..g_where_cols.count loop
			
			if v_first then
				v_col := chr(10) || ' where '; 
				v_first := false;
			else
				v_col := chr(10) || '   and ';
			end if;
			
			case g_where_cols(c).column_left_type
				when 'column' then 
					v_col := v_col || g_where_cols(c).column_left;
				when 'string' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'varchar2',
												helper.stripQuotes(g_where_cols(c).column_left));											  
					v_bind:=v_bind+1;
				when 'number' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'number',
												g_where_cols(c).column_left); 
					v_bind:=v_bind+1;
				else
					raise_application_error(-20023,'Invalid column_type in where clause detected while building the query.');
			end case;
			
			v_col := v_col || ' ' || g_where_cols(c).operator;
			
			if g_where_cols(c).operator not in ('is null','is not null') then
			  case g_where_cols(c).column_right_type
				when 'column' then 
					v_col := v_col || g_where_cols(c).column_right;
				when 'string' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'varchar2',
												helper.stripQuotes(g_where_cols(c).column_right));											  
					v_bind:=v_bind+1;
				when 'number' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'number',
												g_where_cols(c).column_right); 
					v_bind:=v_bind+1;
				else
					raise_application_error(-20024,'Invalid column_type in where clause detected while building the query.');
			  end case;
			end if;
			
			v_query := v_query || v_col;
			
		end loop;
		
		-- now group by
		v_first := true;
		for c in 1..g_groupby_cols.count loop
			if v_first then
				v_col := chr(10) || ' group by '; 
				v_first := false;	
			else
				v_col := ', ';
			end if;
				
			if g_groupby_cols(c).column_function is not null then
				v_col := v_col || g_groupby_cols(c).column_function || '(';
			end if;
			
			case g_groupby_cols(c).column_type
				when 'column' then 
					v_col := v_col || g_groupby_cols(c).column_name;
				when 'string' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'varchar2',
												helper.stripQuotes(g_groupby_cols(c).column_name));											  
					v_bind:=v_bind+1;
				when 'number' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'number',
												g_groupby_cols(c).column_name); 
					v_bind:=v_bind+1;
				else
					raise_application_error(-20025,'Invalid column_type in group by clause detected while building the query.');
			end case;
			
			if g_groupby_cols(c).column_function is not null then
				v_col := v_col || ')';
			end if;
			
			v_query := v_query || v_col;
			
		end loop;
		
		-- and now order by
		v_first := true;
		for c in 1..g_orderby_cols.count loop
			if v_first then
				v_col := chr(10) || ' order by '; 
				v_first := false;	
			else
				v_col := ', ';
			end if;
				
			if g_orderby_cols(c).column_function is not null then
				v_col := v_col || g_orderby_cols(c).column_function || '(';
			end if;
			
			case g_orderby_cols(c).column_type
				when 'column' then 
					v_col := v_col || g_orderby_cols(c).column_name;
				when 'string' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'varchar2',
												helper.stripQuotes(g_orderby_cols(c).column_name));											  
					v_bind:=v_bind+1;
				when 'number' 
					then v_col := v_col || ':b'||v_bind;
					v_bind_table(v_bind) := gdatasource_bind_obj(
												':b'||v_bind,
												'number',
												g_orderby_cols(c).column_name); 
					v_bind:=v_bind+1;
				else
					raise_application_error(-20026,'Invalid column_type in order by clause detected while building the query.');
			end case;
			
			if g_orderby_cols(c).column_function is not null then
				v_col := v_col || ')';
			end if;
			
			if g_orderby_cols(c).column_order is not null then
				v_col := v_col || ' ' || g_orderby_cols(c).column_order;
			end if;
			
			v_query := v_query || v_col;
			
		end loop;
		
		-- limit and offset
		if g_limit is not null or g_offset is not null then
			v_query := 	'select * from (' || chr(10) 
						|| v_query || chr(10) 
						|| ')' || chr(10)
						|| ' where fourteam_rownum';
			if g_limit is null then
				-- only offset
				v_query := v_query || ' >= :b' || v_bind;
				v_bind_table(v_bind) := gdatasource_bind_obj(
										':b'||v_bind,
										'number',
										g_offset
									);
				v_bind:=v_bind+1;
			else
				
				v_query := v_query || ' between :b' || v_bind || ' and :b' || (v_bind+1);
				
				if g_offset is null then
					v_from := 1;
				else
					v_from := to_number(g_offset);	
				end if;
				
				v_to := to_number(g_limit) + v_from - 1;
				
				v_bind_table(v_bind) := gdatasource_bind_obj(
											':b'||v_bind,
											'number',
											to_char(v_from)
										);
				v_bind_table(v_bind+1) := gdatasource_bind_obj(
											':b'||(v_bind+1),
											'number',
											to_char(v_to)
										);
				v_bind:=v_bind+2;
			end if;
		end if;				
		 
		-- parse cursor
		dbms_sql.parse(p_cursor,v_query,dbms_sql.NATIVE);
        
		-- add the bind variables
		for b in 1..v_bind_table.count loop
			case v_bind_table(b).bind_type
					when 'number' then 
						dbms_sql.bind_variable(
							p_cursor,
							v_bind_table(b).bind_name,													
							to_number(v_bind_table(b).bind_value)
						);
					when 'varchar2' then 
						dbms_sql.bind_variable(
							p_cursor,
							v_bind_table(b).bind_name,													
							v_bind_table(b).bind_value
						);
					when 'date' then 
						raise_application_error(-20027,'Date type bind not supported yet.'); 
					else 
						raise_application_error(-20028,'Invalid bind type detected when parsing query.');
			end case;
			
		end loop;
		
	end prepareCursor;
	
END GDataSource;
/
