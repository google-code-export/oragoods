CREATE OR REPLACE PACKAGE BODY GDataSource
	/**
	 * OraGoods - Copyright 2009 www.4tm.com.ar - Jose Luis Canciani
	 * Oracle PL/SQL Implementation for Google Data Source objects 
	 * 
	 * Some support to the Query Language included 
	 * http://code.google.com/apis/visualization/documentation/querylanguage.html#Clauses
	 * 
	 * Copyright Notice
	 *
  	 * This file is part of ORAGOODS, a library developed by Jose Luis Canciani
  	 * 
  	 * ORAGOODS is free software: you can redistribute it and/or modify
  	 * it under the terms of the GNU General Public License as published by
  	 * the Free Software Foundation, either version 3 of the License, or
  	 * (at your option) any later version.
  	 * 
  	 * ORAGOODS is distributed in the hope that it will be useful,
  	 * but WITHOUT ANY WARRANTY; without even the implied warranty of
  	 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  	 * GNU General Public License for more details.
  	 * 
  	 * You should have received a copy of the GNU General Public License
 	 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
	 * 
 	 */
AS 
	
	/*******************************************************************
	*
	* Private prodecures/functions start here
	*
	*******************************************************************/
	
	procedure debug(
		p_message 	IN 		varchar2
	) is
	begin
		if g_debug then
			dbms_output.put_line(p_message);
		end if;
	end debug;
	
	/**
	* Print procedures!
	*/
	procedure p(print in varchar2)
	is
	begin
		if g_debug then
			dbms_output.put_line(print);
		else
			htp.p(print);
		end if;
	end p;
	
	procedure prn(
		print in varchar2
	) is
	begin
		if g_debug then
			dbms_output.put(print);
		else
			htp.prn(print);
		end if;
	end prn;
	
	procedure nl
	is
	begin
		if g_debug then
			dbms_output.new_line;	
		else
			htp.prn(chr(10));	
		end if;
	end nl;
	
	/**
	* Clean package variables for running it again
	*/
	procedure clean
	is
	begin
		g_google_query := null;
		g_datasource_select_clause := null;
		g_datasource_rest_of_clause	:= null;
		g_datasource_columns.delete;
		g_datasource_columns_full.delete;
		g_datasource_needed_columns.delete;
		g_datasource_bind_values.delete;
		g_datasource_bind_types.delete;
		g_datasource_labels.delete;
		g_datasource_formats.delete;
		g_parsed_query	:= null;
		g_opt_no_format	:= false;
		g_opt_no_values	:= false;
	end clean;
	
	function trimme(
		p_string	IN		varchar2
	) return varchar2
	is
	begin
		return
			ltrim(
				rtrim(
					p_string,
					' '||chr(9)||chr(10)||chr(13)||chr(32)),
				' '||chr(9)||chr(10)||chr(13)||chr(32)
			);
	end trimme;
	
	/**
	*  encodeJsonString: encode text with special characters for sending into a JSON
	*		Based on http://www.json.org/
	*/
	procedure printJsonString(
		p_string IN varchar2
	) is
		v_letter 	varchar2(1 char);
		v_buffer 	varchar2(2000 char) := '';
		v_count		pls_integer			:= 0;
	begin
		if p_string is not null and length(p_string) > 0 then
			for l in 1..length(p_string)
			loop
				v_letter := substr(p_string,l,1);
				v_count := v_count + 1;
				v_buffer := v_buffer || 
					case v_letter
						when '\' 		then '\\'
						when '"' 		then '\"'
						when '/' 		then '\/'
						when chr(10)	then '\n'
						when chr(13)	then '\r'
						when chr(9)		then '\t'
						else 			v_letter
					end;
				if v_count = 1000 then
					prn(v_buffer);
					v_count := 0;
					v_buffer := '';
				end if;
			end loop;
			if v_count > 0 then
				prn(v_buffer);
			end if;
		end if;
	end printJsonString;
	
	/**
	*  encodeJsonString: encode text with special characters for sending into a JSON
	*		Based on http://www.json.org/
	*/
	procedure printJsonString(
		p_string IN CLOB
	) is
		v_count binary_integer;
		v_read binary_integer;
		v_text_buffer varchar2(2000 char);
	begin
   		v_count := 1;
   		v_read := 2000;
		loop
			DBMS_LOB.READ (
            	p_string, v_read , v_count, v_text_buffer
			);
			printJsonString(v_text_buffer);
			v_count := v_count  + v_read;
   		end loop;
	exception
   		WHEN NO_DATA_FOUND THEN
			return;
	end printJsonString;
	 
	/**
	*  extractQueryClause
	*  @param string p_query The query string to extract from
	*  @param string p_clause The clause to extract: select | where | etc
	*/
	procedure extractQueryClauses(
		p_query		IN varchar2,
		p_select	OUT varchar2,
		p_where		OUT varchar2,
   		p_groupby	OUT varchar2,
   		p_pivot		OUT varchar2,
   		p_orderby	OUT varchar2,
   		p_limit		OUT varchar2,
   		p_offset	OUT varchar2,
   		p_label		OUT varchar2,
   		p_format	OUT varchar2,
  		p_options	OUT varchar2
	) is
		v_query 			varchar2(32767) 	:= trimme(p_query);
		v_pos_select 		pls_integer 		:= 0;
		v_pos_where 		pls_integer 		:= 0;
		v_pos_groupby 		pls_integer 		:= 0;
		v_pos_pivot 		pls_integer 		:= 0;
		v_pos_orderby 		pls_integer 		:= 0;
		v_pos_limit 		pls_integer 		:= 0;
		v_pos_offset 		pls_integer 		:= 0;
		v_pos_label 		pls_integer 		:= 0;
		v_pos_format 		pls_integer 		:= 0;
		v_pos_options 		pls_integer 		:= 0;
	begin
		-- start getting the positions for each clause
		declare
			v_current_word	varchar2(32767) := '';
			v_last_word		varchar2(32767) := '';
			v_current_quote varchar2(1) := null;
			v_letter		varchar2(1) := '';
			v_last_letter	varchar2(1) := '';
		begin
			for i in 1..length(v_query)
			loop
				
				v_letter := substr(v_query,i,1);
				
				if v_letter in (' ',chr(9),chr(10),chr(13),chr(32)) then
					-- when end of word
					
					-- if this isn't "another" space and I'm not being quoted then...
					if v_last_letter not in (' ',chr(9),chr(10),chr(13),chr(32)) and v_current_quote is null then
						
						-- search clause keywords
						case lower(v_current_word)  
							when 'select' 			then if v_pos_select  > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_select   := i+1; end if;
							when 'where' 			then if v_pos_where   > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_where    := i+1; end if;
							when 'by' then
								case lower(v_last_word)                                                                                                                           
									when 'group' 	then if v_pos_groupby > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_groupby  := i+1; end if;
									when 'order' 	then if v_pos_orderby > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_orderby  := i+1; end if;
									else null;                                                                                                                                    
								end case;				                                                                                                                          
							when 'pivot' 			then if v_pos_pivot   > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_pivot    := i+1; end if;
							when 'limit' 			then if v_pos_limit   > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_limit    := i+1; end if;
							when 'offset' 			then if v_pos_offset  > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_offset   := i+1; end if;
							when 'label' 			then if v_pos_label   > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_label    := i+1; end if;
							when 'format' 			then if v_pos_format  > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_format   := i+1; end if;
							when 'options' 			then if v_pos_options > 0 then raise_application_error(-20003,'Parse error: duplicate clause found'); else v_pos_options  := i+1; end if;
							else null;
						end case;
						-- since I finished a word, clean it
						v_last_word := v_current_word;
						v_current_word := '';
					elsif v_current_quote is not null then
						v_current_word := v_current_word || v_letter;
					end if;
				elsif v_letter in ('''','"','`') then
					-- when quote
					if v_current_quote is not null and v_letter = v_current_quote then
						-- end quote
						v_current_quote := null;
					elsif v_letter is not null and v_letter <> v_current_quote then
						-- ignore quote inside another quote
						null;
					else
						v_current_quote := v_letter;
					end if;
					v_current_word := v_current_word || v_letter;
				else
					-- continue
					v_current_word := v_current_word || v_letter;
				end if;
				
				v_last_letter := v_letter;
									
			end loop;
			if v_current_quote is not null then
				raise_application_error(-20000,'Parsing error: quote not closed');
			end if;
		end;
		
		-- check for valid order
		declare
			v_pos2_select 		pls_integer 		:= v_pos_select ;
			v_pos2_where 		pls_integer 		:= v_pos_where ;
			v_pos2_groupby 		pls_integer 		:= v_pos_groupby;
			v_pos2_pivot 		pls_integer 		:= v_pos_pivot ;
			v_pos2_orderby 		pls_integer 		:= v_pos_orderby;
			v_pos2_limit 		pls_integer 		:= v_pos_limit ;
			v_pos2_offset 		pls_integer 		:= v_pos_offset ;
			v_pos2_label 		pls_integer 		:= v_pos_label ;
			v_pos2_format 		pls_integer 		:= v_pos_format ;
			v_pos2_options 		pls_integer 		:= v_pos_options;
		begin
			if v_pos2_where = 0 then
				v_pos2_where := v_pos2_select;
			end if;
			if v_pos2_groupby = 0 then
				v_pos2_groupby := v_pos2_where;
			end if;
			if v_pos2_pivot = 0 then
				v_pos2_pivot := v_pos2_groupby;
			end if;
			if v_pos2_orderby = 0 then
				v_pos2_orderby := v_pos2_pivot;
			end if;
			if v_pos2_limit = 0 then
				v_pos2_limit := v_pos2_orderby;
			end if;
			if v_pos2_offset = 0 then
				v_pos2_offset := v_pos2_limit;
			end if;
			if v_pos2_label = 0 then
				v_pos2_label := v_pos2_offset;
			end if;
			if v_pos2_format = 0 then
				v_pos2_format := v_pos2_label;
			end if;
			if v_pos2_options = 0 then
				v_pos2_options := length(v_query);
			end if;
			
			if 		v_pos2_select  	> v_pos2_where 
				or 	v_pos2_where 	> v_pos2_groupby
				or 	v_pos2_groupby	> v_pos2_pivot 
				or 	v_pos2_pivot 	> v_pos2_orderby
				or 	v_pos2_orderby 	> v_pos2_limit 
				or 	v_pos2_limit 	> v_pos2_offset 
				or 	v_pos2_offset   > v_pos2_label  
				or 	v_pos2_label    > v_pos2_format 
				or 	v_pos2_format   > v_pos2_options
			then
				raise_application_error(-20001,'Parsing error: invalid order of the query clauses.');
			end if;
		end; 
						
		-- extract clauses 
		declare
			v_end	pls_integer	:= length(v_query) + 1;		
		begin
			-- get options
			if v_pos_options > 0 and v_pos_options < v_end then
				p_options := trimme(substr(v_query,v_pos_options,v_end-v_pos_options));
				v_end := v_pos_options - length('options ') - 1;
			end if;
			-- get format
			if v_pos_format > 0 and v_pos_format < v_end then
				p_format := trimme(substr(v_query,v_pos_format,v_end-v_pos_format));
				v_end := v_pos_format - length('format ') - 1;
			end if;
			-- get label
			if v_pos_label > 0 and v_pos_label < v_end then
				p_label := trimme(substr(v_query,v_pos_label,v_end-v_pos_label));
				v_end := v_pos_label - length('label ') - 1;
			end if;
			-- get offset
			if v_pos_offset > 0 and v_pos_offset < v_end then
				p_offset := trimme(substr(v_query,v_pos_offset,v_end-v_pos_offset));
				v_end := v_pos_offset - length('offset ') - 1;
			end if;
			-- get limit
			if v_pos_limit > 0 and v_pos_limit < v_end then
				p_limit := trimme(substr(v_query,v_pos_limit,v_end-v_pos_limit));
				v_end := v_pos_limit - length('limit ') - 1;
			end if;
			-- get order by
			if v_pos_orderby > 0 and v_pos_orderby < v_end then
				p_orderby := trimme(substr(v_query,v_pos_orderby,v_end-v_pos_orderby));
				v_end := v_pos_orderby - length('order by ') - 1;
			end if;
			-- get pivot
			if v_pos_pivot > 0 and v_pos_pivot < v_end then
				p_pivot := trimme(substr(v_query,v_pos_pivot,v_end-v_pos_pivot));
				v_end := v_pos_pivot - length('pivot ') - 1;
			end if;
			-- get group by
			if v_pos_groupby > 0 and v_pos_groupby < v_end then
				p_groupby := trimme(substr(v_query,v_pos_groupby,v_end-v_pos_groupby));
				v_end := v_pos_groupby - length('group by ') - 1;
			end if;
			-- get group by
			if v_pos_where > 0 and v_pos_where < v_end then
				p_where := trimme(substr(v_query,v_pos_where,v_end-v_pos_where));
				v_end := v_pos_where - length('where ') - 1;
			end if;
			-- finally get select
			if v_pos_select > 0 and v_pos_select < v_end then
				p_select := trimme(substr(v_query,v_pos_select,v_end-v_pos_select));
			end if;
		end;
														
	end extractQueryClauses;
	
	procedure explode(
		p_string				IN		varchar2,
		p_separator				IN		varchar2,
		p_table					OUT		t_varchar2,
		p_trim					IN		boolean default true,
		p_ignore_empty_strings	IN		boolean	default false,
		p_exit					IN		pls_integer default 0,
		p_separator_case		IN		boolean default false
	) is
		v_current_quote		varchar2(1 char) 	:= null;
		v_parenthesis_count	pls_integer			:= 0;
		v_current_word		varchar2(32767) 	:= '';
		v_separator_length	pls_integer 		:= length(p_separator);
		v_letter			varchar2(1 char)	:= '';
	begin
		
		if length(p_string) is not null or length(p_string) > 0 then
		
			for i in 1..length(p_string)
			loop
				
				v_letter := substr(p_string,i,1);
				v_current_word := v_current_word || v_letter;
				
				if v_letter in ('''','"','`') then
					if v_current_quote is null then
						v_current_quote := v_letter;
					elsif v_current_quote = v_letter then
						v_current_quote := null;
					end if;
				elsif v_current_quote is null and v_letter = '(' then
					v_parenthesis_count := v_parenthesis_count + 1;
				elsif v_current_quote is null and v_letter = ')' then
					if v_parenthesis_count = 0 then
						raise_application_error(-20016,'Parsing error: close parenthesis was never opened');
					else
						v_parenthesis_count := v_parenthesis_count - 1;
					end if;
				end if;
				
				if 	v_current_quote is null 
					and v_parenthesis_count = 0 
					and (
						(p_separator_case=true and substr(v_current_word,-1*v_separator_length) = p_separator) 
						or
						(p_separator_case=false and lower(substr(v_current_word,-1*v_separator_length)) = lower(p_separator))
					)
				then
					
					if p_trim then
						v_current_word := trimme(substr(v_current_word,1,length(v_current_word)-v_separator_length));
					else
						v_current_word := substr(v_current_word,1,length(v_current_word)-v_separator_length);
					end if;
					
					if p_ignore_empty_strings = true and (v_current_word = '' or v_current_word is null) then
						-- ignore!
						null;
					else
						p_table(p_table.count+1) := v_current_word;
						v_current_word := '';
						if p_exit > 0 and p_exit = (p_table.count) then
							return;
						end if;
					end if;	
												
				end if;
				
			end loop;
			
			if p_trim then
				v_current_word := trimme(v_current_word);
			end if;
			
			if p_ignore_empty_strings = true and (v_current_word = '' or v_current_word is null) then
				-- ignore!
				null;
			else
				p_table(p_table.count+1) := v_current_word;
			end if;
			
		end if;
		
		if v_parenthesis_count > 0 then
			raise_application_error(-20017,'Parsing error: opened parenthesis is never closed');
		end if;
		
	end explode;
		
	/**
     * @param p_attr
     * @param tqx
     * @return VARCHAR2
     * 
     * Description: receives a tqx string (see google data source) and returns the attr value requested.
     * Returns NULL if nothing is found
     * 
     * Example:
     *    the call
     * 		get_tqx_attr('reqId','version:0.5;reqId:1;sig:5277771;out:json;responseHandler:myQueryHandler');
     *    would return 
     * 		1
     * 
     */
    function get_tqx_attr(
        p_attr 	in varchar2,
        tqx 	in varchar2
	) return varchar2
    is
        v_attributes	t_varchar2;
        v_attr_values	t_varchar2;
    begin
        
        explode(tqx,';',v_attributes);
        
        for i in 1..nvl(v_attributes.count,0) loop
            
            explode(v_attributes(i),':',v_attr_values);
            
            for j in 1..nvl(v_attr_values.count,0) loop
                if j = 1 and v_attr_values(1) = p_attr then
                    return v_attr_values(2);
                end if;
            end loop;
        end loop;
        
        -- nothing found
        return null;
        
    end get_tqx_attr;
	
	procedure checkIfColumnIsInDataSource(
		p_column			IN		varchar2
	) is
	begin
		for i in 1..g_datasource_columns.count loop
			if g_datasource_columns(i) = p_column then
				-- this column is referenced by google query, so we need to include it
				g_datasource_needed_columns(i) := 'YES';
				return;
			end if;
		end loop;
		raise_application_error(-20026,'Column not found in Data Source: '||substr(p_column,1,30));
	end checkIfColumnIsInDataSource;
	
	procedure getColID(
		p_string			IN		varchar2,
		p_colid				OUT		varchar2,
		p_rest				OUT		varchar2
	) is
		v_letter varchar2(1 char);
	begin
		p_colid := '';
		p_rest := null;
		v_letter := substr(p_string,1,1);
		if v_letter = '`' then
			-- find the rest of the ID
			for l in 2..length(p_string) loop
				v_letter :=  substr(p_string,l,1);
				if v_letter = '`' then
					p_rest := trimme(substr(p_string,(l+1),length(p_string)-(l+1)+1));
					return;
				end if;
				p_colid := p_colid || v_letter;
			end loop;
			raise_application_error(-20005,'Parsing error: expecting ` found: '||v_letter);
		else
			raise_application_error(-20004,'Parsing error: expecting ` found: '||v_letter);
		end if;
	end getColID;
	
	procedure getNumber(
		p_string			IN		varchar2,
		p_number			OUT		varchar2,
		p_rest				OUT		varchar2
	) is
		v_letter varchar2(1 char);
		v_dot	 boolean	:= false;
	begin
		
		p_rest := null;
		p_number := '';
		
		-- get the number
		for l in 1..length(p_string) loop
			v_letter :=  substr(p_string,l,1);
			if 	v_letter in ('0','1','2','3','4','5','6','7','8','9')
				or
				(v_letter = '-' and l = 1)
				or
				(v_letter = '.' and nvl(instr(p_number,'.'),0)=0 )
			then
				p_number := p_number || v_letter;
			elsif v_letter in (' ',chr(9),chr(10),chr(13),chr(32)) then
				-- finish!
				p_rest := trimme(substr(p_string,(l+1),length(p_string)-(l+1)+1));
				exit;
			else
				raise_application_error(-20009,'Parsing error: wrong number, found character: '||v_letter);
			end if;
			
		end loop;
		
		-- check for valid number
		declare
			v_number number;
		begin
			v_number := to_number(p_number);
		exception
			when others then
				if sqlcode = -1722 then
					raise_application_error(-20010,'Parsing error: wrong number: '||p_number);
				else
					raise;
				end if;
		end;
		
	end getNumber;
	
	procedure getString(
		p_string_in			IN		varchar2,
		p_string			OUT		varchar2,
		p_rest				OUT		varchar2
	) is
		v_letter varchar2(1 char);
		v_quote  varchar2(1 char);
	begin
		p_string := '';
		p_rest := null;
		v_letter := substr(p_string_in,1,1);
		
		if v_letter in ('''','"') then
			-- find the rest of the ID
			v_quote := v_letter;
			for l in 2..length(p_string_in) loop
				v_letter :=  substr(p_string_in,l,1);
				if v_letter = v_quote then
					p_rest := trimme(substr(p_string_in,(l+1),length(p_string_in)-(l+1)+1));
					return;
				end if;
				p_string := p_string || v_letter;
			end loop;
		end if;	
		raise_application_error(-20008,'Parsing error: expecting '' or " and found: '||v_letter);
		
	end getString;
	
	procedure getLabel(
		p_string_in			IN		varchar2,
		p_string			OUT		varchar2,
		p_rest				OUT		varchar2
	) is
		v_letter varchar2(1 char);
		v_quote  varchar2(1 char);
	begin
		p_string := '';
		p_rest := null;
		v_quote := substr(p_string_in,length(p_string_in),1);
		
		if v_quote in ('''','"') then
			-- find the rest of the label
			for l in reverse 1..(length(p_string_in)-1) loop
				v_letter :=  substr(p_string_in,l,1);
				if v_letter = v_quote then
					-- label found, now collect the column and exit
					p_rest := trimme(substr(p_string_in,1,l-1));
					return;
				end if;
				p_string := v_letter || p_string;
			end loop;
			raise_application_error(-20042,'Parsing error: invalid label, beggining of quote not found');
		else
			raise_application_error(-20041,'Parsing error: invalid label, end of quote not found');
		end if;
		
	end getLabel;
	
	function getAlias(
		p_string			IN		varchar2
	) return varchar2
	is
		v_letter varchar2(1 char);
		p_rest varchar2(32767);
	begin
		v_letter := substr(p_string,1,1);
		if v_letter = '"' then
			-- find the rest of the ID
			for l in 2..length(p_string) loop
				if substr(p_string,l,1) = '"' then
					p_rest := trimme(substr(p_string,(l+1),length(p_string)-(l+1)+1));
					if length(p_rest) is null or length(p_rest) = 0 then
						return substr(p_string,2,l-2);
					else
						raise_application_error(-20024,'Parsing error: found unexpected characters after double quote');
					end if;
				end if;
			end loop;
			raise_application_error(-20023,'Parsing error: expecting " and found: '||v_letter);
		else
			-- only a-b 1-9 _
			if lower(v_letter) in ('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z') then
				for l in 2..length(p_string) loop
					if lower(substr(p_string,l,1)) not in ('_','1','2','3','4','5','6','7','8','9','0','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z') then
						raise_application_error(-20025,'Parsing error: invalid character in column identifier: '||lower(substr(p_string,l,1)));
					end if;
				end loop;
				return p_string;
			else
				raise_application_error(-20025,'Parsing error: column identifier must start with a letter. Found: '||v_letter);		
			end if;
		end if;
	end getAlias;
	
	procedure getWord(
		p_string			IN		varchar2,
		p_word				OUT		varchar2,
		p_rest				OUT		varchar2
	) is
		v_letter varchar2(1 char);
	begin
		p_word := '';
		p_rest := null;
		v_letter := substr(p_string,1,1);
		-- find the rest of the ID
		for l in 1..length(p_string) loop
			v_letter :=  substr(p_string,l,1);
			if 
				-- accepted letters
				(l > 1 and lower(v_letter) not in ('_','0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'))
				OR
				(l = 1 and lower(v_letter) not in ('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'))
			then
				if v_letter not in (' ',chr(9),chr(10),chr(13),chr(32),'+','-','/','*','(') then
					raise_application_error(-20012,'Parsing error: invalid character on keyword: '||v_letter);	
				else
					p_rest := trimme(substr(p_string,(l),length(p_string)-l+1));
					return;
				end if;
			end if;			
			p_word := p_word || v_letter;
		end loop;
	end getWord;
	
	procedure getParams(
		p_argument_string		IN		varchar2,
		p_params				OUT		t_varchar2,
		p_rest					OUT		varchar2,
		p_trim					IN		boolean			DEFAULT true,
		p_ignore_empty_strings	IN		boolean			DEFAULT false
	) is
	begin
		
		p_rest := '';
		
		-- find first and last parenthesis
		if substr(p_argument_string,1,1) <> '(' then
			raise_application_error(-20013,'Invalid arguments, expecting: (');
		end if;
		if substr(p_argument_string,-1) <> ')' then
			raise_application_error(-20014,'Invalid arguments, ")" not properly closed near "'||substr(p_argument_string,1,6)||'..."');
		end if;
		
		explode(substr(p_argument_string,2,length(p_argument_string)-2),',',p_params,p_trim,p_ignore_empty_strings);
		
	end getParams;
	
	procedure findOperator(
		p_string			IN		varchar2,
		p_operator			OUT		varchar2,
		p_rest				OUT		varchar2,
		p_expression_type	IN		varchar2 default 'select'
	)
	is
		v_string			varchar2(32767) := trimme(p_string);
		v_letter			varchar2(1 char);
		v_letter2			varchar2(2 char);
		v_next_word			varchar2(100) := '';
		v_next_word1		varchar2(100) := null;
		v_next_word2		varchar2(100) := null;
		v_next_word_pos1	pls_integer := null;
		v_next_word_pos2	pls_integer := null;
	begin
		
		if p_expression_type = 'where' then
			-- find next words
			v_letter := ' ';
			for c in 1..length(v_string) loop
				v_letter2 := v_letter;
				v_letter := substr(v_string,c,1);
				if v_letter in (' ',chr(9),chr(10),chr(13),chr(32)) and v_letter2 not in (' ',chr(9),chr(10),chr(13),chr(32)) then
					-- end of word
					if v_next_word1 is null then
						v_next_word1 := v_next_word;
						v_next_word := '';
						v_next_word_pos1 := c;
					else
						v_next_word2 := v_next_word;
						v_next_word_pos2 := c;
						exit;
					end if;
				elsif v_letter not in (' ',chr(9),chr(10),chr(13),chr(32)) then
					v_next_word := v_next_word || v_letter;
				end if; 			
			end loop;
		end if;
		
		p_rest := '';
		p_operator := null;
		v_letter := substr(v_string,1,1);
		v_letter2 := substr(v_string,1,2);
		
		if p_expression_type = 'where' and v_letter2 in ('<=','>=','!=','<>') then
			p_operator := v_letter2;
			p_rest := trimme(substr(v_string,3,length(v_string)-1));
		elsif 	(p_expression_type = 'select' and (v_letter in ('+','*','/') OR (v_letter = '-' AND substr(v_string,2,1) not in ('0','1','2','3','4','5','6','7','8','9'))))
			or
			(p_expression_type = 'where' and (v_letter in ('<','>','=','+','*','/') OR (v_letter = '-' AND substr(v_string,2,1) not in ('0','1','2','3','4','5','6','7','8','9'))))
		then
			p_operator := v_letter;
			p_rest := trimme(substr(v_string,2,length(v_string)-1));
		elsif p_expression_type = 'where' and lower(v_next_word1) in ('is','or') then
			p_operator := lower(v_next_word1);
			p_rest := trimme(substr(v_string,3,length(v_string)-1));
		elsif p_expression_type = 'where' and lower(v_next_word1) in ('and') then
			p_operator := 'and';
			p_rest := trimme(substr(v_string,4,length(v_string)-1));
		elsif p_expression_type = 'where' and lower(v_next_word1) in ('contains','matches','like') then
			p_operator := lower(v_next_word1);
			p_rest := trimme(substr(v_string,v_next_word_pos1,length(v_string)-1));
		elsif p_expression_type = 'where' and (
				(lower(v_next_word1) in ('starts','ends') and lower(v_next_word2) = 'with')
			)
		then
			p_operator := lower(v_next_word1)||' '||lower(v_next_word2);
			p_rest := trimme(substr(v_string,v_next_word_pos2,length(v_string)-1));
		end if;
		
	end findOperator;
	
	procedure findExpressionInParenthesis(
		p_string		in	varchar2,
		p_string_out	out	varchar2,
		p_rest			out	varchar2
	) is
		v_letter				varchar2(1 char);
		v_current_quote			varchar2(1 char) := null;
		v_parenthesis_count		pls_integer := 0;
	begin
		
		if substr(p_string,1,1) != '(' then
			raise_application_error(-20038,'GDataSource Internal error, "(" character not found');
		end if;
		
		for i in 2..length(p_string) loop
				
			v_letter := substr(p_string,i,1);
			
			if v_letter in ('''','"','`') then
				if v_current_quote is null then
					v_current_quote := v_letter;
				elsif v_current_quote = v_letter then
					v_current_quote := null;
				end if;
			elsif v_current_quote is null and v_letter = '(' then
				v_parenthesis_count := v_parenthesis_count + 1;
			elsif v_current_quote is null and v_letter = ')' then
				if v_parenthesis_count = 0 then
					-- end!
					p_string_out := substr(p_string,2,i-2);
					p_rest := trimme(substr(p_string,i+1,length(p_string)));
					return;
				else
					v_parenthesis_count := v_parenthesis_count - 1;
				end if;
			end if;
			
		end loop;
			
		raise_application_error(-20039,'Parsing error: opened parenthesis is never closed');
		
	end findExpressionInParenthesis;
	
	
	/**
	*
	*  Receives an expresion and process it recursivly
	*
	*
	*/
	procedure processExpression(
		p_word				IN		varchar2,
		p_column_text		IN OUT	varchar2,
		p_expression_type	IN		varchar2 default 'select', -- select | where
		p_process_operator	IN		boolean default false
	) is
		v_this_word			varchar2(32767);
		v_letter			varchar2(1 char);
		v_word				varchar2(32767);
		v_params			t_varchar2;
		v_buffer			varchar2(32767);
		v_next_operator 	varchar2(10) := null;
		v_next_expression 	varchar2(32767) := null;
		v_rest				varchar2(32767)		:= '';
		v_operator 			varchar2(20 char);
		v_column_text		varchar2(32767) := '';
	begin
		
		if p_process_operator = true then
			v_rest := p_word;
		else
			v_this_word := p_word;
			
			v_letter := substr(v_this_word,1,1);
			
			-- what is this?
			if v_letter = '(' then
				
				-- find closing parameter
				findExpressionInParenthesis(v_this_word,v_this_word,v_rest);
				
				v_column_text := '(';
					-- recursive call to process next expression
					processExpression(v_this_word,v_column_text,p_expression_type);
				v_column_text := v_column_text || ')';
			
			elsif v_letter = '`' then
				-- is colID
				declare
					v_colid	varchar2(100);
				begin
					getColID(v_this_word,v_colid,v_rest);
					
					-- is colID in the gdatasource?
					checkIfColumnIsInDataSource(v_colid);
					
					-- store translated colID
					v_column_text := '"' || v_colid || '"';
					
				end;
			elsif v_letter in ('.','-','1','2','3','4','5','6','7','8','9','0') then
				-- is number
				declare
					v_number varchar2(100);
				begin
					getNumber(v_this_word,v_number,v_rest);
					
					-- store translated colID
					v_column_text := ':b' || g_datasource_bind_values.count;
					g_datasource_bind_values(g_datasource_bind_values.count) := v_number;
					g_datasource_bind_types(g_datasource_bind_types.count) := 'number';
					
				end;
			elsif v_letter in ('"','''') then
				-- is string literal
				declare
					v_string varchar2(100);
				begin
					getString(v_this_word,v_string,v_rest);
					
					-- store translated string
					v_column_text := ':b' || g_datasource_bind_values.count;
					g_datasource_bind_values(g_datasource_bind_values.count) := v_string;
					g_datasource_bind_types(g_datasource_bind_types.count) := 'varchar2';
					
				end;
			elsif lower(v_letter) in ('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z') then
				-- colName, aggr function, scalar function
				declare
					v_string 	varchar2(100);
				begin
					getWord(v_this_word,v_string,v_rest);
					-- is an invalid keyword?
					if lower(v_string) = 'null' then
						v_column_text := 'null';
					elsif p_expression_type = 'where' and lower(v_string) = 'not' then
						getWord(v_rest,v_string,v_rest);
						if lower(v_string) = 'null' then
							v_column_text := 'not null';
						else
							raise_application_error(-20011,'Parsing error: invalid character found after "not" keyword, expecting "null"');
						end if;
					elsif lower(v_string) in ('not','true','false','and','asc','by','false','format','group','label','limit','not','offset','options','desc','or','order','pivot','select','true','where') then
						-- invalid keyword in select
						raise_application_error(-20011,'Parsing error: invalid keyword found in '||p_expression_type||' clause: '||v_string);
					elsif lower(v_string) in ('date','datetime','timeofday','timestamp') then
						-- date-based literal
						declare
							v_date varchar2(100);
						begin
							
							getString(v_rest,v_date,v_rest);
							
							-- check for next expression (after string nothing is expected)
							if v_rest is not null and length(v_rest) > 0 then
								raise_application_error(-20013,'Parsing error: invalid expresion in date-based literal: '||substr(v_rest,1,6)||'...');
							end if;
							
							-- store translated colID
							case lower(v_string)
							when 'date' then
								v_column_text := 'to_date(:b' || g_datasource_bind_values.count || ',''yyyy-mm-dd'')';
							when 'timeofday' then
								v_column_text := 'to_date(:b' || g_datasource_bind_values.count || ',''hh24:mi:ss'')';
							when 'datetime' then
								v_column_text := 'to_timestamp(:b' || g_datasource_bind_values.count || ',''yyyy-mm-dd hh24:mi:ss.ff3'')';
							when 'timestamp' then
								v_column_text := 'to_timestamp(:b' || g_datasource_bind_values.count || ',''yyyy-mm-dd hh24:mi:ss.ff3'')';
							end case;
							
							g_datasource_bind_values(g_datasource_bind_values.count) := v_date;
							g_datasource_bind_types(g_datasource_bind_types.count) := 'varchar2';
													
						end;
					elsif lower(v_string) in ('avg','count','max','min','sum') then
						-- aggregation functions
						-- they receive a single column according to G. Query v0.7
						-- verify there's no extra parameters
						if p_expression_type = 'where' then
							raise_application_error(-20033,'Parsing error: invalid use of aggregation function "'|| lower(v_string) ||'" in where clause');
						end if;
						
						getParams(v_rest,v_params,v_rest,true,true);
						if v_params.count != 1 then
							raise_application_error(-20018,'Parsing error: wrong number of parameters in aggregation function '||lower(v_string));
						end if;
						
						v_column_text := lower(v_string) || '(';
							-- recursive call to process the parameter
							processExpression(v_params(1),v_column_text,p_expression_type);
						v_column_text := v_column_text || ')';
						
					elsif lower(v_string) in ('now') then
						
						-- no-argument functions
						case lower(v_string)
							when 'now' then
								-- store translated now function
								v_column_text := 'systimestamp';
						end case;
						
						-- verify there's no extra parameters
						getParams(v_rest,v_params,v_rest,true,true);
						if v_params.count > 0 then
							raise_application_error(-20012,'Parsing error: too many parameters in now() function');
						end if;
						
					elsif lower(v_string) in ('year','month','day','hour','minute','second','millisecond','quarter','dayofweek','todate','upper','lower') then
						-- one-argument functions
						getParams(v_rest,v_params,v_rest,true,false);
						if v_params.count != 1 then
							raise_application_error(-20012,'Parsing error: wrong parameter count in function '||lower(v_string));
						end if;
						-- store translated now function
						if lower(v_string) = 'todate' then
							-- return date
							v_column_text := 'gdatasource.toDate(';
								-- recursive call to process next expression
								processExpression(v_params(1),v_column_text,p_expression_type);
							v_column_text := v_column_text || ')';
						elsif lower(v_string) in ('upper','lower') then
							-- return string
							v_column_text := lower(v_string)||'(';
								-- recursive call to process next expression
								processExpression(v_params(1),v_column_text,p_expression_type);
							v_column_text := v_column_text || ')';
						else
							-- return number
							-- recursive call to process next expression
							processExpression(v_params(1),v_buffer,p_expression_type);
							case lower(v_string)
								when 'year' then
									v_column_text := 'to_number(to_char('||v_buffer||',''yyyy''))';
								when 'month' then
									v_column_text := 'to_number(to_char('||v_buffer||',''mm''))';
								when 'day' then
									v_column_text := 'to_number(to_char('||v_buffer||',''dd''))';
								when 'hour' then
									v_column_text := 'to_number(to_char('||v_buffer||',''hh24''))';
								when 'minute' then
									v_column_text := 'to_number(to_char('||v_buffer||',''mi''))';
								when 'second' then
									v_column_text := 'to_number(to_char('||v_buffer||',''ss''))';
								when 'millisecond' then
									v_column_text := 'to_number(to_char('||v_buffer||',''ff3''))';
								when 'quarter' then
									v_column_text := 'to_number(to_char('||v_buffer||',''q''))';
								when 'dayofweek' then
									v_column_text := 'to_number(to_char('||v_buffer||',''d''))';
							end case;
						end if;
						
					elsif lower(v_string) in ('datediff') then
						-- two-argument functions
						getParams(v_rest,v_params,v_rest,true,false);
						if v_params.count != 2 then
							raise_application_error(-20012,'Parsing error: wrong parameter count in function '||lower(v_string));
						end if;
						-- store translated datediff function
						-- recursive call to process next expression
						v_column_text := '(';
						processExpression(v_params(1),v_column_text);
						v_column_text := v_column_text || ') - (';
						processExpression(v_params(2),v_column_text);
						v_column_text := v_column_text || ')';
					
					else
						
						-- not a known keyword, it should be a column name
						checkIfColumnIsInDataSource(upper(v_string));
						v_column_text := upper(v_string);
						
					end if;
					
				end;
			else
				if v_letter = '' or v_letter is null then
					raise_application_error(-20002,'Parsing error: missing expresion in '||p_expression_type||' clause');
				else
					raise_application_error(-20006,'Parsing error: unexpected character in '||p_expression_type||' clause: '||v_letter);
				end if;
			end if;
			
		end if; -- if p_process_operator
			
		-- finally, if this is a where clause look for next operator
		v_rest := trimme(v_rest);
		if nvl(length(v_rest),0) > 0 then 
			
			findOperator(v_rest,v_operator,v_word,p_expression_type);
		
			if v_operator is not null then
				
				-- recursive call to process next expression, adding the operator_count parameter
				if v_operator in ('contains','ends with','starts with') then
				
					-- expecting string literal
					declare
						v_string varchar2(32767);
					begin
						
						getString(v_word,v_string,v_rest);
						
						case v_operator
							when 'contains' then
								v_column_text := 'contains(' || v_column_text || ',' || ':b' || g_datasource_bind_values.count || ') > 0';
								g_datasource_bind_values(g_datasource_bind_values.count) := v_string;
							when 'ends with' then
								v_column_text := v_column_text || ' like :b' || g_datasource_bind_values.count;
								g_datasource_bind_values(g_datasource_bind_values.count) := '%'||v_string;
							when 'starts with' then
								v_column_text := v_column_text || ' like :b' || g_datasource_bind_values.count;
								g_datasource_bind_values(g_datasource_bind_values.count) := v_string||'%';
						end case;
						
						g_datasource_bind_types(g_datasource_bind_types.count) := 'varchar2';
						
						p_column_text := p_column_text || ' ' || v_column_text;
						v_column_text := '';
						processExpression(v_rest,v_column_text,p_expression_type,true);
						p_column_text := p_column_text || v_column_text;
					end;
					
				else
					
					p_column_text := p_column_text || ' ' || v_column_text || ' ' || v_operator || ' ';
					v_column_text := '';
					processExpression(v_word,v_column_text,p_expression_type,false);
					p_column_text := p_column_text || v_column_text;
					
				end if;
			else
				raise_application_error(-20040,'Parsing error: invalid operator found in '||p_expression_type||' clause near "'||substr(v_rest,1,6)||'..."');
			end if;
		else
			p_column_text := p_column_text || v_column_text;
		end if;
		
	end processExpression;
	
	
	/*******************************************************************
	*
	* Public functions/procedures start here
	*
	*******************************************************************/
	
	
	/**
	* Parse a server database query string
	*/
	procedure setDataSource(
		p_datasource_query 			IN 			varchar2
	) is
		v_select_clause	    varchar2(32767) := '';
		v_select_columns	t_varchar2;
		v_select_column		t_varchar2;
	begin
		
		-- get select clause
		declare
			v_letter			varchar2(1 char) := '';
			v_last_letter		varchar2(1 char) := '';
			v_current_quote		varchar2(1 char) := '';
			v_current_word		varchar2(32767) := '';
			v_pos_select		pls_integer;
			v_pos_from			pls_integer;
		begin
			for i in 1..length(p_datasource_query)
			loop
				
				v_letter := substr(p_datasource_query,i,1);
				
				if v_letter in (' ',chr(9),chr(10),chr(13),chr(32)) then
					-- when end of word
					
					-- if this isn't "another" space and I'm not being quoted then...
					if v_last_letter not in (' ',chr(9),chr(10),chr(13),chr(32)) and v_current_quote is null then
						
						-- search clause keywords
						case lower(v_current_word)  
							when 'select' 			then v_pos_select   := i+1;
							when 'from' 			then v_pos_from     := i+1;
							else null;
						end case;
						-- since I finished a word, clean it
						if lower(v_current_word) = 'from' then
							exit;
						end if;
						v_current_word := '';
					elsif v_current_quote is not null then
						v_current_word := v_current_word || v_letter;
					end if;
				elsif v_letter in ('''','"','`') then
					-- when quote
					if v_current_quote is not null and v_letter = v_current_quote then
						-- end quote
						v_current_quote := null;
					elsif v_letter is not null and v_letter <> v_current_quote then
						-- ignore quote inside another quote
						null;
					else
						v_current_quote := v_letter;
					end if;
					v_current_word := v_current_word || v_letter;
				else
					-- continue
					v_current_word := v_current_word || v_letter;
				end if;
				
				v_last_letter := v_letter;
									
			end loop;
			
			if v_current_quote is not null then
				raise_application_error(-20020,'Parsing error on Datasource query: quote not closed');
			end if;
			
			-- finally get select
			g_datasource_select_clause := trimme(substr(p_datasource_query,v_pos_select,v_pos_from - 6 - v_pos_select));
			g_datasource_rest_of_clause := substr(p_datasource_query,v_pos_from-5);
			
		end;
		
		explode(g_datasource_select_clause,',',v_select_columns);
		
		if v_select_columns.count = 0 then
			raise_application_error(-20021,'Parsing error on Datasource query: no select columns found');
		end if;
		
		for c in 1..v_select_columns.count loop
			
			explode(v_select_columns(c),' as ',v_select_column);
			
			if v_select_column.count > 2 then
				raise_application_error(-20022,'Parsing error on Datasource query: invalid column, found to many aliases');
			end if;
			
			if v_select_column.count = 2 then
				-- there's an alias!
				if substr(v_select_column(2),1,1) = '"' then
					g_datasource_columns(c) := getAlias(v_select_column(2));
				else
					g_datasource_columns(c) := upper(getAlias(v_select_column(2)));
				end if;
			else
				-- no alias!
				if substr(v_select_column(1),1,1) = '"' then
					g_datasource_columns(c) := getAlias(v_select_column(1));
				else
					g_datasource_columns(c) := upper(getAlias(v_select_column(1)));
				end if;
			end if;
			
			-- mark this column as "not needed" for now. If the google query references it, we will then mark it as YES 
			g_datasource_needed_columns(c) := 'NO';
			g_datasource_columns_full(c) := v_select_columns(c);
			
		end loop;
		
	end setDataSource;
	
	/**
	* parse a client's google datasource query string
	*/
	procedure setQuery(
		p_google_query 				IN 			varchar2
	) is
	
		-- for extracting query clauses
		v_select_clause	    varchar2(32767) := '';
		v_where_clause		varchar2(32767) := '';
		v_groupby_clause	varchar2(32767) := '';
		v_pivot_clause		varchar2(32767) := '';
		v_orderby_clause	varchar2(32767) := '';
		v_limit_clause      varchar2(32767) := '';
		v_offset_clause	    varchar2(32767) := '';
		v_label_clause		varchar2(32767) := '';
		v_format_clause	    varchar2(32767) := '';
		v_options_clause	varchar2(32767) := '';
		
		-- select extract variables
		v_select_cols		t_varchar2;
		v_groupby_cols		t_varchar2;
		v_orderby_cols		t_varchar2;
		v_label_cols		t_varchar2;
		v_format_cols		t_varchar2;
		v_option_cols		t_varchar2;
		
		-- the new query!
		v_parsed_query		varchar2(32767);
				
		-- needed vars
		v_label				varchar2(32767);
		v_rest				varchar2(32767);
		v_datasource_labels_cols	t_varchar2; -- store labels
		v_datasource_labels_values	t_varchar2; -- store labels
		v_datasource_formats_cols	t_varchar2; -- store formats
		v_datasource_formats_values	t_varchar2; -- store formats
    v_comma       boolean;
		
		v_buffer			varchar2(32767); -- temporary store translated select clause
		v_where_buffer		varchar2(32767); -- temporary store the where clause
		v_select_count		pls_integer;
		
		-- limit and offset
		v_limit				pls_integer;
		v_offset			pls_integer;
		
	begin
		
		if g_datasource_columns.count = 0 then
			raise_application_error(-20024,'No google datasource set, please run the setDataSource() method first');
		end if;
		
		g_google_query := p_google_query;
		
		-- start parsing the query
		extractQueryClauses(
			p_google_query,
			v_select_clause,
			v_where_clause,
	   		v_groupby_clause,
	   		v_pivot_clause,
	   		v_orderby_clause,
	   		v_limit_clause,
	   		v_offset_clause,
	   		v_label_clause,
	   		v_format_clause,
	  		v_options_clause	
		);
		/*
		debug('v_select_clause    : ->' || v_select_clause || '<- ' ) ;
		debug('v_where_clause     : ->' || v_where_clause  || '<- ' ) ;
		debug('v_groupby_clause   : ->' || v_groupby_clause ||'<- ' ) ;
		debug('v_pivot_clause     : ->' || v_pivot_clause  || '<- ' ) ;
		debug('v_orderby_clause   : ->' || v_orderby_clause ||'<- ' ) ;
		debug('v_limit_clause     : ->' || v_limit_clause  || '<- ' ) ;
		debug('v_offset_clause    : ->' || v_offset_clause || '<- ' ) ;
		debug('v_label_clause     : ->' || v_label_clause  || '<- ' ) ;
		debug('v_format_clause    : ->' || v_format_clause || '<- ' ) ;
		debug('v_options_clause   : ->' || v_options_clause || '<-'	);
		*/
		
		-- start building the query
		v_parsed_query := 'select ';
		
		-- load labels
		explode(v_label_clause,',',v_label_cols);
		for i in 1..nvl(v_label_cols.count,0) loop
			getLabel(v_label_cols(i),v_label,v_rest);
			v_datasource_labels_cols(i) := '';
			processExpression(v_rest,v_datasource_labels_cols(i),'select');
			v_datasource_labels_values(i) := v_label;
		end loop;
		
		-- load formats
		explode(v_format_clause,',',v_format_cols);
		for i in 1..nvl(v_format_cols.count,0) loop
			getLabel(v_format_cols(i),v_label,v_rest);
			v_datasource_formats_cols(i) := '';
			processExpression(v_rest,v_datasource_formats_cols(i),'select');
			v_datasource_formats_values(i) := v_label;
		end loop;
		
		-- select clause	
		explode(v_select_clause,',',v_select_cols);
		
		if v_select_cols.count = 0 then
			raise_application_error(-20050,'Parse error: select clause is required');
		end if;
		
		-- * or no select clause
		if 	v_select_cols.count = 1 and v_select_cols(1) = '*' then
			v_select_count := g_datasource_columns.count;
		else
			v_select_count := v_select_cols.count;
		end if;
		
		for i in 1..v_select_count
		loop
			-- parse the google query select clause and validate and translate each column
			
			v_buffer := '';
			
			if v_select_cols.count = 1 and v_select_cols(1) = '*' then
				-- get the column from the datasource
				v_buffer := g_datasource_columns(i);
				g_datasource_needed_columns(i) := 'YES';
			else
				-- first process the column
				processExpression(v_select_cols(i),v_buffer,'select');
			end if;
			
			-- concat to the parsed query
			if i > 1 then
				v_parsed_query := v_parsed_query || ', ' || v_buffer;
			else
				v_parsed_query := v_parsed_query || v_buffer;
			end if;
			
			-- add the label if any
			g_datasource_labels(i) := null;
			for l in 1..nvl(v_datasource_labels_cols.count,0) loop
				if v_datasource_labels_cols(l) = v_buffer then
					g_datasource_labels(i) := v_datasource_labels_values(l);
					exit;
				end if;
			end loop;
			
			-- add the format if any
			g_datasource_formats(i) := null;
			for l in 1..nvl(v_datasource_formats_cols.count,0) loop
				if v_datasource_formats_cols(l) = v_buffer then
					g_datasource_formats(i) := v_datasource_formats_values(l);
					exit;
				end if;
			end loop;
			
		end loop;
		
		-- where clause (process now to add needed columns to the datasource)
		if length(v_where_clause) is not null and length(v_where_clause) > 0 then
			processExpression(v_where_clause,v_where_buffer,'where');
		end if;
		
		-- limit
		if nvl(length(v_limit_clause),0) > 0 then
			begin
				v_limit := to_number(v_limit_clause);
			exception
				when others then
					raise_application_error(-20042,'Parse error: invalid limit number');
			end;
		else
			v_limit := 0;
		end if;
		
		-- offset
		if nvl(length(v_offset_clause),0) > 0 then
			begin
				v_offset := to_number(v_offset_clause);
			exception
				when others then
					raise_application_error(-20044,'Parse error: invalid offset number');
			end;
		else
			v_offset := 0;
		end if;
		
		-- add the DataSource query
		if g_datasource_needed_columns.count = 0 then
			raise_application_error(-20050,'Parse error: no reference to a Datasource column found on the query');
		end if;
		v_parsed_query := v_parsed_query || ' from ('||chr(10)||'select ';
    v_comma := false;
		for i in 1..g_datasource_needed_columns.count loop
			if g_datasource_needed_columns(i) = 'YES' then
        -- add comma only if this is not the first needed column
        if not v_comma then
          for j in 1..(i-1) loop
            if g_datasource_needed_columns(j) = 'YES' then
              v_comma := true;
              exit;
            end if;
          end loop;
        end if;
        if v_comma then
          v_parsed_query := v_parsed_query || ', ';
        end if;
				v_parsed_query := v_parsed_query || g_datasource_columns_full(i);
			end if;
		end loop;
		v_parsed_query := v_parsed_query || chr(10) || g_datasource_rest_of_clause || chr(10)|| ')';
		
		-- where clause
		if length(v_where_clause) is not null and length(v_where_clause) > 0 then
			v_parsed_query := v_parsed_query || chr(10) || ' where ' || v_where_buffer;
		end if;
		
		-- group by clause
		explode(v_groupby_clause,',',v_groupby_cols);
		
		-- * or no select clause
		for i in 1..nvl(v_groupby_cols.count,0)
		loop
			-- parse the google query group by clause and validate and translate each column
			if i > 1 then
				v_parsed_query := v_parsed_query || ', ';
			else
				v_parsed_query := v_parsed_query || chr(10) || 'group by ';
			end if;
			-- process the column
			processExpression(v_groupby_cols(i),v_parsed_query,'select');
			
		end loop;
		
		-- TODO: pivot clause!
		
		-- order by clause
		explode(v_orderby_clause,',',v_orderby_cols);
		
		-- * or no select clause
		declare
			v_order			varchar2(5);
			v_order_by_col 	varchar2(32767);
		begin
			for i in 1..nvl(v_orderby_cols.count,0)
			loop
				
				-- find asc/desc keyword
				if lower(trimme(substr(v_orderby_cols(i),-4))) = 'asc' then
					v_order := ' asc';
					v_order_by_col := substr(v_orderby_cols(i),1,length(v_orderby_cols(i))-4);
				elsif lower(trimme(substr(v_orderby_cols(i),-5))) = 'desc' then
					v_order := ' desc';
					v_order_by_col := substr(v_orderby_cols(i),1,length(v_orderby_cols(i))-5);
				else
					v_order := '';
					v_order_by_col := v_orderby_cols(i);	
				end if;
				
				-- parse the google query group by clause and validate and translate each column
				if i > 1 then
					v_parsed_query := v_parsed_query || ', ';
				else
					v_parsed_query := v_parsed_query || chr(10) || 'order by ';
				end if;
				
				-- process the column
				processExpression(v_order_by_col,v_parsed_query,'select');
				
				-- add order
				v_parsed_query := v_parsed_query || v_order;
				
			end loop;
		end;
		
		-- limit and offset
		if v_offset > 0 or v_limit > 0 then
			
			if v_offset = 0 then
				v_offset := 1;
			end if;
			
			v_parsed_query := 	'select iv.*, rownum rnum from ( '|| chr(10) ||
								v_parsed_query || chr(10) ||
								') ';
			if v_limit > 0 then
				v_parsed_query := v_parsed_query
								|| 'iv where rownum < :b' || g_datasource_bind_values.count || chr(10);
				g_datasource_bind_values(g_datasource_bind_values.count) := to_char(v_limit + v_offset);
				g_datasource_bind_types(g_datasource_bind_types.count) := 'number';
			end if;
			
			if v_offset > 1 then
				v_parsed_query := 	'select * from (' || chr(10) ||
									v_parsed_query || chr(10) ||
									') where rnum >= :b' || g_datasource_bind_values.count;
				g_datasource_bind_values(g_datasource_bind_values.count) := to_char(v_offset);
				g_datasource_bind_types(g_datasource_bind_types.count) := 'number';
			end if;
			
		end if;
		
		-- load options
		explode(v_options_clause,',',v_option_cols);
		for i in 1..nvl(v_option_cols.count,0) loop
			case lower(v_option_cols(i))
				when 'no_format' then
					g_opt_no_format	:= true;
				when 'no_values' then
					g_opt_no_values	:= true;
				else
					raise_application_error(-20050,'Parse error: invalid Option found');				
			end case;
		end loop;
		
		debug('Final query is: '||v_parsed_query);
		
		g_parsed_query := v_parsed_query;
		
	end setQuery;
	
	/**
	* build, parse and bind query based on datasource query and datasource cursor
	*/
	procedure prepareCursor(
		p_cursor 					IN OUT		NUMBER
	) is
	begin
		-- parse cursor
		dbms_sql.parse(p_cursor,g_parsed_query,dbms_sql.NATIVE);
		
		-- add the bind variables
		for b in 0..nvl(g_datasource_bind_values.count-1,-1) loop
			case g_datasource_bind_types(b)
				when 'number' then 
					dbms_sql.bind_variable(
						p_cursor,
						':b'||b,													
						to_number(g_datasource_bind_values(b))
					);
				when 'varchar2' then 
					dbms_sql.bind_variable(
						p_cursor,
						':b'||b,											
						g_datasource_bind_values(b)
					); 
				else 
					raise_application_error(-20053,'Invalid bind type detected when parsing query.');
			end case;
			
		end loop;
	end prepareCursor;
	
	procedure get_json(
		p_datasource_id 			IN 			gdatasources.id%type,
		tq 							IN			varchar2 default 'select *',
		tqx							IN			varchar2 default NULL
	) is
        
        -- datasource query
        v_query				gdatasources.sql_text%type;
        
		-- dynamic cursor info
        v_cursor NUMBER;						-- cursor id
        v_cursor_output NUMBER;					-- execute cursor output
        v_col_cnt PLS_INTEGER;					-- # of columns
        record_desc_table dbms_sql.desc_tab; 	-- description table
        
        -- to store output values of the query
        v_col_char 			VARCHAR2(32767);
        v_col_number 		NUMBER;
		v_col_date 			DATE;
		v_col_datetime 		TIMESTAMP;
		v_col_clob			CLOB;
        
		-- logic helper vars
		v_first boolean;
		
		-- buffer to avoid printing before testing column formats
		v_buffer			varchar2(32767) := '';
		v_test_format		varchar2(1000);
		
	BEGIN
		
		begin
			select sql_text 
			into v_query
			from gdatasources
			where id = p_datasource_id;
		exception
			when no_data_found then
				-- treat error!
				p('error, no datasource found');
				return;
		end;
		
		clean;
		
		-- set DataSource
		setDataSource(v_query);
		
		-- set the google query
		setQuery(tq);
		
		-- open cursor
        v_cursor := dbms_sql.open_cursor;
        
        -- prepare cursor (parse and bind)
		prepareCursor(v_cursor);
		
		-- execute the cursor
        v_cursor_output := dbms_sql.execute(v_cursor); 
		
		-- get columns of the cursor
        dbms_sql.describe_columns(v_cursor, v_col_cnt, record_desc_table);
		
		-- NOTE
		-- from now on, we should not have any errors!
		-- Parsing and execute was done, so we assume the rest will be ok
		-- If an error happens, then the resulting JSON will be invalid
		
	    for col in 1..v_col_cnt
	    loop if record_desc_table(col).col_name != 'RNUM' then 
	        
		    -- create column details
            v_buffer := v_buffer || '   {'	||
            			'id: "'||record_desc_table(col).col_name||'", ';
            
            if not g_opt_no_values then
            	if g_datasource_labels(col) is not null then
            		v_buffer := v_buffer || 'label: "'||g_datasource_labels(col)||'", ';
            	else
            		v_buffer := v_buffer || 'label: "'||record_desc_table(col).col_name||'", ';
            	end if;
            end if;
            
            if record_desc_table(col).col_type in (1,9,96,112) then
                -- varchar, varchar2, char and CLOB
                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_char, 32767);
                v_buffer := v_buffer || 'type: "string"';
            elsif record_desc_table(col).col_type = 2 then
                -- number
                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_number);
                v_buffer := v_buffer || 'type: "number"';
			elsif record_desc_table(col).col_type = 12 then
                -- date
                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_date);
                v_buffer := v_buffer || 'type: "date"';
            elsif record_desc_table(col).col_type = 187 then
                -- timestamp
                DBMS_SQL.DEFINE_COLUMN(v_cursor, col, v_col_datetime);
                v_buffer := v_buffer || 'type: "datetime"';
            else 
                raise_application_error(-20001,'Not expected datatype');
            END IF;
            
            -- test format! Only valid for number, date and timestamp, ignore the rest
			if record_desc_table(col).col_type in (2,12,187) and g_datasource_formats(col) is not null and not g_opt_no_format then
				declare
					v_type varchar2(20);
				begin
					if record_desc_table(col).col_type = 2 then
						v_type := 'Number';
               			v_test_format := to_char(1,g_datasource_formats(col));
               		elsif record_desc_table(col).col_type = 12 then
               			v_type := 'Date';
               			v_test_format := to_char(sysdate,g_datasource_formats(col));
               		elsif record_desc_table(col).col_type = 187 then
               			v_type := 'Datetime';
               			v_test_format := to_char(systimestamp,g_datasource_formats(col));
               		end if;
               	exception
               		when others then
               			raise_application_error(-20060,'Invalid format given for '||v_type||' column: '||g_datasource_formats.count||' '||g_datasource_formats(col));
               	end;
            end if;
            
            if col < v_col_cnt then
            	v_buffer := v_buffer || '},';
            else
            	v_buffer := v_buffer || '}';
            end if;
            
            v_buffer := v_buffer || chr(10);
	        
	    end if; end loop;
	    
	    -- return the json object with the proper response Handler given
		if not g_debug then
	    	owa_util.mime_header('text/x-json', FALSE, NULL);
			htp.p('Pragma: no-cache');
			htp.p('Expires: Thu, 01 Jan 1970 12:00:00 GMT');
			owa_util.http_header_close;
		end if;
		
		-- start the JSON object
        p(nvl(get_tqx_attr('responseHandler',tqx),'google.visualization.Query.setResponse')||'(');
		p('{');
		p(' version: "'||g_version||'",');
		p(' status: "ok",');
		p(' reqId: '||nvl(get_tqx_attr('reqId',tqx),0)||',');
		
		-- TODO: signature ??
		-- p(' signature: "928347923874923874",');
		
		-- start building the table
		p(' table: {');
		
		-- define cols
		p('  cols: [');
			
			p(v_buffer);
			
		p('  ],');
		
		-- rows!
		p('  rows: [');
		
		v_first := true;
		
		loop
			
			-- Fetch a row from the source table
	        exit when dbms_sql.fetch_rows(v_cursor) = 0;
	        
	        -- create row details
			
			-- Add the col and rows objects to the table json
			if v_first then
				p('   {c: [ ');
				v_first := false;
			else
				p('   ,{c: [ ');
			end if;
	        
	        for col in 1..v_col_cnt
	        loop if record_desc_table(col).col_name != 'RNUM' then 
            
				prn('    {');
                
            	if record_desc_table(col).col_type in (1,9,96) then
            		-- varchar, varchar2, char
                    dbms_sql.column_value(v_cursor, col, v_col_char);
                    if v_col_char is null then
                    	prn('v: null');
                    else
                    	prn('v: "');
                    	printJsonString(v_col_char);
                    	prn ('"');
                    end if;
            	elsif record_desc_table(col).col_type = 2 then
            		-- number
                    dbms_sql.column_value(v_cursor, col, v_col_number);
                    -- TODO: opt no_values
					if v_col_number is null then
						prn('v: null');
					else
	                    prn('v: '||to_char(v_col_number));
	                    if g_datasource_formats(col) is not null and not g_opt_no_format then
	                    	prn(', f: "'||to_char(v_col_number,g_datasource_formats(col))||'"');
	                    end if;
					end if;
				elsif record_desc_table(col).col_type = 12 then
                    dbms_sql.column_value(v_cursor, col, v_col_date);
                    if v_col_date is null then
                    	prn('v: null');
                    else
	                    prn('v: new Date('||                    
	    					nvl(trim(leading '0' from to_char(v_col_date,'yyyy')),'0')	||','||
	    					nvl(trim(leading '0' from to_char(v_col_date,'mm')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_date,'dd')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_date,'hh24')),'0')	||','||
	    					nvl(trim(leading '0' from to_char(v_col_date,'mi')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_date,'ss')),'0')		||')');
	                    if g_datasource_formats(col) is not null and not g_opt_no_format then
	                    	prn(', f: "'||to_char(v_col_date,g_datasource_formats(col))||'"');
	                    elsif not g_opt_no_format then
	                    	prn(', f: "'||to_char(v_col_date,'yyyy-mm-dd hh24:mi:ss')||'"');	
	                    end if;
	                end if;
            	elsif record_desc_table(col).col_type = 187 then
                    dbms_sql.column_value(v_cursor, col, v_col_datetime);
                    if v_col_datetime is null then
                    	prn('v: null');
                    else
	                    prn('v: new Date('||                    
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'yyyy')),'0')	||','||
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'mm')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'dd')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'hh24')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'mi')),'0')		||','||
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'ss')),'0')		||'.'||
	    					nvl(trim(leading '0' from to_char(v_col_datetime,'ff3')),'0')		||')');
	                    if g_datasource_formats(col) is not null and not g_opt_no_format then
	                    	prn(', f: "'||to_char(v_col_datetime,g_datasource_formats(col))||'"');
	                    elsif not g_opt_no_format then
	                    	prn(', f: "'||to_char(v_col_datetime,'yyyy-mm-dd hh24:mi:ss')||'"');	
	                    end if;
					end if;
				elsif record_desc_table(col).col_type = 112 then
            		-- CLOB
                    dbms_sql.column_value(v_cursor, col, v_col_clob);
                    if nvl(dbms_lob.GETLENGTH(v_col_clob),0) = 0 or v_col_clob is null then
                    	prn('v: null');
                    else
                    	prn('v: "');
                    	printJsonString(v_col_clob);
                    	prn ('"');
                    end if;
            	end if;
            	
            	if col < v_col_cnt then
	            	prn('},');
	            else
	            	prn('}');
	            end if;
	            
				nl;
				                
	        end if; end loop;
	        
	        p('   ]}');
	    
		end loop;
	    
	    p('  ]');
	    p(' }');
	    p('}');
	    
	    -- finish!
	    p(')');
	     
	    dbms_sql.close_cursor(v_cursor);
	    
	EXCEPTION
		WHEN OTHERS THEN
			declare
				v_errors t_varchar2;
				v_messages t_varchar2;
				v_detailed_messages t_varchar2;
			begin
				v_errors(1) := SQLCODE;
				v_messages(1) := SQLERRM;
				print_json_error(
					v_errors,
					v_messages,
					v_detailed_messages,
					tqx
				);
			end;
	
    END get_json;
    
	/**
	 * Send errors
	 *
	 */
	procedure print_json_error(
		p_reasons 			IN t_varchar2,
		p_messages 			IN t_varchar2,
		p_detailed_messages IN t_varchar2,
		tqx VARCHAR2 DEFAULT NULL
	) is
	begin
		
		-- return the json object with the proper response Handler given
		if not g_debug then
			owa_util.mime_header('text/x-json', FALSE, NULL);
			p('Pragma: no-cache');
			p('Expires: Thu, 01 Jan 1970 12:00:00 GMT');
			owa_util.http_header_close;
		end if;
		
		
		p(nvl(get_tqx_attr('responseHandler',tqx),'google.visualization.Query.setResponse')||'(');
				
		p('{');
		p(' version: "'||g_version||'",');
		p(' status: "error",');
		p(' reqId: '||nvl(get_tqx_attr('reqId',tqx),0)||',');
		-- signature ??
		-- p(' signature: "928347923874923874",');
		
		p(' errors: [');
		
		if nvl(p_reasons.COUNT,0) = 0 then
		
			p('   {reason: "Undefined error in GDataSource package"}');	
			
		else
		
			for e in 1..p_reasons.COUNT loop
				if e > 1 then
				    p(',');
					prn('   {');
				else
					prn('   {');
				end if;
				prn('reason: "');
					printJsonString(p_reasons(e));
				prn('"');
				if p_messages.exists(e) and nvl(length(p_messages(e)),0) > 0 then
					prn(', message: "');
						printJsonString(p_messages(e));
					prn('"');
				end if;
				if p_detailed_messages.exists(e) and nvl(length(p_detailed_messages(e)),0) > 0 then
					prn(', detailed_message: "');
						printJsonString(p_detailed_messages(e));
					prn('"');
				end if;
				p('}');		
			end loop;
		end if;
		
		p(' ]');
	    p('}');
	    
	    -- finish!
	    p(')');		
		
	end print_json_error;
	
	/**
	*  
	*  Wrapper functions needed for Google Query functions
	*
	*/
	
	function toDate(p_date	IN	timestamp) return date is
	begin
		return to_date(to_char(p_date,'yyyy-mm-dd'),'yyyy-mm-dd');
	end toDate;
	function toDate(p_date	IN	number) return date is
	begin
		return to_date('1970-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')+(p_date/1000/60/60/24);
	end toDate;
	
END GDataSource;
/
