CREATE OR REPLACE PACKAGE GDataSource
	/**
	 * 
	 * Oracle PL/SQL Implementation for Google Data Source objects 
	 * 
	 * Some support to the Query Language included 
	 * http://code.google.com/apis/visualization/documentation/querylanguage.html#Clauses
	 *
	 * Dependencies: gdatasources table:
	 * 	create table gdatasources ( 
	 * 		id varchar2(100) not null primary key, 
	 * 		sql_text varchar2(4000)
	 *	);
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
	
	-- types
	type t_varchar2 is table of varchar2(32767) index by binary_integer;
	
	-- global variables
	g_debug						              boolean := false;
  g_clob_output                   CLOB; -- only used WHEN g_opt_clob_output = false AND g_debug = false
	
  -- package variables
  g_clob_initialized              boolean := false;
	g_google_query 				          varchar2(32767);	-- the query as send by the user (set by setQuery proc)
	-- the datasource query (needs to be already defined, it's set by setDataSource proc)
	g_datasource_select_clause	    varchar2(32767);	-- the select part
	g_datasource_rest_of_clause	    varchar2(32767);	-- the rest part of the query
	g_datasource_columns		        t_varchar2;			-- datasource columns need to parse valid columns in the google query
	g_datasource_columns_full	      t_varchar2;			-- datasource columns full text to add to the query
	g_datasource_needed_columns	    t_varchar2;			-- datasource needed columns to satisfy the google query
	g_datasource_bind_values	      t_varchar2;			-- bind values collected during parsing
	g_datasource_bind_types		      t_varchar2;			-- bind types collected during parsing
	g_datasource_labels			        t_varchar2;			-- labels, if any, to print to the client
	g_datasource_formats		        t_varchar2;			-- formats, if any, to print to the client
	g_parsed_query				          varchar2(32767);	-- the final query to be executed
	-- options
	g_opt_no_format				          boolean := false;
	g_opt_no_values				          boolean := false;
  g_opt_clob_output               boolean := false;
	-- version
	g_version					              varchar2(3) := '0.7';
	
	-- 	Parse a server database query string
	procedure setDataSource(
		p_datasource_query 			IN 			varchar2
	);
	
	-- parse a client's google datasource query string
	procedure setQuery(
		p_google_query 				  IN 			varchar2
	);
	
	-- build, parse and bind query based on datasource query and datasource cursor
	procedure prepareCursor(
		p_cursor 					      IN OUT		NUMBER
	);
	
	-- once query is ready, you can print the JSON with the results
	procedure get_json(
		p_datasource_id 			  IN 			gdatasources.id%type,
		tq 							        IN			varchar2 default 'select *',
		tqx							        IN			varchar2 default NULL
	);
  function get_json(
    p_datasource_id 			  IN 			gdatasources.id%type,
		tq 							        IN			varchar2 default 'select *',
		tqx							        IN			varchar2 default NULL
  ) return sys.KU$_VCNT pipelined;
	
	-- print json errors
	procedure print_json_error(
		p_reasons 			        IN t_varchar2,
		p_messages 			        IN t_varchar2,
		p_detailed_messages     IN t_varchar2,
		tqx                     VARCHAR2 DEFAULT NULL
	);
	
	/**
	 * Wrapper functions needed for simulating Google Query functions
	 */
	 
	function toDate(p_date	  IN	timestamp)  return date;
	function toDate(p_date	  IN	number)     return date;
	
END GDataSource;
/
