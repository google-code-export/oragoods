CREATE OR REPLACE PACKAGE GDataSource
	/**
	 * 
	 * Oracle PL/SQL Implementation for Google Data Source objects 
	 * 
	 * Some support to the Query Language included 
	 * http://code.google.com/apis/visualization/documentation/querylanguage.html#Clauses
	 * 	
	 */
AS 
	
	g_google_query 				varchar2(32767);
	g_datasource_query			varchar2(32767);
	g_needed_datasource_cols 	gdatasource_rows_obj_table;
	g_query_cols 				gdatasource_rows_obj_table;
	g_select_cols 				gdatasource_select_obj_table;
	g_where_cols 				gdatasource_where_obj_table;
	g_groupby_cols 				gdatasource_groupby_obj_table;
	g_orderby_cols 				gdatasource_orderby_obj_table;
	g_limit						integer;
	g_offset 					integer;
	g_labels 					gdatasource_rows_obj_table;
	
	g_init						boolean := false;
	
	-- Initialize package variables
	PROCEDURE INIT;
	
	-- 	Parse a server database query string
	procedure parse (
		p_datasource_query 			IN 			varchar2
	);
	
	-- parse a client's google datasource query string
	procedure filter (
		p_query_string 				IN 			varchar2
	);
	
	-- build, parse and bind query based on datasource query and datasource cursor
	procedure prepareCursor(
		p_cursor 					IN 			NUMBER
	);
	
	/**
	 * Helper functions now
	 */
	 
	-- validate that query columns are indeed part of the datasource
	procedure validateQueryColumns;
	
	-- find alias (col AS "alias")
	procedure findAlias(
		p_string 					IN 			varchar2, 
		p_col 						OUT NOCOPY 	varchar2, 
		p_alias 					OUT NOCOPY 	varchar2
	);
	
	-- find function ("func(col)")
	procedure findFunction(
		p_col 						in 			varchar2,
		p_function_col 				out nocopy 	varchar2,
		p_function_col_type 		out nocopy 	varchar2,
		p_function 					out nocopy 	varchar2,
		allow_schema_prefix			IN			boolean 	default false 
	);

end GDataSource;
/ 
