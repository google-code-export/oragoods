CREATE OR REPLACE PACKAGE helper
AS
	
	-- table of varchar for implode/explode functions 
 	TYPE T_VARCHAR2 IS TABLE OF VARCHAR2(32767);
	
	FUNCTION IMPLODE (
		p_cursor SYS_REFCURSOR,
		p_delimiter VARCHAR2 DEFAULT ','
	) RETURN VARCHAR2;
	
	FUNCTION EXPLODE (
		p_string    						IN VARCHAR2,
      	p_delimiter 						IN VARCHAR2
	) RETURN T_VARCHAR2 PIPELINED;
	
	PROCEDURE EXPLODE (
      p_string    						IN VARCHAR2,
      p_delimiter 						IN VARCHAR2,
      p_table							OUT NOCOPY T_VARCHAR2,
      p_trim							IN BOOLEAN DEFAULT FALSE
    );
	
	PROCEDURE EXPLODE_SELECT_CLAUSE (
		p_select_clause						IN VARCHAR2,
		p_table								OUT NOCOPY T_VARCHAR2,
		p_trim								IN BOOLEAN DEFAULT FALSE
	);
	
	-- parse google data source uri for attributes
 	FUNCTION get_tqx_attr(
    	p_attr IN VARCHAR2,
		tqx IN VARCHAR2
	) RETURN VARCHAR2;
	
	-- find if a string with quotes is present (literal value)
	function isStringWithQuotes(p_string IN varchar2) return boolean;
	
	function stripQuotes(p_string IN varchar2) return varchar2;
	
	-- find if a string with double quotes is present (column alias)
	function isStringWithDoubleQuotes(p_string IN varchar2) return boolean;
	
	-- find if a string is a number
	function isNumeric(p_string IN varchar2) return boolean;
	
	-- validate string to see if is a valid column name
	function isValidColumnName(
		p_string in varchar2, 
		force_uppercase IN boolean default false,
		allow_schema_prefix	IN boolean default false
	) return boolean;
	
	-- print (htp.p) a clob in chunks of 2000 chars
	procedure htp_clob_print (
		v_return IN CLOB
	);
	
	output varchar2(100);
	procedure p(print in varchar2);
	procedure prn(print in varchar2);
	procedure nl;

END helper;
/ 
