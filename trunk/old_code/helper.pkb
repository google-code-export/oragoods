	CREATE OR REPLACE PACKAGE BODY helper AS
    /**
     * @param p_cursor
     * @param p_delimiter
     * @return VARCHAR2
     * 
     * USAGE:
     * =====
     * SELECT IMPLODE( CURSOR( SELECT ENAME FROM EMP ) )
     * FROM DUAL;
     * 
     **/
    FUNCTION IMPLODE (
      p_cursor SYS_REFCURSOR,
      p_delimiter VARCHAR2 DEFAULT ','
    )
    RETURN VARCHAR2
    AS
      v_token  VARCHAR2(32767);
      v_output VARCHAR2(32767);
    BEGIN
      LOOP
        FETCH p_cursor INTO v_token;
        EXIT WHEN p_cursor%NOTFOUND;
        IF v_output IS NOT NULL THEN
          v_output := v_output || p_delimiter;
        END IF;
        v_output := v_output || v_token;
      END LOOP;
      RETURN v_output;
    END IMPLODE;
    
    
    /**
     * @param p_string
     * @param p_delimiter
     * @return T_VARCHAR2
     * 
     * USAGE
     * =====
     * SELECT  ENAME 
     * FROM EMP 
     * WHERE TO_CHAR(HIREDATE, 'YY') IN (
     * 		SELECT COLUMN_VALUE FROM TABLE(EXPLODE('81,82')
     * );
     * 
     */
    FUNCTION EXPLODE (
      p_string    						IN VARCHAR2,
      p_delimiter 						IN VARCHAR2
    ) RETURN T_VARCHAR2 PIPELINED
    AS
      v_length    pls_integer;
      v_token     VARCHAR2(32767);
    BEGIN
      v_token := p_string;
      LOOP
	  	v_length := INSTR(v_token, p_delimiter);
        IF v_length = 0 OR v_length IS NULL THEN
          PIPE ROW(v_token);
          EXIT;
        END IF;
        PIPE ROW(SUBSTR(v_token, 1, v_length - 1));
        v_token := SUBSTR(v_token, v_length + 1);
      END LOOP;
      RETURN;
    END EXPLODE;
    
    PROCEDURE EXPLODE (
      p_string    						IN VARCHAR2,
      p_delimiter 						IN VARCHAR2,
      p_table							OUT NOCOPY T_VARCHAR2,
      p_trim							IN BOOLEAN DEFAULT FALSE
    )
    AS
      v_length    pls_integer;
      v_token     VARCHAR2(32767);
      i			  pls_integer;
    BEGIN
      v_token := p_string;
      i:=1;
      p_table := t_varchar2();
      LOOP
        p_table.extend;
        v_length := INSTR(v_token, p_delimiter);
        IF v_length = 0 OR v_length IS NULL THEN
        	if p_trim then
          		p_table(i) := trim(v_token);
          	else
          		p_table(i) := v_token;
          	end if;
          EXIT;
        END IF;
        if p_trim then
        	p_table(i) := trim(SUBSTR(v_token, 1, v_length - 1));
        else
        	p_table(i) := SUBSTR(v_token, 1, v_length - 1);
        end if;
        v_token := SUBSTR(v_token, v_length + 1);
        i := i + 1;
      END LOOP;
      RETURN;
    END EXPLODE;
	
	/**
	 * explode_select_clause will extract select columns
	 * 
	 * @param p_select_clause varchar2
	 * @param p_table t_varchar2 return object
	 * 
	 */
	PROCEDURE EXPLODE_SELECT_CLAUSE (
		p_select_clause		IN VARCHAR2,
		p_table				OUT NOCOPY T_VARCHAR2,
		p_trim				IN BOOLEAN DEFAULT FALSE
	)
	AS
		v_parenthesis_count pls_integer;
		v_current char(1 char);
		v_last varchar2(32767);
	BEGIN
		v_parenthesis_count := 0;
		v_last := '';
		p_table := T_VARCHAR2();
		
		for i in 1..length(p_select_clause) 
		loop
			v_current := substr(p_select_clause,i,1);
			if v_current = ',' then
				if v_parenthesis_count > 0 then
					-- ignore ','
					v_last := v_last || v_current;
				else
					 p_table.extend;
					 if p_trim then
						 p_table(p_table.count) := trim(v_last);
					 else
					 	 p_table(p_table.count) := v_last;
					 end if;
					 v_last := '';
					 v_parenthesis_count := 0;
				end if;
			elsif v_current = '(' then
				v_parenthesis_count := v_parenthesis_count + 1;
				v_last := v_last || v_current;
			elsif v_current = ')' then
				v_parenthesis_count := v_parenthesis_count - 1;
				v_last := v_last || v_current;
			else
				v_last := v_last || v_current;
			end if;			
		end loop;
		if length(v_last) > 0 then
			 p_table.extend;
			 if p_trim then
				 p_table(p_table.count) := trim(v_last);
			 else
			 	 p_table(p_table.count) := v_last;
			 end if;
		end if;
	END EXPLODE_SELECT_CLAUSE;
    
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
     * 		helper.get_tqx_attr('reqId','version:0.5;reqId:1;sig:5277771;out:json;responseHandler:myQueryHandler');
     *    would return 
     * 		1
     * 
     */
    FUNCTION get_tqx_attr(
        p_attr IN VARCHAR2,
        tqx IN VARCHAR2)
    RETURN VARCHAR2
    IS
        v_found BOOLEAN;
    BEGIN
        
        FOR i IN (SELECT COLUMN_VALUE AS data FROM TABLE(helper.explode(tqx,';')))
        LOOP
            v_found := FALSE;
            
            FOR j IN (SELECT ROWNUM AS cnt, COLUMN_VALUE AS data FROM TABLE(helper.explode(replace(i.data,'\:',chr(13)),':')))
            LOOP
                IF j.cnt = 1 AND j.data = p_attr THEN
                    v_found := TRUE;
                END IF;
                    
                IF j.cnt = 2 AND v_found = TRUE THEN
                    RETURN replace(j.data,chr(13),':');
                END IF;
                    
            END LOOP;
        END LOOP;
        
        -- nothing found
        RETURN NULL;
    END;
	
	function isStringWithQuotes(
		p_string IN varchar2)
	return boolean
	is
	begin
		if 	substr(p_string,1,1) = '''' and substr(p_string,length(p_string),1) = ''''
		then
			-- validate contents
			for i in 2..length(p_string)-1 loop
				if instr('abcdefghijklmn�opqrstuvwxyz0123456789-_|!"#$%&/()=����^`;,:.~*+<>@',
						 substr(lower(p_string),i,1)) = 0 
				then
					raise_application_error(-20002,'String between quotes contains invalid characters.');					
				end if;				
			end loop;
			return true;
		else
			return false;
		end if;
	end isStringWithQuotes;
	
	function stripQuotes(p_string IN varchar2)
	return varchar2
	is
	begin
		if isStringWithQuotes(p_string) then
			return substr(p_string,2,length(p_string)-2);
		else
			raise_application_error(-20003,'String does not look quoted.');
		end if;		
	end stripQuotes;
	
	function isStringWithDoubleQuotes(
		p_string IN varchar2)
	return boolean
	is
	begin
		if 	substr(p_string,1,1) = '"' and substr(p_string,length(p_string),1) = '"'
		then
			-- validate contents
			for i in 2..length(p_string)-1 loop
				if instr(' abcdefghijklmn�opqrstuvwxyz0123456789-_|!#$%&/()=����^`;,:.~*+<>@''',
						 substr(lower(p_string),i,1)) = 0 
				then
					raise_application_error(-20004,'String between double quotes contains invalid characters.');					
				end if;				
			end loop;
			return true;
		else
			return false;
		end if;
	end isStringWithDoubleQuotes;
	
	function isNumeric(p_string IN varchar2)
	return boolean
	is
	begin
		if 	LENGTH(TRIM(TRANSLATE(p_string,' +-.0123456789',' '))) is null
			and instr(p_string,'+',1,2) = 0
			and instr(p_string,'-',1,2) = 0
			and instr(p_string,'.',1,2) = 0
			and instr(p_string,' ') = 0
		then
			return true;
		else
			return false;
		end if;
	end isNumeric;		
	
	function isValidColumnName(
		p_string in varchar2, 
		force_uppercase IN boolean default false,
		allow_schema_prefix	IN boolean default false)
	return boolean
	is
		v_string varchar2(32767) := '';
		v_chars varchar2(100) := 'abcdefghijklmn�opqrstuvwxyz';
		v_numbers varchar2(10) := '0123456789';
		v_special varchar2(3) := '$_#';
		v_all varchar2(113) := ''; 
	begin
		
		v_string := p_string;
		
		if v_string is null or length(v_string) = 0 then
			return false;
		end if;
		
		-- does it has a schema prefix?
		if allow_schema_prefix then
			
			if instr(v_string,'.') > 0 then
				
				for i in (select column_value as name, rownum as id 
							from table(helper.explode(v_string,'.')))
				loop
					if i.id >= 3 then
						return false;						
					elsif i.id = 1 then
						if not isValidColumnName(i.name,force_uppercase,false) then
							return false;
						else
							v_string := i.name;
						end if;
					end if;									
				end loop;
				
			end if;
		end if;
		
		if force_uppercase then
			v_all := upper(v_chars);
		else
			v_all := v_chars||upper(v_chars);
		end if;
		
		-- first character needs to be a letter
		if instr(v_all,substr(v_string,1,1)) = 0 then
			return false;
		end if;		
		
		v_all := v_all||v_numbers||v_special;
		
		for i in 2..length(v_string) 
		loop
			
			if instr(v_all,substr(v_string,i,1)) = 0 
			then
				return false;					
			end if;		
						
		end loop;
		
		return true;
		
	end isValidColumnName;
	
	procedure htp_clob_print (
		v_return IN CLOB
	) is
		v_count binary_integer;
		v_read binary_integer;
		v_text_buffer varchar2(2000);
	begin
   		v_count := 1;
   		v_read := 2000;
		loop
			DBMS_LOB.READ (
            	v_return, v_read , v_count, v_text_buffer
			);
			htp.PRN(v_text_buffer);
			v_count := v_count  + v_read;
   		end loop;
	exception
   		WHEN NO_DATA_FOUND THEN
			return;
	end htp_clob_print;
	
	
	/**
	* Print procedures!
	*/
	procedure p(print in varchar2)
	is
	begin
		if output = 'html' then
			htp.p(print);
		else
			dbms_output.put_line(print);		
		end if;
	end p;
	
	procedure prn(print in varchar2)
	is
	begin
		if output = 'html' then
			htp.prn(print);
		else
			dbms_output.put(print);		
		end if;
	end prn;
	
	procedure nl
	is
	begin
		if output = 'html' then
			htp.prn(chr(10));
		else
			dbms_output.new_line;		
		end if;
	end nl;

END helper;
/

