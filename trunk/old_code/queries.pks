CREATE OR REPLACE PACKAGE queries
AS
	-- returns configuration as a google query table result
	PROCEDURE get_json_config (
		tq VARCHAR2 DEFAULT NULL,
        tqx VARCHAR2 DEFAULT NULL,
		tqcustom VARCHAR2 DEFAULT NULL
	);
	
	-- to be called from the web
	PROCEDURE get_json (
            p_datasource_id VARCHAR2,
            tq VARCHAR2 DEFAULT NULL,
            tqx VARCHAR2 DEFAULT NULL,
			tqcustom VARCHAR2 DEFAULT NULL); 
	
	PROCEDURE find_datasource(
        p_datasource_id IN VARCHAR2, -- query identifier
        p_query_description OUT NOCOPY varchar2,
        p_query OUT NOCOPY varchar2
	);
	
	-- json error object table for send_json_error proc
	TYPE json_error_obj_table IS TABLE OF json_error_obj INDEX BY BINARY_INTEGER;
	
	-- send json errors back to the browser
	procedure send_json_error(
		p_errors in json_error_obj_table,
		tqx VARCHAR2 DEFAULT NULL,
		tqcustom VARCHAR2 DEFAULT NULL
	);

END queries;
/ 
