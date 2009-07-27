create table gdatasources ( 
	id varchar2(100) not null primary key,
	sql_text varchar2(4000)
);

insert into gdatasources values ('test','select ''this'' as col1, ''is'' as col2, ''test'' as col3 from dual');

commit;
