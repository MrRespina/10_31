CREATE TABLE seoul_air(
	air_num number(3) primary key,
	air_MSRDT date not null,
	air_MSRRGN_NM varchar2(10 char) not null,
	air_MSRSTE_NM varchar2(10 char) not null,
	air_PM10 number(4) not null,
	air_PM25 number(4) not null,
	air_O3 number(5,3) not null,
	air_IDEX_NM varchar2(3 char) not null
);

CREATE SEQUENCE seoul_air_seq;
DROP SEQUENCE seoul_air_seq;

DROP TABLE seoul_air cascade constraint purge;

INSERT INTO SEOUL_AIR VALUES(seoul_air_seq.nextval,to_date('202310311200','YYYYMMDDHH24MI'),'도심권','강서구',66,75,0.026,'나쁨');

SELECT * FROM seoul_air;

TRUNCATE TABLE seoul_air;