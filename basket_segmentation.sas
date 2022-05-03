cas mySession sessopts=(caslib=CASUSER timeout=1800 locale="en_US"); 
caslib _all_ assign;

/* GET ORDER DATA FROM DB(LAST 1 YEAR)*/
proc sql noerrorstop;
connect to postgres as pg_conn
(server=*** port=*** user=***
password=*** database=***);

create table casuser.ORDER_DATA as
SELECT CUSTOMER_ID length 15,PRODUCT length 25,ORDER_NO length 15, 
	 AREA length 10,
	 ORDER_DATE, ORDER_QUANTITY FROM CONNECTION TO pg_conn (
 
			select t1.*,t2."AREA"	
			FROM "DB"."ORDER_DATA" as t1
			LEFT JOIN "DB"."CUSTOMER_DATA" as t2
			ON t1."CUSTOMER_ID" = t2."CUSTOMER_ID"
			where "ORDER_DATE" >= (NOW() - interval '365 day') and "ORDER_DATE" <= NOW()
);
disconnect from pg_conn;
quit;

/* ADD FILTER AS AREA='DIREKT' */
data casuser.ORDER_DATA;
	set casuser.ORDER_DATA;
	where AREA = 'DİREKT';
run;

/* FIND CUSTOMER'S NUMBER OF UNIQUE ITEMS ACCORDING TO THE CART */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.ORDER_DATA2 {options replace=True} AS
    select CUSTOMER_ID, ORDER_NO, COUNT(DISTINCT PRODUCT) AS PRODUCT_VARIETY
    from casuser.ORDER_DATA
    group by CUSTOMER_ID, ORDER_NO;
quit;


/* FIND CUSTOMER'S TOTAL UNIQUE PRODUCTS */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.TOTAL_UNQ_PROD {options replace=True} AS
    select CUSTOMER_ID, COUNT(DISTINCT PRODUCT) AS TOTAL_PRODUCT
    from casuser.ORDER_DATA
    group by CUSTOMER_ID;
quit;


/* ASSIGN LABEL TO CUSTOMERS THAT GET ONLY ONE TYPE PRODUCT */
data casuser.TOTAL_UNQ_PROD;
	set casuser.TOTAL_UNQ_PROD;
	where TOTAL_PRODUCT = 1;
	CLUSTER1 = 'ONE TYPE';
run;


/* JOIN ALL CUSTOMERS WITH ONE TYPE CUSTOMERS */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.ORDER_DATA2 {options replace=True} AS
	SELECT T1.CUSTOMER_ID,PRODUCT_VARIETY,TOTAL_PRODUCT, CLUSTER1
	FROM casuser.ORDER_DATA2 AS T1
	LEFT JOIN casuser.TOTAL_UNQ_PROD AS T2
	ON T1.CUSTOMER_ID = T2.CUSTOMER_ID;
quit;


/* FIND THE AVERAGE BASKET VALUE OF CUSTOMERS, EXCEPT ONE TYPE CUSTOMERS */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.ORDER_DATA3 {options replace=True} AS
    select CUSTOMER_ID, MEAN(PRODUCT_VARIETY) AS BASKET_AVG
    from casuser.ORDER_DATA2
	where CLUSTER1 ^= 'ONE TYPE'
    group by CUSTOMER_ID;

quit;


/* GET LOG AVERAGE BASKET VALUE */
data casuser.ORDER_DATA3;
	set casuser.ORDER_DATA3;
	LOG_BASKET_AVG = LOG(BASKET_AVG);
run;


/* CLUSTERS FOR CUSTOMERS EXCEPT ONE TYPE CUSTOMERS*/
ods noproctitle;

proc stdize data=CASUSER.ORDER_DATA3 out=casuser._std_ method=range;
	var LOG_BASKET_AVG;
run;

proc fastclus data=casuser._std_ maxclusters=4 out=casuser.fastclus_scores outseed=casuser.Fastclus_seeds;
	var LOG_BASKET_AVG;
run;

proc delete data=casUser._std_;
run;

data work.Fastclus_seeds;
 set casuser.Fastclus_seeds;
run;


/* SCATTER PLOT OF CLUSTERS */
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=CASUSER.fastclus_scores;
  	styleattrs datacontrastcolors=(CXACB0FF CX98DDCA CXD5ECC2 CXF6C6EA);
	title height=14pt "CLUSTERS BY AVG BASKET";
	scatter x=LOG_BASKET_AVG y=CLUSTER / group=CLUSTER;
	xaxis grid;
	yaxis grid;
	keylegend / location=inside;
run;

ods graphics / reset;
title;

/* SORT MEAN OF CLUSTERS */
PROC SQL;
	create table work.cluster_sorted AS
	select * from work.Fastclus_seeds
	order by LOG_BASKET_AVG desc;
quit;


/* ASSIGN LABEL TO CLUSTER MEANS ON DESCENDING ORDER */
PROC SQL;
%let i=0;

    CREATE TABLE work.CLUSTER_ORDER   AS             
    SELECT CLUSTER,  LOG_BASKET_AVG ,
     input(resolve('%let i=%eval(&i+1);&i'),32.) as ROW_NUMBER
    FROM work.cluster_sorted ;

	/* ASSIGN SEGMENT NAME TO CLUSTERS AFTER SORTING*/
    CREATE TABLE work.cluster_labeled   AS            
    SELECT  CLUSTER,  ROW_NUMBER,
      (CASE WHEN ROW_NUMBER = 1 THEN 'LOVES VARIETY'
            WHEN ROW_NUMBER = 2 THEN 'LIKE VARIETY'
            WHEN ROW_NUMBER = 3 THEN 'DOES NOT LIKE VARIETY'
            WHEN ROW_NUMBER = 4 THEN 'HATE VARIETY'             
       END)  AS SEGMENT_LABEL
    FROM work.CLUSTER_ORDER  ;  


data casuser.cluster_labeled;
	set work.cluster_labeled;
run;

/* JOIN CLUSTERED CUSTOMERS EXCEPT ONE TYPE CUSTOMERS */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.ORDER_DATA4 {options replace=True} AS
	SELECT CUSTOMER_ID,t1.CLUSTER, SEGMENT_LABEL
	FROM casuser.fastclus_scores as t1
	LEFT JOIN casuser.cluster_labeled as t2
	ON t1.CLUSTER = t2.CLUSTER;
quit;



/* GET CUSTOMER ID OF ALL CUSTOMERS */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.ORDER_DATA2 {options replace=True} AS
    select CUSTOMER_ID
    from casuser.ORDER_DATA2
    group by CUSTOMER_ID;
quit;


/* JOIN ALL CUSTOMERS AND SEGMENTS OF NOT ONE TYPE CUSTOMERS */
PROC FEDSQL SESSREF = mySession;
    CREATE TABLE casuser.segment {options replace=True} AS
	SELECT siparis2.CUSTOMER_ID, SEGMENT_LABEL AS SEGMENT
	FROM casuser.ORDER_DATA2 as t1
	LEFT JOIN casuser.ORDER_DATA4 as t2
	ON t1.CUSTOMER_ID = t2.CUSTOMER_ID;
quit;

/* ASSIGN 'ONE TYPE' LABEL TO IF SEGMENT FIELD IS EMPTY */
data casuser.segment;
	set casuser.segment;
	if SEGMENT=" " then SEGMENT="ONE TYPE";
	keep CUSTOMER_ID SEGMENT;

run;

/* PIE CHART */
proc template;
	define statgraph SASStudio.Pie;
		begingraph;
		layout region;
		piechart category=SEGMENT /;
		endlayout;
		endgraph;
	end;
run;

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgrender template=SASStudio.Pie data=CASUSER.SEGMENT;
run;

ods graphics / reset;


/* BAR CHART */
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=CASUSER.SEGMENT;
	vbar SEGMENT /;
	yaxis grid;
run;
ods graphics / reset;

/**** WRITE THE RESULT TABLE TO DB ***/

LIBNAME psqlsvr postgres schema=*** server=*** database=*** 
user=*** password=*** insertbuff=32767 preserve_tab_names=yes;

PROC SQL;
  Drop table psqlsvr.SEGMENT;
  Create Table psqlsvr.SEGMENT (BULKLOAD = YES,PRESERVE_COL_NAMES = Yes) as 
  SELECT * from CASUSER.SEGMENT;
QUIT;







