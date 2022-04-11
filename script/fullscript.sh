#!/bin/bash

. /home/saif/LFS/cohort_c9/PROJECTS/env/project.env

##################################################################
echo "\n\ndata loading in mysql starting in 5 sec\n\n"
sleep 5
mysql -u $user -p"$password" -e "truncate table $db.$maintable;"

mysql -u $user -p"$password" -e "SET GLOBAL local_infile=1;"
mysql --local-infile=1 -u $user -p"$password" -e "load data local infile '$datafile' into table $db.$maintable fields terminated by ',' lines terminated by '\n'; update $db.$maintable set entry_time=replace(entry_time,' -0500',''); update $db.$maintable set entry_date= str_to_date(entry_time,'%d/%M/%Y:%H:%i:%s'),Year = EXTRACT(YEAR from str_to_date(entry_time,'%d/%M/%Y:%H:%i:%s')),Month = EXTRACT(MONTH from str_to_date(entry_time,'%d/%M/%Y:%H:%i:%s')),Day = EXTRACT(DAY from str_to_date(entry_time,'%d/%M/%Y:%H:%i:%s')); update recon.customer_data set last_update=sysdate();"

######################################################################
echo "\n\ndata loading in mysql backup table starting in 5 sec\n\n"
sleep 5

mysql -u $user -p"$password" -e "INSERT INTO $db.$backup_tbl SELECT * FROM $db.$maintable;"

##################################################################
echo "\n\nsqoop job execution starting in 5 sec\n\n"
sleep 5

sqoop job --exec $sqoopjob_name

#################################################################

echo "\n\ndata loading in hive managed table starting in 5 sec\n\n"
sleep 5


hive -e "load data inpath '/user/saif/HFS/Input/ln_poc' into table $hive_db.$hive_tbl"

echo "\n\ndata loading hive external table starting in 5 sec\n\n"
sleep 5

#######################################################################

hive -e "alter table recon.ext_customer_data set tblproperties('EXTERNAL'='false');
truncate table recon.ext_customer_data;
alter table recon.ext_customer_data set tblproperties('EXTERNAL'='True');"


hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions = 1000;
set hive.exec.max.dynamic.partitions.pernode = 1000;
INSERT OVERWRITE TABLE $hive_db.$hive_ext_tbl partition(Year, Month, Day) SELECT CUSTID, USERNAME, QUOTE_COUNT, IP, ENTRY_TIME, PRP_1, PRP_2, PRP_3, MS, HTTP_TYPE,PURCHASE_CATEGORY, TOTAL_COUNT,  PURCHASE_SUB_CATEGORY, HTTP_INFO, STATUS_CODE ,entry_date,last_update ,Year, Month, Day FROM $hive_db.$hive_tbl where custid not in ( select custid from $hive_db.$hive_tbl where last_update=(select max(last_update) from $hive_db.$hive_tbl ));  INSERT INTO TABLE $hive_db.$hive_ext_tbl partition(Year, Month, Day) select * from $hive_db.$hive_tbl where last_update=(select max(last_update) from $hive_db.$hive_tbl );"

