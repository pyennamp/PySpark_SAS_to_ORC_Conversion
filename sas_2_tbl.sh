
#This script calls two pyspark scripts which converts sas7bdat to orc format

proj_loc=/mapr/datalake/data_sets/

for i in `ls $proj_loc`;
do
count=0
for j in `ls $proj_loc/$i | egrep '.sas7bdat'`;
do
count=$(($count + 1))
if [ $count == 1 ]; then
echo "Creating table for $i" >> sas_2_tbl_orc.log
tbl_nm=adr.$i
fl_nm=`echo $proj_loc/$i/$j | sed -e 's|/mapr||g'`
/opt/mapr/spark/spark-2.1.0/bin/spark-submit  --num-executors 10 --executor-cores 10  --executor-memory 8G --driver-memory 2G  --packages saurfang:spark-sas7bdat:2.0.0-s_2.10  sas_2_tbl_crt.py $fl_nm $tbl_nm
echo "Table created for $i" >> sas_2_tbl_orc.log
else
echo "Appending data for $i" >> sas_2_tbl_orc.log
tbl_nm=adr.$i
fl_nm=`echo $proj_loc/$i/$j | sed -e 's|/mapr||g'`
/opt/mapr/spark/spark-2.1.0/bin/spark-submit  --num-executors 10 --executor-cores 10  --executor-memory 8G --driver-memory 2G  --packages saurfang:spark-sas7bdat:2.0.0-s_2.10  sas_2_tbl_append.py $fl_nm $tbl_nm
echo "Appended data for $i" >> sas_2_tbl_orc.log
fi
done 
done

mailx -s "Converted SAS to ORC" -r sas_orc_noreply@poc.com prashanth@poc.com 