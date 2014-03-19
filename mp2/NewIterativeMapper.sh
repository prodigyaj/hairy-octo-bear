#/bin/bash
echo $1'\t'$1 > List
compare=">$2$"
for i in `seq 1 4`
do
~/hadoop-1.2.1/bin/hadoop fs -rmr List
~/hadoop-1.2.1/bin/hadoop fs -rmr ListOutput
~/hadoop-1.2.1/bin/hadoop fs -put List List
echo $i
~/hadoop-1.2.1/bin/hadoop jar ~/mp2/hairy-octo-bear/mp2/filesp.jar List ListOutput
rm -fr ListOutput/ 2/dev/null
~/hadoop-1.2.1/bin/hadoop fs -get ListOutput .
cp ListOutput/part* .
rm -f List 2>/dev/null
mv part* List
if  grep --quiet $compare List
then 
	grep $compare List
	break
fi
done
