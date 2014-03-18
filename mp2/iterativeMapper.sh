#/bin/bash
for i in `seq 1 3`
do
echo $i
~/hadoop-1.2.1/bin/hadoop jar ~/mp2/hairy-octo-bear/mp2/shortestPath.jar NodeList NodeOutput
~/hadoop-1.2.1/bin/hadoop -get NodeOutput .
cp NodeList/part* .
rm -f NodeList
mv part* NodeList
if  grep --quiet "[enter dest here]$" NodeList
then 
	grep "[enter dest here]$" NodeList
	break
fi
~/hadoop-1.2.1/bin/hadoop -rmr NodeList
~/hadoop-1.2.1/bin/hadoop -rmr NodeOutput
~/hadoop-1.2.1/bin/hadoop -put NodeList NodeList
done
