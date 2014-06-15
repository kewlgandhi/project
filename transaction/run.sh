#!/bin/bash
ps -ef| grep 'rmiregistry'|grep -v 'grep'|awk '{print $2}'|xargs kill -9
make clean
rm -f ../test.part2/results/*
/bin/tcsh ./setenviron.sh
export CLASSPATH=$CLASSPATH:.:~/Documents/1Travel/Travel-reservation-system/project/transaction/fst-1.56.jar:~/Documents/1Travel/Travel-reservation-system/project/transaction/fst-1.56-onejar.jar:~/Documents/1Travel/Travel-reservation-system/project/transaction/javassist-3.18.1-GA.jar
make server
make client
rmiregistry -J-classpath -J.. 2100 &
make runtm &
make runrmflights &
make runrmrooms &
make runrmcustomers &
make runwc &
make runclient &
#/bin/tcsh ./setenviron.sh
cd ../test.part2
#setenv CLASSPATH .:gnujaxp.jar
/usr/bin/javac RunTests.java
rm -fr *.log
#java -cp -DrmiPort=2100 RunTests MASTER.xml >> Run.log &
java -DrmiPort=2100 RunTests MASTER.xml >> Run.log &
while [ ! -f results/RM.log ]
do
    sleep 1
done
tail -f results/RM.log >> Run.log &
tail -f Run.log
