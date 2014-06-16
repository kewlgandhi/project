#!/bin/bash
ps -ef| grep 'rmiPort'|grep -v 'grep'|awk '{print $2}'|xargs kill -9
ps -ef| grep 'rmiregistry'|grep -v 'grep'|awk '{print $2}'|xargs kill -9
make clean
rm -rf */*.class
rm -f ../test.part2/results/*

make runregistry &
sleep 5
make runtm &
sleep 5
make runrmflights &
sleep 5
make runrmrooms &
sleep 5
make runrmcars &
sleep 5
make runrmcustomers &
sleep 5
make runwc &
sleep 5
make client &

cd ../test.part2
/bin/tcsh ./setenviron.sh

/usr/bin/javac RunTests.java
rm -fr *.log
java -DrmiPort=2100 RunTests MASTER.xml >> Run.log &

while [ ! -f results/RMFlights.log ]
do
    sleep 1
done
tail -f results/RMFlights.log >> Run.log &
tail -f results/TM.log >> Run.log &
tail -f Run.log
