#!/bin/bash
ps -ef| grep 'rmiPort'|grep -v 'grep'|awk '{print $2}'|xargs kill -9
ps -ef| grep 'rmiregistry'|grep -v 'grep'|awk '{print $2}'|xargs kill -9
make clean
rm -rf */*.class
rm -f ../test.part2/results/*

make runregistry &
sleep 3
make runtm &
sleep 3
make runrmflights &
sleep 3
make runrmrooms &
sleep 3
make runrmcars &
sleep 3
make runrmcustomers &
sleep 3
make runwc &
sleep 3
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
