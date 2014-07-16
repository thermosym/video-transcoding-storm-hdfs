#!/bin/bash


echo copy to $1
cp ./target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./target/$1
echo transfer to node2--file $1
scp ./target/$1 ym@node2:/home/ym/datacenter


