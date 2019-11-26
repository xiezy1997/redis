#!/bin/bash

#================================================================
#
#   File Name:     make.sh
#   Creator:       Xie Zhiyong
#   Create Time:   2019.11.24
#
#================================================================

if [[ $# < 1 ]]; then
    echo "./make.sh opt[string/hash/list/set/zset/clean]"
    exit
fi

opt=$1

if  [ $opt == "string" ] || \
    [ $opt == "hash" ] || \
    [ $opt == "list" ] || \
    [ $opt == "set" ] || \
    [ $opt == "zset" ] ; then
    cd ./example
    make $opt=1
elif [ $opt == "clean" ]; then
    cd ./example
    make clean
else
    echo "./make.sh opt[string/hash/list/set/zset/clean]"
fi

