#!/bin/bash

uri=$1
filepath=$2
md5=$3

if [ ! -e $filepath ]; then
    echo "Downloading $uri to $filepath"
    curl -skL $uri -o $filepath
fi

if [ "$md5" ]; then
    if md5sum -s -c <<< "$md5  $filepath"; then
        echo "File checksum verified: $filepath"
    else
        echo "Checksum failed for $filepath"
        exit 1
    fi
fi

if [[ $filepath == *.zip ]]; then
    parentdir=$(dirname $filepath)
    unzip -o -d $parentdir $filepath
fi
