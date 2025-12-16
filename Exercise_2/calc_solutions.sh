#!/usr/bin/env bash
for file in data/*
do
  echo -ne "$(basename "$file")\t"
  awk '{
         temp = substr($0, 88, 5) + 0
         q = substr($0, 93, 1)
         if (temp != 9999 && q ~ /[01459]/) {
             sum += temp
             count++
         }
       }
       END {
         if (count > 0)
             print sum / count
         else
             print "N/A"
       }' "$file"
done
