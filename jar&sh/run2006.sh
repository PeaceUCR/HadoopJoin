#!/bin/sh

hadoop fs -put u2006_process.txt u2006_process

hadoop fs -put location_process_ST.txt location_process_ST

hadoop jar MPJoin.jar classes/MPJoin u2006_process location_process_ST hdfsOutput

hadoop fs -get hdfsOutput/part-r-00000 result_join.txt

hadoop fs -rmr hdfsOutput

hadoop fs -put result_join.txt result_join

hadoop jar PerMonth.jar classes/PerMonth result_join hdfsOutput

hadoop fs -get hdfsOutput/part-r-00000 result_permonth.txt

hadoop fs -rmr hdfsOutput

hadoop fs -put result_permonth.txt result_permonth

hadoop jar PerState.jar classes/PerState result_permonth hdfsOutput

hadoop fs -get hdfsOutput/part-r-00000 result_perstate.txt

hadoop fs -rmr hdfsOutput




