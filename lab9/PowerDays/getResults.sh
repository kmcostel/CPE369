# Holly Haraguchi, Kevin Costello
# Lab 9, Problem 7: Power Days
# Gets the output from our Hadoop job as a file called 'powerDays.output'

rm powerDays.output
hadoop fs -get test/highest-out/part-r-00000 powerDays.output
