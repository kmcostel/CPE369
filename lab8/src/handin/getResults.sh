# Kevin Costello and Holly Haraguchi
# Gets each program's output file and renames them accordingly
#!/bin/bash

hdfs dfs -get test/accounting-output/part-r-00000 accounting-output.json
hdfs dfs -get test/hashtags-output/part-r-00000 hashtags-output.json
hdfs dfs -get test/favoriteMovie-output/part-r-00000 favoriteMovie-output.json
hdfs dfs -get test/userSim-output/part-r-00000 userSim-output.json
