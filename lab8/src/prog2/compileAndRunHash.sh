# Kevin Costello, Holly Haraguchi
#!/bin/bash

# This seems like it is definitely correct
hadoop com.sun.tools.javac.Main hashtags.java

jar cvf hashtags.jar hashtags*class

hadoop jar hashtags.jar hashtags -libjars json-simple-1.1.1.jar lab8_inputs/hashtags-input.json lab8_outputs/prog2

