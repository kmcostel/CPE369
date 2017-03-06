# Kevin Costello, Holly Haraguchi
#!/bin/bash

hadoop com.sun.tools.javac.Main hashtags.java

jar cvf hashtags.jar hashtags*class

hadoop jar hashtags.jar hashtags -libjars json-simple-1.1.1.jar test/hashtags-input.json hashtags-output.json
