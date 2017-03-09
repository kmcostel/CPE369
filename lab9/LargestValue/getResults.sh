#!/bin/bash
hadoop fs -get accounting-output
vi accounting-output/part-r-10000
