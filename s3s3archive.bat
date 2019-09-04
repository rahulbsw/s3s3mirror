@echo off
java -Dlog4j.configuration=file:log4j.xml -Ds3tos3util.version=1.3.0 -jar ./s3tos3util*.jar --move %*
