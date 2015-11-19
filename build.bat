call mvn versions:set -DnewVersion=2.0.0a46 -f ./pom.xml clean install

call mvn -f ./pom.xml assembly:assembly
copy .\target\*.jar .\
