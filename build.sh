mvn clean package -Psource-run
mvn -pl site.ycsb:postgrenosql-binding -am clean package -Psource-run  -Dcheckstyle.skip