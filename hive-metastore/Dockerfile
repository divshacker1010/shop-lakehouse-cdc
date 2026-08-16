# FROM apache/hive:standalone-metastore-4.2.0
FROM apache/hive:3.1.3
USER root
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar