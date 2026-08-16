FROM confluentinc/cp-kafka-connect:7.6.1
 
# Debezium's Postgres connector plugin.
# Debezium isn't reliably published on Confluent Hub for recent versions,
# so pull the official plugin bundle directly from Maven Central instead.
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-postgres \
    && curl -fsSL \
      https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.0.Final/debezium-connector-postgres-2.7.0.Final-plugin.tar.gz \
      -o /tmp/debezium-postgres.tar.gz \
    && tar -xzf /tmp/debezium-postgres.tar.gz -C /usr/share/confluent-hub-components/debezium-connector-postgres --strip-components=1 \
    && rm /tmp/debezium-postgres.tar.gz
 
# Confluent's Avro converter, so Connect can serialize records as Avro
# and register schemas with Schema Registry (rather than default JSON)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.6.1