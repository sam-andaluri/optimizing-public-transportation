FROM confluentinc/cp-kafka-connect:7.6.1

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6 \
    && curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
        -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar

USER appuser
