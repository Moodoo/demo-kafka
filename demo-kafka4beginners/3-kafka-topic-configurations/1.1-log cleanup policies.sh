docker exec -it kafka-1 /bin/bash
root dit:
cd /opt/bitnami/kafka/bin


kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic configured-topic
kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic __consumer_offsets