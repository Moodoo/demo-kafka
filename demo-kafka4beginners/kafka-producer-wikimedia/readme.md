
docker exec -it kafka-1 /bin/bash
root dit:
cd /opt/bitnami/kafka/bin
kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic wikimedia.recentchange