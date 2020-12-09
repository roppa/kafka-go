# Running

Create 2 consumers, one for topica and one for topicb:

```
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic topica
```

```
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic topicb
```