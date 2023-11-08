# Project to demo kafka service
```gem 'rdkafka'```

```Server at cloudkafka.com```

#### See at app/services/kafka.rb

```rails runner Services::Kafka.new.consumer to consume```

```rails runner Services::Kafka.new.producer to produce```