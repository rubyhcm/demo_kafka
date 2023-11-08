require "rdkafka"
class Services::Kafka
  def producer
    username = "yyxbyrlc"
    password = "4VUUhC0O9uQys4Bu2hLHpUb9wrkv19U7"
    hostname = "dory.srvs.cloudkafka.com"
    brokers = "#{hostname}:9094"
    topic = "#{username}-default"

    rdkafka = Rdkafka::Config.new(
      "bootstrap.servers" => brokers,
      "sasl.username" => username,
      "sasl.password" => password,
      "security.protocol" => "SASL_SSL",
      "sasl.mechanisms" => "SCRAM-SHA-512"
    )
    producer = rdkafka.producer

    puts "Producing 100 records to topic #{topic} on #{username}@#{brokers}"

    msg_id = 0
    batch_size = 10
    loop do
      Array.new(batch_size).map do
        puts "Producing message #{msg_id}"
        payload = "My message #{msg_id}"
        key = "key-#{msg_id}"
        msg_id += 1
        producer.produce(topic: topic, payload: payload, key: key)
      end.each(&:wait)
    end
  end

  def consumer
    username = "yyxbyrlc"
    password = "4VUUhC0O9uQys4Bu2hLHpUb9wrkv19U7"
    hostname = "dory.srvs.cloudkafka.com"
    brokers = "#{hostname}:9094"
    group_id = "#{username}-testing4"
    topic = "#{username}-default"

    rdkafka = Rdkafka::Config.new(
      "bootstrap.servers" => brokers,
      "group.id" => group_id,
      "sasl.username" => username,
      "sasl.password" => password,
      "security.protocol" => "SASL_SSL",
      "sasl.mechanisms" => "SCRAM-SHA-512",
      "client.id": "ruby-rdkafka"
    )
    consumer = rdkafka.consumer
    consumer.subscribe(topic)

    puts "Consuming topic #{topic} from #{username}@#{brokers}..."

    begin
      consumer.each do |msg|
        puts "[#{msg.topic}-#{msg.partition}] offset=#{msg.offset} key='#{msg.key}' payload='#{msg.payload}'"
      end
    rescue Rdkafka::RdkafkaError => e
      retry if e.is_partition_eof?
      raise
    end
  end
end
