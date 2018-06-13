package pb.ticket.service.dbsetup;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TicketHoldMonitor implements Runnable {
	private static String SEAT_HOLD_ID = "_seat_hold_id_";
	private static String HOLD_EXPIRING_AT = "_expiring_at_";
	private static String RESERVATION_HOLD_ID = "_reservation_hold_id_";

	private static String KAFKA_TOPIC_NAME = "ticket-service";

	private static final Gson GSON = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private KafkaConsumer<String, String> consumer = null;
	private TicketServiceDB dbService;
	private final String kafkaBroker;
	private final ConcurrentMap<Integer, Long> ticketHoldMap;

	public TicketHoldMonitor(String kafkaBroker, TicketServiceDB db) {
		this.dbService = db;
		this.kafkaBroker = kafkaBroker;
		this.ticketHoldMap = new ConcurrentHashMap<Integer, Long>();
	}

	// Thread proc
	@Override
	public void run() {
		if (verifyTopic()) {

			consumerStart();

			try {
				while (!closed.get()) {
					ConsumerRecords<String, String> records = consumer.poll(1000);
					for (ConsumerRecord<String, String> record : records) {
						String item = record.value();

						Map<?, ?> messageMap = GSON.fromJson(item, Map.class);
						Object objReserveHoldId = messageMap.get(RESERVATION_HOLD_ID);

						if (objReserveHoldId != null) {
							this.ticketHoldMap.remove(Integer.valueOf(objReserveHoldId.toString()));
						} else {
							Object objSeatHoldId = messageMap.get(SEAT_HOLD_ID);
							Object objExpiringAt = messageMap.get(HOLD_EXPIRING_AT);

							if (objSeatHoldId != null && objExpiringAt != null) {
								this.ticketHoldMap.put(Integer.valueOf(objSeatHoldId.toString()),
										Long.valueOf(objExpiringAt.toString()));
							}
						}
					}

					consumer.commitSync();

					// Release expired seats
					// Give a 5 seconds buffer
					long currentTime = System.currentTimeMillis() - 5000;

					if (this.ticketHoldMap.size() > 0) {
						List<Entry<Integer, Long>> expiredHolds = this.ticketHoldMap.entrySet().stream()
								.filter(P -> P.getValue() <= currentTime).collect(Collectors.toList());

						if (expiredHolds != null && expiredHolds.size() > 0) {
							for (Entry<Integer, Long> e : expiredHolds) {
								this.dbService.releaseHold(e.getKey());

								this.ticketHoldMap.remove(e.getKey());
							}
						}
					}
				}
			} catch (Exception e) {
				if (!closed.get()) {
					e.printStackTrace();
				}
			} finally {
				if (consumer != null) {
					consumer.close();
					consumer = null;
				}
			}
		} else {
			System.out.println("Could not verify Kafka topic [ticket-service]");
		}

		System.out.println("Ticket hold release thread terminated.");
	}

	public void exit() {
		this.closed.set(true);

		if (consumer != null) {
			consumer.wakeup();
		}
	}

	private Boolean verifyTopic() {
		Properties props = new Properties();

		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);

		try {
			AdminClient kafkaAdmin = AdminClient.create(props);

			try {
				ListTopicsResult result = kafkaAdmin.listTopics();

				KafkaFuture<Set<String>> topicList = result.names();

				for (String topic : topicList.get()) {
					if (topic.equals(KAFKA_TOPIC_NAME))
						return true;
				}

				NewTopic nt = new NewTopic(KAFKA_TOPIC_NAME, 1, (short) 1);

				Map<String, String> topicConfig = new HashMap<>();
				topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "300000");
				topicConfig.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "300000");

				nt.configs(topicConfig);

				List<NewTopic> newTopics = new ArrayList<NewTopic>();
				newTopics.add(nt);

				CreateTopicsResult createResult = kafkaAdmin.createTopics(newTopics);

				for (Map.Entry<String, KafkaFuture<Void>> e : createResult.values().entrySet()) {
					e.getValue().get();

					return e.getValue().isDone();
				}
			} finally {
				kafkaAdmin.close();
			}

		} catch (Exception e) {

			e.printStackTrace();
		}

		return false;
	}

	private void consumerStart() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.kafkaBroker);
		props.put("enable.auto.commit", "false");
		props.put("client.id", getHostName() + "-ts-consumer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("request.timeout.ms", "30000");
		consumer = new KafkaConsumer<>(props);

		List<TopicPartition> partitionToAssign = new ArrayList<TopicPartition>();
		TopicPartition pt = new TopicPartition(KAFKA_TOPIC_NAME, 0);
		partitionToAssign.add(pt);

		consumer.assign(partitionToAssign);
		consumer.seekToBeginning(partitionToAssign);
	}

	private static String getHostName() {
		try {
			InetAddress myHost = InetAddress.getLocalHost();
			return myHost.getHostName();
		} catch (Exception e) {

		}

		return "Unknown";
	}
}
