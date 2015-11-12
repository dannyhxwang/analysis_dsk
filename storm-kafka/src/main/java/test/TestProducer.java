package test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class TestProducer {

	public static void main(String[] args) throws Exception {

		Properties originalProps = new Properties();
		//broker
		originalProps.put("metadata.broker.list", "datanode1:9092,datanode2:9092,datanode4:9092");
		//把数据序列化到broker
		originalProps.put("serializer.class", "kafka.serializer.StringEncoder");
		originalProps.put("request.required.acks", "1");
		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(originalProps ));

        int j = 0;
		while(j < 1000) {
            Thread.sleep(1000);
			producer.send(new KeyedMessage<String, String>("topic1", null, j+""));
            j++;
		}
		producer.close();
	}

}
