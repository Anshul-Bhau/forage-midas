// package com.jpmc.midascore;

// import com.jpmc.midascore.foundation.Transaction;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.stereotype.Component;

// @Component
// public class KafkaProducer {
//     private final String topic;
//     private final KafkaTemplate<String, Transaction> kafkaTemplate;

//     public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
//         this.topic = topic;
//         this.kafkaTemplate = kafkaTemplate;
//     }

//     public void send(String transactionLine) {
//     if (transactionLine == null || transactionLine.isBlank()) return;

//     // 1. Split by commas first to get potential raw fields
//     String[] data = transactionLine.split(",");

//     // 2. We need exactly 3 parts: senderId, recipientId, amount
//     if (data.length >= 3) {
//         try {
//             // Use regex to strip everything EXCEPT digits from IDs
//             long senderId = Long.parseLong(data[0].replaceAll("[^0-9]", ""));
//             long recipientId = Long.parseLong(data[1].replaceAll("[^0-9]", ""));

//             // Use regex to keep only digits and the first decimal point for the amount
//             // This effectively turns "1200.23\ngrommit" into "1200.23"
//             String amountCleaned = data[2].replaceAll("[^0-9.]", " ");
//             String amountFinal = amountCleaned.trim().split("\\s+")[0];
            
//             float amount = Float.parseFloat(amountFinal);

//             kafkaTemplate.send(topic, new Transaction(senderId, recipientId, amount));
//         } catch (Exception e) {
//             // If it still fails, it's truly bad data; we skip it to let the test continue
//         }
//     }
// }
// }

package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        if (transactionLine == null || transactionLine.isBlank()) return;

        // 1. Split the incoming string by newlines (handles both \r\n and \n)
        // This prevents the entire file from being processed as a single line.
        String[] lines = transactionLine.split("\\r?\\n");

        // 2. Loop through every actual line
        for (String line : lines) {
            if (line.trim().isEmpty()) continue;
            
            String[] transactionData = line.split(", ");
            
            // Only process if it has the required 3 parts (sender, recipient, amount)
            if (transactionData.length >= 3) {
                try {
                    long senderId = Long.parseLong(transactionData[0].trim());
                    long recipientId = Long.parseLong(transactionData[1].trim());
                    float amount = Float.parseFloat(transactionData[2].trim());

                    kafkaTemplate.send(topic, new Transaction(senderId, recipientId, amount));
                } catch (NumberFormatException e) {
                    // Safely ignore lines that are headers or genuinely malformed (like "grommit")
                    System.out.println("Skipping invalid transaction line: " + line);
                }
            }
        }
    }
}
