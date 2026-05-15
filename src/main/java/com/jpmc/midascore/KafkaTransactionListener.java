package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.web.client.RestTemplate;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRepository;
    private final RestTemplate restTemplate;

    public KafkaTransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRepository,
            RestTemplate restTemplate) {

        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId());

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        // invalid users
        if (sender == null || recipient == null) {
            return;
        }

        // insufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentiveResponse = 
            restTemplate.postForObject("http://localhost:8080/incentive",
                transaction,
                Incentive.class);  
        
        float incentiveAmount = 0;

        if (incentiveResponse != null) {
            incentiveAmount = incentiveResponse.getAmount();
        }

        // update balances
        sender.setBalance(
                sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance() + transaction.getAmount() + incentiveAmount
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        // save transaction
        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                );

        transactionRepository.save(record);
    }
}