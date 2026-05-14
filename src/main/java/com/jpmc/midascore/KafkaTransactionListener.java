package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRepository;

    public KafkaTransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRepository) {

        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
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

        // update balances
        sender.setBalance(
                sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance() + transaction.getAmount()
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        // save transaction
        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount()
                );

        transactionRepository.save(record);
    }
}