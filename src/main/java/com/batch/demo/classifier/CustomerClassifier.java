package com.batch.demo.classifier;

import com.batch.demo.entity.Customer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

public class CustomerClassifier implements Classifier<Customer, ItemWriter<? super Customer>> {

    private static final long serialVersionUID = -1L;

    private ItemWriter<Customer> evenWriter;
    private ItemWriter<Customer> oddWriter;

    public CustomerClassifier(ItemWriter<Customer> evenW, ItemWriter<Customer> oddW) {
        this.evenWriter = evenW;
        this.oddWriter = oddW;
    }

    @Override
    public ItemWriter<? super Customer> classify(Customer customer) {
        return customer.getId() % 2 == 0 ? evenWriter : oddWriter;
    }
}
