package com.batch.demo.classifier;

import com.batch.demo.entity.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.transform.LineAggregator;

public class CustomLineAggregator implements LineAggregator<Customer> {

    ObjectMapper objectMapper=new ObjectMapper();

    @Override
    public String aggregate(Customer item) {

        try {
            return objectMapper.writeValueAsString(item);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to serialize Customer", e);
        }


    }
}
