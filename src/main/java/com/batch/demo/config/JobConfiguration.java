package com.batch.demo.config;

import com.batch.demo.classifier.CustomLineAggregator;
import com.batch.demo.classifier.CustomerClassifier;
import com.batch.demo.entity.Customer;
import com.batch.demo.mapper.CustomerRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class JobConfiguration {

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    DataSource dataSource;

    @Bean
    public JdbcPagingItemReader<Customer> customerJdbcPagingItemReader() {

        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(this.dataSource);
        reader.setFetchSize(1000);
        reader.setRowMapper(new CustomerRowMapper());

        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id,firstName,lastName,birthdate");
        queryProvider.setFromClause("from customer");
        queryProvider.setSortKeys(sortKeys);
        reader.setQueryProvider(queryProvider);

        return reader;
    }

    @Bean
    public FlatFileItemWriter<Customer> jsonItemWriter() throws Exception {
        String customerOutputPath = File.createTempFile("customerOutput", ".out").getAbsolutePath();
        System.out.println(">> output path = " + customerOutputPath);

        FlatFileItemWriter<Customer> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setLineAggregator(new CustomLineAggregator());
        fileItemWriter.setResource(new FileSystemResource(customerOutputPath));
        fileItemWriter.afterPropertiesSet();

        return fileItemWriter;
    }

    @Bean
    public StaxEventItemWriter<Customer> xmlItemWriter() throws Exception {

        String customerOutputPath = File.createTempFile("customerOutput", ".out").getAbsolutePath();
        System.out.println(">> Output Path = " + customerOutputPath);
        Map<String, Class> aliases = new HashMap<>();
        aliases.put("customer", Customer.class);

        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(aliases);

        // StAX and Marshaller for serializing object to XML.
        StaxEventItemWriter<Customer> writer = new StaxEventItemWriter<>();
        writer.setRootTagName("customers");
        writer.setMarshaller(marshaller);
        writer.setResource(new FileSystemResource(customerOutputPath));
        writer.afterPropertiesSet();
        return writer;
    }

    @Bean
    public ClassifierCompositeItemWriter<Customer> classifierCustomerCompositeItemWriter() throws Exception {
        ClassifierCompositeItemWriter<Customer> compositeItemWriter = new ClassifierCompositeItemWriter<>();
        compositeItemWriter.setClassifier(new CustomerClassifier(xmlItemWriter(), jsonItemWriter()));
        return compositeItemWriter;
    }

    @Bean
    public Step step1() throws Exception {
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(10)
                .reader(customerJdbcPagingItemReader())
                .writer(classifierCustomerCompositeItemWriter())
                .stream(xmlItemWriter())
                .stream(jsonItemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("job")
                .start(step1())
                .build();
    }


}
