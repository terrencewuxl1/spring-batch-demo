package com.batch.demo.mapper;

import com.batch.demo.entity.Customer;
import org.springframework.jdbc.core.RowMapper;

import javax.swing.tree.TreePath;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CustomerRowMapper implements RowMapper<Customer> {

    @Override
    public Customer mapRow(ResultSet resultSet, int i) throws SQLException {
        return Customer.builder().id(resultSet.getLong("id"))
                .firstName(resultSet.getString("firstName"))
                .lastName(resultSet.getString("lastName"))
                .birthdate(resultSet.getString("birthdate"))
                .build();
    }
}
