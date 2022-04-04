package com.example;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.hamcrest.CoreMatchers.is;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class HelloControllerTest {
    @Value("${local.server.port}")
    int port;

    @Test
    public void testHello() throws Exception {
    }

    @Test
    public void testCalc() throws Exception {
    }
}