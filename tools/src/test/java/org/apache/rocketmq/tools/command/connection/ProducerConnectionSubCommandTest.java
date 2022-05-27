/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.tools.command.connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static org.mockito.Mockito.mock;

public class ProducerConnectionSubCommandTest {

    private static final int NAME_SERVER_PORT = 45677;

    private static final int BROKER_PORT = 45676;

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = NameServerMocker.startByDefaultConf(NAME_SERVER_PORT, BROKER_PORT);
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        ProducerConnectionSubCommand cmd = new ProducerConnectionSubCommand();
        Options options = new Options();
        ServerUtil.configCommandlineOptions(options);
        String[] subargs = new String[] {"-g default-producer-group", "-t unit-test"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startOneBroker() {
        ProducerConnection producerConnection = new ProducerConnection();
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection connection = mock(Connection.class);
        connectionSet.add(connection);
        producerConnection.setConnectionSet(connectionSet);

        // start broker
        return ServerResponseMocker.startServer(BROKER_PORT, producerConnection.encode());
    }
}
