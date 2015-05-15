/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchpress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;

public class BenchPress {
    private static final Logger LOG = Logger.getLogger(BenchPress.class);
    
    private static BenchPressService service = null;
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) {
        // Initialize log4j
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }
        //GAME_BEHAVIOR = true;
        System.out.println("HELLO!!!!");
        // create the command line parser
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption(
                "p",
                "port",
                true,
                "Set the port");
        options.addOption(
                "h",
                "hostname",
                true,
                "Set the hostname");
        options.addOption(
                "help",
                false,
                "Print this help");
        
        // defaults
        int port = 3001;
        String hostname = "localhost";
        
        // parse the command line arguments
        try {
            CommandLine argsLine = parser.parse(options, args);
            if (argsLine.hasOption("help")) {
                printUsage(options);
                return;
            }
            
            if (argsLine.hasOption("p")) {
                port = Integer.parseInt(argsLine.getOptionValue("p"));
            }
            
            if (argsLine.hasOption("h")) {
                hostname = argsLine.getOptionValue("h");
            }
        } catch (ParseException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        Configuration serverConfig = new Configuration();
        serverConfig.setHostname(hostname);
        serverConfig.setPort(port);

        final SocketIOServer server = new SocketIOServer(serverConfig);
        service = new BenchPressService(server);
        server.addListeners(service);

        server.start();
    }
    
    public static BenchPressService getService() {
        return service;
    }
    
    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }
   
}
