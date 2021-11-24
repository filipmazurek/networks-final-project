package edu.duke.raft;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class StartClient {

  public static void main (String[] args) {
        if (args.length != 2) {
      System.out.println ("usage: java edu.duke.raft.StartClient -Djava.rmi.server.codebase=<codebase url> <int: server id> <string: operation path>");
      System.exit(1);
    }

    int id = Integer.parseInt (args[0]);
    String ops = args[1];
    
    String url = "rmi://localhost:" + "/C" + id;
    System.out.println ("Testing C" + id);
    System.out.println ("Contacting server via rmiregistry " + url);

    RaftClientImpl client = new RaftClientImpl(id);

    try {
      System.out.println("Client " + id + " reading operations from path " + ops);
      List<String> lines = Files.readAllLines(ops, StandardCharsets.US_ASCII);
      for (String line: lines) {
        /*
        Line := <Operation>:<ArgList>
        Operation := {SLEEP, SEND}
        ArgList := {<SleepArg>, <SendArg>}
        SleepArg := <int>
        SendArg := <int: serverID>,<int: port>
         */
        String[] tokens = line.split(":");
        if ((tokens != null) && tokens.length == 2) {
          String operation = tokens[0];
          String value = tokens[1];
          switch(operation) {
            case "SLEEP":
              Thread.sleep(Integer.parseInt(value));
              break;
            case "SEND":
              String[] arguments = value.split(",");
              client.writeEntry(Integer.parseInt(arguments[0]), Integer.parseInt(arguments[1]));
              break
            default: 
              throw new RuntimeException("Unexpected operation " + operation);
          }
        }
      }
    } catch (Exception e) {
      System.out.println(e);
      throw e;
    }
  }
}
