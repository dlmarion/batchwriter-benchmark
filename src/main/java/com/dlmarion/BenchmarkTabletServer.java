package com.dlmarion;

import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.ThriftClientHandler;

public class BenchmarkTabletServer extends TabletServer {

  BenchmarkTabletServer(ServerOpts opts, String[] args) {
      super(opts, args);
    }

    @Override
    protected ThriftClientHandler getThriftClientHandler() {
      return new NonUpdatingThriftClientHandler(this);
    }

    public static void main(String[] args) throws Exception {
      try (
        BenchmarkTabletServer tserver = new BenchmarkTabletServer(new ServerOpts(), args)) {
        tserver.runServer();
      }

    }

  }
