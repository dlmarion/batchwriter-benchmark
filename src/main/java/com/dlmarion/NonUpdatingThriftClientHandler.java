package com.dlmarion;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.ThriftClientHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonUpdatingThriftClientHandler extends ThriftClientHandler implements TabletClientService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(NonUpdatingThriftClientHandler.class);
  
  public NonUpdatingThriftClientHandler(TabletServer server) {
    super(server);
  }

  @Override
  public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent tkeyExtent,
      List<TMutation> tmutations) {
    TableId tid = TableId.of(new String(tkeyExtent.getTable(), UTF_8));
    if (!tid.canonical().equals("1")) {
      LOG.error("Updating table: {}", tid.canonical());
      super.applyUpdates(tinfo, updateID, tkeyExtent, tmutations);
    }
  }

  @Override
  public void update(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent,
      TMutation tmutation, TDurability tdurability)
      throws NotServingTabletException, ConstraintViolationException, ThriftSecurityException {
    TableId tid = TableId.of(new String(tkeyExtent.getTable(), UTF_8));
    if (!tid.canonical().equals("1")) {
      LOG.error("Updating table: {}", tid.canonical());
      super.update(tinfo, credentials, tkeyExtent, tmutation, tdurability);
    }
  }

  
}
