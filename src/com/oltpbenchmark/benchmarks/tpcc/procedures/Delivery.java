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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.procedures.PrettyPrintingMap;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetOrderIdResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetSumOrderAmountResult;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Stats;

public class Delivery extends TPCCProcedure {

    public SQLStmt delivGetOrderIdSQL = new SQLStmt("SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1");
    public SQLStmt delivDeleteNewOrderSQL = new SQLStmt("DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + "" + " WHERE NO_O_ID = ? AND NO_D_ID = ?" + " AND NO_W_ID = ?");
    public SQLStmt delivGetCustIdSQL = new SQLStmt("SELECT O_C_ID" + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?");
    public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?");
    public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt delivSumOrderAmountSQL = new SQLStmt("SELECT SUM(OL_AMOUNT) AS OL_TOTAL" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?");
    public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + ?" + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1" + " WHERE C_W_ID = ?" + " AND C_D_ID = ?" + " AND C_ID = ?");

    // Delivery Txn
    public PreparedStatement delivGetOrderId = null;
    public PreparedStatement delivDeleteNewOrder = null;//
    public PreparedStatement delivGetCustId = null;
    public PreparedStatement delivUpdateCarrierId = null;//
    public PreparedStatement delivUpdateDeliveryDate = null;//
    public PreparedStatement delivSumOrderAmount = null;
    public PreparedStatement delivUpdateCustBalDelivCnt = null;//

    private final static Logger logger = Logger.getLogger(Delivery.class);

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, 
            int terminalDistrictLowerID, int terminalDistrictUpperID, Map<String, Object> tres) throws SQLException {
        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);
//        int orderCarrierID = 0;

        delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL);
        delivDeleteNewOrder = this.getPreparedStatement(conn, delivDeleteNewOrderSQL);
        delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL);
        delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdSQL);
        delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateSQL);
        delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountSQL);
        delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntSQL);
        
        if (Config.DEBUG) {
            out.println(String.format("Delivery w_id = %d, carrier_id=%d", terminalWarehouseID, orderCarrierID));
        }

        deliveryTransaction(terminalWarehouseID, orderCarrierID, conn, tres);
        return null;
    }

    public int deliveryTransaction(int w_id, int o_carrier_id, 
            Connection conn, Map<String, Object> tres) throws SQLException {

        int d_id, c_id;
        long deliveryDate = System.currentTimeMillis();// 1444665555000L;//
        String ol_total;
        int[] orderIDs;
        if (TPCCConstants.DML_Trace) {
            tres.put("count", 0);
        }

        orderIDs = new int[10];

        for (d_id = 1; d_id <= 10; d_id++) {

            delivGetOrderId.setInt(1, d_id);
            delivGetOrderId.setInt(2, w_id);
            ResultSet rs = delivGetOrderId.executeQuery();
            int no_o_id = -1;
            if (!rs.next()) {
                // This district has no new orders; this can happen but should
                // be rare
                if (Config.ENABLE_LOGGING) {
                    tres.put("o" + d_id, no_o_id);
                }
                continue;
            }

            no_o_id = rs.getInt("NO_O_ID");
            orderIDs[d_id - 1] = no_o_id;
            rs.close();
            rs = null;

            delivDeleteNewOrder.setInt(1, no_o_id);
            delivDeleteNewOrder.setInt(2, d_id);
            delivDeleteNewOrder.setInt(3, w_id);
            int result = delivDeleteNewOrder.executeUpdate();
            if (result != 1) {
                // This code used to run in a loop in an attempt to make this
                // work
                // with MySQL's default weird consistency level. We just always
                // run
                // this as SERIALIZABLE instead. I don't *think* that fixing
                // this one
                // error makes this work with MySQL's default consistency.
                // Careful
                // auditing would be required.
                throw new UserAbortException("New order w_id=" + w_id + " d_id=" + d_id + " no_o_id=" + no_o_id + " delete failed (not running with SERIALIZABLE isolation?)");
            }

            if (TPCCConstants.DML_Trace) {
                int count = (Integer) tres.get("count");
                tres.put("DML" + count, delivDeleteNewOrder.toString());
                count++;
                tres.put("count", count);
            }

            delivGetCustId.setInt(1, no_o_id);
            delivGetCustId.setInt(2, d_id);
            delivGetCustId.setInt(3, w_id);
            rs = delivGetCustId.executeQuery();

            if (!rs.next())
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
            c_id = rs.getInt("O_C_ID");
            rs.close();
            rs = null;

            delivUpdateCarrierId.setInt(1, o_carrier_id);
            delivUpdateCarrierId.setInt(2, no_o_id);
            delivUpdateCarrierId.setInt(3, d_id);
            delivUpdateCarrierId.setInt(4, w_id);
            result = delivUpdateCarrierId.executeUpdate();

            if (TPCCConstants.DML_Trace) {
                int count = (Integer) tres.get("count");
                tres.put("DML" + count, delivUpdateCarrierId.toString());
                count++;
                tres.put("count", count);
            }

            if (result != 1)
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

            delivUpdateDeliveryDate.setTimestamp(1, new Timestamp(deliveryDate));
            delivUpdateDeliveryDate.setInt(2, no_o_id);
            delivUpdateDeliveryDate.setInt(3, d_id);
            delivUpdateDeliveryDate.setInt(4, w_id);
            result = delivUpdateDeliveryDate.executeUpdate();

            if (TPCCConstants.DML_Trace) {
                int count = (Integer) tres.get("count");
                tres.put("DML" + count, delivUpdateDeliveryDate.toString());
                count++;
                tres.put("count", count);
            }

            if (result == 0)
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

            delivSumOrderAmount.setInt(1, no_o_id);
            delivSumOrderAmount.setInt(2, d_id);
            delivSumOrderAmount.setInt(3, w_id);
            rs = delivSumOrderAmount.executeQuery();

            if (!rs.next())
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
            ol_total = rs.getString("OL_TOTAL");
            rs.close();
            // ol_total=Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(ol_total));
            rs = null;
            // System.out.println("OL_Total:"+ol_total);

            ol_total = TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(ol_total));
            // System.out.println("OL_Total:"+ol_total);
            BigDecimal bd = new BigDecimal(ol_total);

            delivUpdateCustBalDelivCnt.setBigDecimal(1, bd);
            delivUpdateCustBalDelivCnt.setInt(2, w_id);
            delivUpdateCustBalDelivCnt.setInt(3, d_id);
            delivUpdateCustBalDelivCnt.setInt(4, c_id);
            result = delivUpdateCustBalDelivCnt.executeUpdate();

            if (TPCCConstants.DML_Trace) {
                int count = (Integer) tres.get("count");
                tres.put("DML" + count, delivUpdateCustBalDelivCnt.toString());
                count++;
                tres.put("count", count);
            }
            if (Config.ENABLE_LOGGING) {
                tres.put("o" + d_id, no_o_id);
                tres.put("c" + d_id, c_id);
                tres.put("b" + d_id, TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(ol_total)));
            }

            if (result == 0)
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
        }

        conn.commit();
        if (Config.ENABLE_LOGGING) {
            tres.put("w_id", w_id);

            tres.put("delivery_date", deliveryDate);
            // w.put("d_id", d_id);
            // w.put("o_count",j);
            tres.put("o_carrier_id", o_carrier_id);
        }

        if (Config.DEBUG) {
            System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
        }

        // TODO: This part is not used
        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- DELIVERY ---------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n Carrier:   ");
        terminalMessage.append(o_carrier_id);
        terminalMessage.append("\n\n Delivered Orders\n");
        int skippedDeliveries = 0;
        for (int i = 1; i <= 10; i++) {
            if (orderIDs[i - 1] >= 0) {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": Order number ");
                terminalMessage.append(orderIDs[i - 1]);
                terminalMessage.append(" was delivered.\n");
            } else {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": No orders to be delivered.\n");
                skippedDeliveries++;
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");

        return skippedDeliveries;
    }

    public int deliveryTransaction(int w_id, int o_carrier_id, 
            Connection conn, NgCache cafe, Map<String, Object> tres) throws SQLException {

        int d_id, c_id;
        long deliveryDate = System.currentTimeMillis();// 1444665555000L;//
        String ol_total;
        int[] orderIDs;
        if (TPCCConstants.DML_Trace) {
            tres.put("count", 0);
        }

        orderIDs = new int[10];

        for (d_id = 1; d_id <= 10; d_id++) {
            int retry = 0;
            while (true) {
                try {
                    cafe.startSession("Delivery");
                    
                    String getOrderId = String.format(TPCCConfig.QUERY_GET_ORDER_ID, w_id, d_id);
                    QueryGetOrderIdResult res1 = (QueryGetOrderIdResult) cafe.readStatement(getOrderId);
//                    System.out.println(Stats.getAllStats().toString(2));
                    int no_o_id = -1;                        
                    if (res1 == null || res1.getNewOrderIds().size() == 0) {
                        // This district has no new orders; this can happen but should
                        // be rare
                        if (Config.ENABLE_LOGGING) {
                            tres.put("o" + d_id, no_o_id);
                        }
                        continue;
                    }

                    no_o_id = res1.getNewOrderIds().get(0);
                    orderIDs[d_id - 1] = no_o_id;

                    String deleteNewOrder = String.format(TPCCConfig.DML_DELETE_NEW_ORDER, w_id, d_id, no_o_id);
                    boolean success = cafe.writeStatement(deleteNewOrder);
                   // System.out.println(Stats.getAllStats().toString(2));
                    if (!success) {
                        // This code used to run in a loop in an attempt to make this
                        // work
                        // with MySQL's default weird consistency level. We just always
                        // run
                        // this as SERIALIZABLE instead. I don't *think* that fixing
                        // this one
                        // error makes this work with MySQL's default consistency.
                        // Careful
                        // auditing would be required.
                        throw new UserAbortException("New order w_id=" + w_id + " d_id=" + d_id + " no_o_id=" + no_o_id + " delete failed (not running with SERIALIZABLE isolation?)");
                    }

                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivDeleteNewOrder.toString());
                        count++;
                        tres.put("count", count);
                    }

                    String getCustId = String.format(TPCCConfig.QUERY_DELIVERY_GET_CUST_ID, w_id, d_id, no_o_id);
                    QueryGetCustIdResult res2 = (QueryGetCustIdResult) cafe.readStatement(getCustId);

                    if (res2 == null)
                        throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
                    c_id = res2.getOCId();

                    String updateCarrierId = String.format(TPCCConfig.DML_UPDATE_CARRIER_ID, w_id, d_id, no_o_id, o_carrier_id, c_id);
                    success = cafe.writeStatement(updateCarrierId);
//                    System.out.println(Stats.getAllStats().toString(2));
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivUpdateCarrierId.toString());
                        count++;
                        tres.put("count", count);
                    }

                    if (!success)
                        throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

                    String updateDeliveryDate = String.format(TPCCConfig.DML_UPDATE_DELIVERY_DATE, w_id, d_id, no_o_id, deliveryDate, c_id);
                    success = cafe.writeStatement(updateDeliveryDate);

                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivUpdateDeliveryDate.toString());
                        count++;
                        tres.put("count", count);
                    }

                    if (!success)
                        throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

                    String getSumOrderAmount = String.format(TPCCConfig.QUERY_GET_SUM_ORDER_AMOUNT, w_id, d_id, no_o_id);
                    QueryGetSumOrderAmountResult res3 = (QueryGetSumOrderAmountResult) cafe.readStatement(getSumOrderAmount);

                    if (res3 == null)
                        throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
                    ol_total = String.valueOf(res3.getTotal());
                    //rs.close();
                    // ol_total=Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(ol_total));
                    //rs = null;
                    // System.out.println("OL_Total:"+ol_total);

                    ol_total = TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(ol_total));
                    String updateCustBalDelivCnt = String.format(TPCCConfig.DML_UPDATE_CUST_BAL_DELIVERY_CNT, w_id, d_id, c_id, ol_total);
                    success = cafe.writeStatement(updateCustBalDelivCnt);
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivUpdateCustBalDelivCnt.toString());
                        count++;
                        tres.put("count", count);
                    }
                    if (Config.ENABLE_LOGGING) {
                        tres.put("o" + d_id, no_o_id);
                        tres.put("c" + d_id, c_id);
                        tres.put("b" + d_id, TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(ol_total)));
                    }

                    if (!success)
                        throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");

                    conn.commit();
                    cafe.commitSession();
                    break;
                } catch (Exception e) {
                    //                e.printStackTrace(System.out);
                    try {
                        cafe.abortSession();
                    } catch (Exception e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    // throw new UserAbortException("Some error happens. "+ e.getMessage());

                    if (e instanceof COException) {
//                        cafe.getStats().incr(((COException) e).getKey());
                    }

                    try {
                        conn.rollback();
                    } catch (SQLException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }

                sleepRetry();
                retry++;
            }


            cafe.getStats().incr("retry"+retry);
        }

        if (Config.ENABLE_LOGGING) {
            tres.put("w_id", w_id);

            tres.put("delivery_date", deliveryDate);
            // w.put("d_id", d_id);
            // w.put("o_count",j);
            tres.put("o_carrier_id", o_carrier_id);
        }

        if (Config.DEBUG) {
            System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
        }

        return 0;
    }

    public int deliveryTransaction2(int w_id, int o_carrier_id, 
            Connection conn, NgCache cafe, Map<String, Object> tres) throws SQLException {

        int d_id, c_id;
        long deliveryDate = System.currentTimeMillis();// 1444665555000L;//
        String ol_total;
        int[] orderIDs;
        if (TPCCConstants.DML_Trace) {
            tres.put("count", 0);
        }
        int deliveryItems = TPCCConfig.configDistPerWhse;

        orderIDs = new int[deliveryItems];

        String[] queries = new String[deliveryItems];
        for (d_id = 1; d_id <= deliveryItems; d_id++) {
            String getOrderId = String.format(TPCCConfig.QUERY_GET_ORDER_ID, w_id, d_id);
            queries[d_id-1] = getOrderId;            
        }
        String[] queries2 = new String[deliveryItems*2];
        
        int retry = 0;
        while (true) {
            try {                
                cafe.startSession("Delivery", String.valueOf(w_id));
                
                QueryResult[] results = cafe.readStatements(queries);
                for (d_id = 1; d_id <= deliveryItems; d_id++) {
                    QueryGetOrderIdResult res1 = (QueryGetOrderIdResult) results[d_id-1];
                    int no_o_id = -1;                        
                    if (res1 == null || res1.getNewOrderIds().size() == 0) {
                        // This district has no new orders; this can happen but should
                        // be rare
                        if (Config.ENABLE_LOGGING) {
                            tres.put("o" + d_id, no_o_id);
                        }
                        continue;
                    }

                    no_o_id = res1.getNewOrderIds().get(0);
                    orderIDs[d_id - 1] = no_o_id;
//                    updateLast[d_id - 1] = (res1.getNewOrderIds().size() == 1);
//                    cafe.getStats().incrBy(String.format("unprocessed_orders_w%d_d%d", w_id, d_id), res1.getNewOrderIds().size());
//                    cafe.getStats().incr(String.format("delivery_w%d_d%d", w_id, d_id));
                }

                for (d_id = 1; d_id <= deliveryItems; d_id++) {
                    queries2[2*(d_id-1)] = String.format(TPCCConfig.QUERY_DELIVERY_GET_CUST_ID, w_id, d_id, orderIDs[d_id-1]);
                    queries2[2*(d_id-1)+1] = String.format(TPCCConfig.QUERY_GET_SUM_ORDER_AMOUNT, w_id, d_id, orderIDs[d_id-1]);
                }          
                results = cafe.readStatements(queries2);
                
                int idx = 0;
                for (d_id = 1; d_id <= deliveryItems; d_id++) {
                    int no_o_id = orderIDs[d_id-1];
                    
                    String deleteNewOrder = String.format(TPCCConfig.DML_DELETE_NEW_ORDER, w_id, d_id, no_o_id);
                    boolean success = cafe.writeStatement(deleteNewOrder);
                    if (!success) {
                        // This code used to run in a loop in an attempt to make this
                        // work
                        // with MySQL's default weird consistency level. We just always
                        // run
                        // this as SERIALIZABLE instead. I don't *think* that fixing
                        // this one
                        // error makes this work with MySQL's default consistency.
                        // Careful
                        // auditing would be required.
                        throw new UserAbortException("New order w_id=" + w_id + " d_id=" + d_id + " no_o_id=" + no_o_id + " delete failed (not running with SERIALIZABLE isolation?)");
                    }

                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivDeleteNewOrder.toString());
                        count++;
                        tres.put("count", count);
                    }

                    QueryGetCustIdResult res2 = (QueryGetCustIdResult) results[idx++];

                    if (res2 == null)
                        throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
                    c_id = res2.getOCId();

                    String updateCarrierId = null;
                    updateCarrierId = String.format(TPCCConfig.DML_UPDATE_CARRIER_ID, w_id, d_id, no_o_id, o_carrier_id, c_id);
                    success = cafe.writeStatement(updateCarrierId);
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivUpdateCarrierId.toString());
                        count++;
                        tres.put("count", count);
                    }

                    if (!success)
                        throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

                    String updateDeliveryDate;
                    updateDeliveryDate = String.format(TPCCConfig.DML_UPDATE_DELIVERY_DATE, w_id, d_id, no_o_id, deliveryDate);
                    success = cafe.writeStatement(updateDeliveryDate);

                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivUpdateDeliveryDate.toString());
                        count++;
                        tres.put("count", count);
                    }

                    if (!success)
                        throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

                    QueryGetSumOrderAmountResult res3 = (QueryGetSumOrderAmountResult) results[idx++];

                    if (res3 == null)
                        throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
                    ol_total = String.valueOf(res3.getTotal());
                    //rs.close();
                    // ol_total=Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(ol_total));
                    //rs = null;
                    // System.out.println("OL_Total:"+ol_total);

                    ol_total = TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(ol_total));
                    String updateCustBalDelivCnt = String.format(TPCCConfig.DML_UPDATE_CUST_BAL_DELIVERY_CNT, w_id, d_id, c_id, ol_total);
                    success = cafe.writeStatement(updateCustBalDelivCnt);
                    if (TPCCConstants.DML_Trace) {
                        int count = (Integer) tres.get("count");
                        tres.put("DML" + count, delivUpdateCustBalDelivCnt.toString());
                        count++;
                        tres.put("count", count);
                    }
                    if (Config.ENABLE_LOGGING) {
                        tres.put("o" + d_id, no_o_id);
                        tres.put("c" + d_id, c_id);
                        tres.put("b" + d_id, TPCCConstants.DECIMAL_FORMAT.format(Double.parseDouble(ol_total)));
                    }

                    if (!success)
                        throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
                }

                if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }
                
                break;
            } catch (Exception e) {
//                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                // throw new UserAbortException("Some error happens. "+ e.getMessage());

                if (e instanceof COException) {
//                    cafe.getStats().incr(((COException) e).getKey());
//                    System.out.println(((COException) e).getKey());
                }

                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }

            sleepRetry();
            retry++;
            cafe.getStats().incr("retry"+retry);
        }


        if (Config.ENABLE_LOGGING) {
            tres.put("w_id", w_id);

            tres.put("delivery_date", deliveryDate);
            // w.put("d_id", d_id);
            // w.put("o_count",j);
            tres.put("o_carrier_id", o_carrier_id);
        }

        if (Config.DEBUG) {
            System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
        }

        return 0;
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID, 
            NgCache cafe, Map<String, Object> tres)
                    throws SQLException {
        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);
        
        if (Config.DEBUG) {
            out.println(String.format("Delivery w_id = %d, carrier_id=%d", terminalWarehouseID, orderCarrierID));
        }
        
        deliveryTransaction2(terminalWarehouseID, orderCarrierID, conn, cafe, tres);
        return null;
    }

}
