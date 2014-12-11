package com.datastax.tickdata;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.LongArrayList;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.timeseries.utils.TimeSeries;

public class TickDataDao {
	
	private static Logger logger = LoggerFactory.getLogger(TickDataDao.class);

	private Session session;
	private static String keyspaceName = "datastax_tickdata_binary_demo";
	private static String tableNameTick = keyspaceName + ".tick_data";

	private static final String INSERT_INTO_TICK = "Insert into " + tableNameTick + " (symbol,dates,ticks) values (?, ?,?);";
	private static final String SELECT_FROM_TICK = "Select symbol, dates, ticks from " + tableNameTick + " where symbol = ?";

	private PreparedStatement insertStmtTick;
	private PreparedStatement selectStmtTick;
	
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.zzz"); 

	public TickDataDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();
		
		this.session = cluster.connect();

		this.insertStmtTick = session.prepare(INSERT_INTO_TICK);		
		this.insertStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
		this.selectStmtTick = session.prepare(SELECT_FROM_TICK);		
		this.selectStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
	}
	
	public TimeSeries getTimeSeries(String symbol){
		
		BoundStatement boundStmt = new BoundStatement(this.selectStmtTick);
		boundStmt.setString(0, symbol);
		
		ResultSet resultSet = session.execute(boundStmt);		
		
		DoubleArrayList valueArray = new DoubleArrayList(10000);
		LongArrayList dateArray = new LongArrayList(10000);

		if (resultSet.isExhausted()){
			logger.info("No results found for symbol : " + symbol);
			dateArray.trimToSize();
			valueArray.trimToSize();
			
			return new TimeSeries(symbol, dateArray.elements(), valueArray.elements());
		}
		
		Row row = resultSet.one();
		
		LongBuffer dates = row.getBytes("dates").asLongBuffer();		
		DoubleBuffer ticks = row.getBytes("ticks").asDoubleBuffer();
		
		while (dates.hasRemaining()){
			dateArray.add(dates.get());
			valueArray.add(ticks.get());	
		}
		
		dateArray.trimToSize();
		valueArray.trimToSize();
		
		return new TimeSeries(symbol, dateArray.elements(), valueArray.elements());
	}

	public void insertTimeSeries(TimeSeries timeSeries) throws Exception{
		logger.info("Writing " + timeSeries.getSymbol());
		
		BoundStatement boundStmt = new BoundStatement(this.insertStmtTick);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		TimeSeries mostRecent = null;

		ByteBuffer datesBuffer = ByteBuffer.allocate(10_000_000*8);
		ByteBuffer pricesBuffer = ByteBuffer.allocate(10_000_000*8);
		
		long[] dates = timeSeries.getDates();
		double[] values = timeSeries.getValues();
		
		for (int i=0; i <dates.length; i++) {
			
			datesBuffer.putLong(dates[i]);
			pricesBuffer.putDouble(values[i]);
		}
				
		session.executeAsync(boundStmt.bind(timeSeries.getSymbol(), datesBuffer.flip(), pricesBuffer.flip()));		
		
		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		
		datesBuffer.clear();
		pricesBuffer.clear();
		
		return;
	}

	private String fillNumber(int num) {
		return num < 10 ? "0" + num : "" + num;
	}
}
