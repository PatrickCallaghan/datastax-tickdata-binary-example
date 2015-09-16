package com.datastax.tickdata;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.timeseries.utils.TimeSeries;

public class Read {
	private static Logger logger = LoggerFactory.getLogger(Read.class);


	public Read() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String symbol = PropertyHelper.getProperty("symbol", "AMEX-VWO-2014-12-11");

		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));
		
		Timer timer = new Timer();
		timer.start();
		
		TimeSeries timeSeries = dao.getTimeSeries(symbol);
			
		timer.end();
		logger.info("Data read took " + timer.getTimeTakenMillis() + " ms. Total Points " + timeSeries.getDates().length);
		
		System.exit(0);
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Read();
	}
}
