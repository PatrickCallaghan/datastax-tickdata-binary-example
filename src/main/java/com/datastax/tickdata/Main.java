package com.datastax.tickdata;

import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.tickdata.engine.TickGenerator;
import com.datastax.timeseries.utils.TimeSeries;

public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class);

	private String ONE_MILLION = "1000000";
	private String TEN_MILLION = "10000000";
	private String FIFTY_MILLION = "50000000";
	private String ONE_HUNDRED_MILLION = "100000000";
	private String ONE_BILLION = "1000000000";

	public Main() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "10");
		
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));
		
		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		//Create shared queue 
		BlockingQueue<TimeSeries> queue = new ArrayBlockingQueue<TimeSeries>(1000);
		
		//Executor for Threads
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		Timer timer = new Timer();
		timer.start();
			
		for (int i = 0; i < noOfThreads; i++) {
			executor.execute(new TimeSeriesWriter(dao, queue));
		}
		
		//Load the symbols
		DataLoader dataLoader = new DataLoader ();
		List<String> exchangeSymbols = dataLoader.getExchangeData();
		
		//Start the tick generator
		Iterator<TimeSeries> tickGenerator = new TickGenerator(exchangeSymbols);
		
		while (tickGenerator.hasNext()){
			TimeSeries next = tickGenerator.next();
			logger.info("Adding " + next.getSymbol());
			
			try {
				queue.put(next);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		while(!queue.isEmpty() ){
			sleep(1);
		}		
		
		timer.end();
		logger.info("Data Loading took " + timer.getTimeTakenSeconds() + " secs. Total Points " + dao.getTotalPoints() + " (" + (dao.getTotalPoints()/timer.getTimeTakenSeconds()) + " a sec)");
		
		System.exit(0);
	}
	
	class TimeSeriesWriter implements Runnable {

		private TickDataDao dao;
		private BlockingQueue<TimeSeries> queue;

		public TimeSeriesWriter(TickDataDao dao, BlockingQueue<TimeSeries> queue) {
			this.dao = dao;
			this.queue = queue;
		}

		@Override
		public void run() {
			TimeSeries timeSeries;
			while(true){				
				timeSeries = queue.poll(); 
				
				if (timeSeries!=null){
					try {
						this.dao.insertTimeSeries(timeSeries);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}				
			}				
		}
	}
	
	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();
	}
}
