create keyspace if not exists datastax_tickdata_binary_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor':3};

use datastax_tickdata_binary_demo; 

CREATE TABLE if not exists tick_data ( 
	symbol text,
	dates blob,
	ticks blob,
	PRIMARY KEY (symbol)
); 

truncate tick_data;
