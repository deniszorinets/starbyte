insert into pipelines(pipeline_id, name) values('148fd0b6-f51f-4a95-b904-41a59dfcb62b', 'test');

insert into steps (step_id, pipeline_id, input_step_id, image, runtime_config, config, name) values ('84225863-da7a-4b44-bb6a-bd3e3f801682', '148fd0b6-f51f-4a95-b904-41a59dfcb62b', null, '', 
'{
    "batch_size": 1,
    "batch_timeout": 1000,
    "max_retries": 3
}', 
'{
    "file_uri": "http://minioadmin:minioadmin@localhost:9000/test/test.csv",
    "has_header": false
}', 
    'input'
);
        
insert
	into
	steps (step_id, pipeline_id,
	input_step_id,
	image,
	runtime_config,
	config,
	name)
values ('84225863-da7a-4b44-bb6a-bd3e3f801683', '148fd0b6-f51f-4a95-b904-41a59dfcb62b', '84225863-da7a-4b44-bb6a-bd3e3f801682',
'',
'{
    "max_retries": 3
}',
'{
    "columns": {
    "ym": {
        "json_path": ".column0",
        "cast_to": "datetime",
        "date_format": "%Y%m"
    },
    "import_export": {
        "json_path": ".column1",
        "cast_to": "int"
    },
    "hs_code": {
        "json_path": ".column2",
        "cast_to": "int"
    },
    "customs": {
        "json_path": ".column3",
        "cast_to": "int"
    },
    "country": {
        "json_path": ".column4",
        "cast_to": "str"
    },
    "q1": {
        "json_path": ".column5",
        "cast_to": "int"
    },
    "q2": {
        "json_path": ".column6",
        "cast_to": "int"
    },
    "value": {
        "json_path": ".column7",
        "cast_to": "int"
    }
    }
}',
'extract');

insert
	into
	steps (step_id, pipeline_id,
	input_step_id,
	image,
	runtime_config,
	config,
	name)
values ('84225863-da7a-4b44-bb6a-bd3e3f801684', '148fd0b6-f51f-4a95-b904-41a59dfcb62b', '84225863-da7a-4b44-bb6a-bd3e3f801683',
'',
'{
    "max_retries": 3
}',
'{
  "user": "postgres",
  "password": "postgres",
  "host": "localhost",
  "port": 5432,
  "db_name": "coordinator_test_db",
  "table": "japan_trade_stats_from_1988_to_2019",
  "columns": [
    "ym",
    "import_export",
    "hs_code",
    "customs",
    "country",
    "q1",
    "q2",
    "value"
  ]
}',
'sink');


CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE japan_trade_stats_from_1988_to_2019 (
	ym timestamptz, 
	import_export smallint, 
	hs_code integer, 
	customs integer, 
	country char(9),
	q1 bigint,
	q2 bigint, 
	value bigint
);

SELECT create_hypertable(
	'japan_trade_stats_from_1988_to_2019',
	'ym', 
	chunk_time_interval => INTERVAL '1 month'
);