CREATE TABLE datagen (
f_sequence INT,
sec_code INT,
test_number DECIMAL(6,3),
f_random_str STRING,
price INT,
ts AS localtimestamp,
WATERMARK FOR ts AS ts
) WITH (
'connector' = 'datagen',
'rows-per-second'='1000',
'fields.f_sequence.kind'='sequence',
'fields.f_sequence.start'='1',
'fields.f_sequence.end'='1000000',
'fields.sec_code.min'='100000',
'fields.sec_code.max'='100100',
'fields.test_number.kind'='random',
'fields.f_random_str.length'='4',
'fields.price.min'='1',
'fields.price.max'='10');

CREATE VIEW temp_view as select
sec_code,
TUMBLE_END(ts, INTERVAL '2' SECOND) AS window_end,
sum(price),
count(1)
from datagen
group by sec_code,TUMBLE(ts,INTERVAL '2' SECOND);

CREATE TABLE print_table (
sec_code INT,
end_timestamp TIMESTAMP,
sum_price INT,
cnt BIGINT
) WITH (
'connector' = 'kafka',
'topic' = 'test-agg',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'testGroup',
'scan.startup.mode' = 'earliest-offset',
'format' = 'csv'
);

insert into print_table select
*
from temp_view;