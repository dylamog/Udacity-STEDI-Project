CREATE EXTERNAL TABLE IF NOT EXISTS `databased`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsofDate` bigint,
  `shareWithPublicAsofDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://dingus-dongus/customer/landing/'
TBLPROPERTIES ('classification' = 'json');