-- Find storage integration
SHOW INTEGRATIONS;


CREATE OR REPLACE FILE FORMAT json_gzip_format
  TYPE = 'JSON'
  COMPRESSION = 'GZIP'
  STRIP_OUTER_ARRAY = FALSE
  STRIP_NULL_VALUES = FALSE
  REPLACE_INVALID_CHARACTERS = TRUE
  DATE_FORMAT = 'AUTO'
  TIME_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';


CREATE OR REPLACE STAGE s3_raw_events_stage
  STORAGE_INTEGRATION = ECOMMERCE_S3_INTEGRATION
  URL = 's3://ecommerce-streaming-data-0001/raw-events/'
  FILE_FORMAT = json_gzip_format;


CREATE OR REPLACE TABLE RAW_EVENTS (
  event_data VARIANT,                    
  source_file VARCHAR(500),              
  file_row_number NUMBER,                
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  
  
  CONSTRAINT unique_event PRIMARY KEY (source_file, file_row_number)
);
