# import psycopg2
# from psycopg2.extras import execute_values
# import json
# import logging
# from datetime import datetime

# class LocalStorage:
#     def __init__(self, db_params):
#         self.db_params = db_params
#         self.buffer = []
#         self.buffer_limit = 30  # Batch 30 seconds to protect RPi storage
#         self._init_db()

#     def _get_connection(self):
#         return psycopg2.connect(**self.db_params)

#     def _init_db(self):
#         try:
#             with self._get_connection() as conn:
#                 with conn.cursor() as cur:
#                     # Flexible schema for all device types
#                     cur.execute("""
#                         CREATE TABLE IF NOT EXISTS device_logs (
#                             timestamp TIMESTAMPTZ NOT NULL,
#                             device_id INTEGER NOT NULL,
#                             data JSONB NOT NULL
#                         );
#                     """)
#                     # TimescaleDB hypertable for per-second efficiency
#                     cur.execute("""
#                         SELECT create_hypertable('device_logs', 'timestamp', 
#                         if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');
#                     """)
#                     # 90-day retention policy
#                     cur.execute("""
#                         SELECT add_retention_policy('device_logs', INTERVAL '90 days', 
#                         if_not_exists => TRUE);
#                     """)
#                     conn.commit()
#         except Exception as e:
#             logging.error(f"PostgreSQL Init Error: {e}")

#     def save_reading(self, device_id, data_payload):
#         # Capture the exact moment of reading
#         capture_time = datetime.now()
#         data_payload.pop('type', None) # Remove metadata
        
#         self.buffer.append((
#             capture_time,
#             device_id,
#             json.dumps(data_payload)
#         ))

#         if len(self.buffer) >= self.buffer_limit:
#             self.flush()

#     def flush(self):
#         if not self.buffer: return
#         try:
#             with self._get_connection() as conn:
#                 with conn.cursor() as cur:
#                     execute_values(cur, 
#                         "INSERT INTO device_logs (timestamp, device_id, data) VALUES %s", 
#                         self.buffer
#                     )
#                     conn.commit()
#             self.buffer = []
#         except Exception as e:
#             logging.error(f"PostgreSQL Flush Error: {e}")


import psycopg2
from psycopg2.extras import execute_values
import json
import logging
from datetime import datetime

class LocalStorage:
    def __init__(self, db_params):
        self.db_params = db_params
        self.buffer = []
        self.buffer_limit = 30 # Writes to SD card every 30 readings
        self._init_db()

    def _get_connection(self):
        return psycopg2.connect(**self.db_params)

    def _init_db(self):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Flexible schema for any device type
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS device_logs (
                            timestamp TIMESTAMPTZ NOT NULL,
                            device_id INTEGER NOT NULL,
                            data JSONB NOT NULL
                        );
                    """)
                    # Hypertable for high-frequency data performance
                    cur.execute("""
                        SELECT create_hypertable('device_logs', 'timestamp', 
                        if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');
                    """)
                    # 90-day retention policy to manage RPi storage
                    cur.execute("""
                        SELECT add_retention_policy('device_logs', INTERVAL '90 days', 
                        if_not_exists => TRUE);
                    """)
                    conn.commit()
        except Exception as e:
            logging.error(f"PostgreSQL Init Error: {e}")

    def save_device_data(self, device_id, data_dict):
        """Captures reading with timestamp and buffers it."""
        capture_time = datetime.now() # Site-time capture
        data_dict.pop('type', None) # Remove metadata
        
        self.buffer.append((capture_time, device_id, json.dumps(data_dict)))

        if len(self.buffer) >= self.buffer_limit:
            self.flush()

    def flush(self):
        if not self.buffer: return
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, 
                        "INSERT INTO device_logs (timestamp, device_id, data) VALUES %s", 
                        self.buffer)
                    conn.commit()
            self.buffer = []
        except Exception as e:
            logging.error(f"PostgreSQL Flush Error: {e}")