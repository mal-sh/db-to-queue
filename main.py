import os
import time
import logging
import psycopg2
from psycopg2.extras import DictCursor
import redis
from typing import Generator, Optional
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/app.log')
    ]
)
logger = logging.getLogger(__name__)

class GracefulShutdown:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    
    def exit_gracefully(self, signum, frame):
        logger.info(f"Received shutdown signal {signum}")
        self.shutdown = True

class DatabaseReader:
    def __init__(self):
        self.db_config = {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
        self.table_name = os.getenv('POSTGRES_TABLE')
        self.column_name = os.getenv('POSTGRES_COLUMN')
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        
    def get_connection(self):
        """Create a new database connection"""
        return psycopg2.connect(**self.db_config, cursor_factory=DictCursor)
    
    def read_rows(self, last_id: int = 0) -> Generator[list, None, None]:
        """Read rows in batches using cursor-based pagination"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                while True:
                    query = f"""
                    SELECT id, {self.column_name} 
                    FROM {self.table_name} 
                    WHERE id > %s 
                    ORDER BY id 
                    LIMIT %s
                    """
                    cursor.execute(query, (last_id, self.batch_size))
                    rows = cursor.fetchall()
                    
                    if not rows:
                        break
                    
                    yield rows
                    last_id = rows[-1]['id']
                    
        finally:
            conn.close()

class RedisQueue:
    def __init__(self):
        self.redis_config = {
            'host': os.getenv('REDIS_HOST'),
            'port': int(os.getenv('REDIS_PORT', '6379')),
            'password': os.getenv('REDIS_PASSWORD', None),
            'db': int(os.getenv('REDIS_DB', '0')),
            'decode_responses': True
        }
        self.queue_name = os.getenv('REDIS_QUEUE_NAME', 'default_queue')
        self.connection_pool = None
        
    def get_connection(self):
        """Get Redis connection from pool"""
        if not self.connection_pool:
            self.connection_pool = redis.ConnectionPool(**self.redis_config)
        
        return redis.Redis(connection_pool=self.connection_pool)
    
    def send_messages(self, messages: list) -> int:
        """Send batch of messages to Redis queue"""
        if not messages:
            return 0
            
        conn = self.get_connection()
        try:
            # Use pipeline for better performance
            pipe = conn.pipeline()
            for message in messages:
                pipe.rpush(self.queue_name, message)
            results = pipe.execute()
            return sum(results)
        except redis.RedisError as e:
            logger.error(f"Redis error: {e}")
            raise
        finally:
            conn.close()

class BatchProcessor:
    def __init__(self):
        self.db_reader = DatabaseReader()
        self.redis_queue = RedisQueue()
        self.shutdown_handler = GracefulShutdown()
        self.processed_count = 0
        self.batch_delay = float(os.getenv('BATCH_DELAY', '0.1'))
        
    def validate_env_variables(self):
        """Validate all required environment variables"""
        required_vars = [
            'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD',
            'POSTGRES_HOST', 'POSTGRES_TABLE', 'POSTGRES_COLUMN',
            'REDIS_HOST'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
    
    def process_batch(self, rows: list) -> int:
        """Process a batch of rows"""
        messages = [str(row[self.db_reader.column_name]) for row in rows]
        
        try:
            sent_count = self.redis_queue.send_messages(messages)
            self.processed_count += len(rows)
            logger.info(f"Processed batch: {len(rows)} rows, Total: {self.processed_count}")
            return sent_count
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            # Implement retry logic here if needed
            raise
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting batch processing...")
        self.validate_env_variables()
        
        last_processed_id = 0
        batch_count = 0
        
        try:
            for batch in self.db_reader.read_rows(last_processed_id):
                if self.shutdown_handler.shutdown:
                    logger.info("Shutdown requested, stopping processing")
                    break
                
                sent_count = self.process_batch(batch)
                last_processed_id = batch[-1]['id']
                batch_count += 1
                
                # Small delay to prevent overwhelming systems
                time.sleep(self.batch_delay)
                
                # Log progress every 100 batches
                if batch_count % 100 == 0:
                    logger.info(f"Progress: {batch_count} batches, {self.processed_count} rows processed")
                    
        except Exception as e:
            logger.error(f"Fatal error during processing: {e}")
            raise
        
        logger.info(f"Processing completed. Total rows processed: {self.processed_count}")

def main():
    processor = BatchProcessor()
    processor.run()

if __name__ == "__main__":
    main()
