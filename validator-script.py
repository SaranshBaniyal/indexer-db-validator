import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()

tx_msg_type = [
    'msggrant',
    'msgexec',
    'msgrevoke',
    'msgsend',
    'msgmultisend',
    'msgverifyinvariant',
    'msgsetwithdrawaddress',
    'msgwithdrawdelegatorreward',
    'msgwithdrawvalidatorcommission',
    'msgfundcommunitypool',
    'msgsubmitevidence',
    'msggrantallowance',
    'msgrevokeallowance',
    'msgsubmitproposal',
    'msgvoteproposal',
    'msgdepositproposal',
    'msgvoteweightedproposal',
    'msgunjail',
    'msgdelegate',
    'msgbeginredelegate',
    'msgundelegate',
    'msgcancelunbondingdelegation'
]

class IndexerDatabase:
    def __init__(self, db_name, user, password, host, port):

        self._PG_DB = db_name
        self._PG_USER = user
        self._PG_PW = password
        self._PG_HOST = host
        self._PG_PORT = port

        try:
            self.conn = psycopg2.connect(database=self._PG_DB,
                                                user=self._PG_USER,
                                                password=self._PG_PW,
                                                host=self._PG_HOST,
                                                port=self._PG_PORT)
            logger.info(f"Successfully connected to {db_name}")
        except psycopg2.OperationalError as e:
            logger.error(f"Couldn't connect to {db_name}: {e}")

def main(start_height, end_height):
    ingespy = IndexerDatabase(db_name='indexerdb', user='postgres', password='postgres', host='localhost', port='5432')
    ingesrs = IndexerDatabase(db_name='indexerdb', user='postgres', password='postgres', host='localhost', port='5432')

    with ingespy.conn as py_conn, ingesrs.conn as rs_conn:
        with py_conn.cursor(cursor_factory=RealDictCursor) as py_cursor, rs_conn.cursor(cursor_factory=RealDictCursor) as rs_cursor:
            py_cursor.execute("""
                                SELECT height, proposer, hash, block_time, txcount, chain_id, total_gas_wanted, total_gas_used, metadata
                                FROM block_metadata
                                WHERE height BETWEEN %s AND %s ;
                                """, (start_height, end_height))
            py_blocks = py_cursor.fetchall()
            rs_cursor.execute("""
                                SELECT height, proposer, hash, block_time, txcount, chain_id, total_gas_wanted, total_gas_used, metadata
                                FROM block_metadata
                                WHERE height BETWEEN %s AND %s ;
                                """, (start_height, end_height))
            rs_blocks = rs_cursor.fetchall()
            # TODO: exclude rowid maybe
            if(py_blocks != rs_blocks):
                logger.error(f"Inconsistent block data")
                return False

            py_cursor.execute("""
                                SELECT height, tx_hash, tx_index, tx_time, code, log, info, gas_wanted, gas_used, fees, codespace
                                FROM txn_fact_table
                                WHERE height BETWEEN %s AND %s
                                ORDER BY height, tx_index;
                                """, (start_height, end_height))
            py_txns = py_cursor.fetchall()
            rs_cursor.execute("""
                                SELECT height, tx_hash, tx_index, tx_time, code, log, info, gas_wanted, gas_used, fees, codespace
                                FROM txn_fact_table
                                WHERE height BETWEEN %s AND %s
                                ORDER BY height, tx_index;
                                """, (start_height, end_height))
            rs_txns = rs_cursor.fetchall()
            if(py_txns != rs_txns):
                logger.error(f"Inconsistent transaction data")
                return False
            
            for tx in py_txns:
                py_cursor.execute("""
                                    SELECT tx_hash, msg_index, msg_type
                                    FROM msg_fact_table
                                    WHERE tx_hash = %s
                                    ORDER BY msg_index;
                                    """, (tx["tx_hash"],))
                py_msgs = py_cursor.fetchall()
                rs_cursor.execute("""
                                    SELECT tx_hash, msg_index, msg_type
                                    FROM msg_fact_table
                                    WHERE tx_hash = %s
                                    ORDER BY msg_index;
                                    """, (tx["tx_hash"],))
                rs_msgs = rs_cursor.fetchall()

                if(py_msgs != rs_msgs):
                    logger.error(f"Inconsistent transaction message data")
                    return False
                
            for msg_type in tx_msg_type:
                py_cursor.execute(f"""
                                SELECT *
                                FROM {msg_type}
                                WHERE height BETWEEN {start_height} AND {end_height}
                                ORDER BY height, hash, index;
                                """, (start_height, end_height))
                py_msg_body = py_cursor.fetchall()
                rs_cursor.execute(f"""
                                    SELECT *
                                    FROM {msg_type}
                                    WHERE height BETWEEN {start_height} AND {end_height}
                                    ORDER BY height, hash, index;
                                    """, (start_height, end_height))
                rs_msg_body = rs_cursor.fetchall()

                if(py_msg_body != rs_msg_body):
                    logger.error(f"Inconsistent transaction message body data")
                    return False
            
            return True

if __name__ == "__main__":
    print(main(19906800, 19906809))