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

def compare_dict_lists(a, b, ignore_key=None):
    if len(a) != len(b):
        raise ValueError("Lists a and b must have the same length.")
    
    for dict_a, dict_b in zip(a, b):
        for key in dict_a:
            if key != ignore_key and dict_a.get(key) != dict_b.get(key):
                return dict_a
    return None

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
                                SELECT *
                                FROM block_metadata
                                WHERE height BETWEEN %s AND %s
                                ORDER BY height;
                                """, (start_height, end_height))
            py_blocks = py_cursor.fetchall()
            rs_cursor.execute("""
                                SELECT *
                                FROM block_metadata
                                WHERE height BETWEEN %s AND %s
                                ORDER BY height;
                                """, (start_height, end_height))
            rs_blocks = rs_cursor.fetchall()

            result = compare_dict_lists(py_blocks, rs_blocks, "rowid")
            if result:
                logger.error(f"Inconsistent block data at height {result['height']}")
                result=None
                return False

            py_cursor.execute("""
                                SELECT *
                                FROM txn_fact_table
                                WHERE height BETWEEN %s AND %s
                                ORDER BY height, tx_index;
                                """, (start_height, end_height))
            py_txns = py_cursor.fetchall()
            rs_cursor.execute("""
                                SELECT *
                                FROM txn_fact_table
                                WHERE height BETWEEN %s AND %s
                                ORDER BY height, tx_index;
                                """, (start_height, end_height))
            rs_txns = rs_cursor.fetchall()
            result = compare_dict_lists(py_txns, rs_txns, "rowid")
            if result:
                logger.error(f"Inconsistent transaction data at height {result['height']} and tx_hash {result['tx_hash']}")
                result = None
                return False
            
            for tx in py_txns:
                py_cursor.execute("""
                                    SELECT *
                                    FROM msg_fact_table
                                    WHERE tx_hash = %s
                                    ORDER BY msg_index;
                                    """, (tx["tx_hash"],))
                py_msgs = py_cursor.fetchall()
                rs_cursor.execute("""
                                    SELECT *
                                    FROM msg_fact_table
                                    WHERE tx_hash = %s
                                    ORDER BY msg_index;
                                    """, (tx["tx_hash"],))
                rs_msgs = rs_cursor.fetchall()

                result = compare_dict_lists(py_msgs, rs_msgs, "rowid")
                if result:
                    logger.error(f"Inconsistent transaction message at height {tx['height']}, "
                                 f"tx_hash {result['tx_hash']}, msg_index {result['msg_index']} and msg_type {result['msg_type']}")
                    result = None
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

                result = compare_dict_lists(py_msg_body, rs_msg_body, "rowid")
                if result:
                    logger.error(f"Inconsistent transaction message body at height {tx['height']}, "
                                 f"tx_hash {result['tx_hash']}, msg_index {result['msg_index']}, msg_type {result['msg_type']} and"
                                 f"msg_index {result['index']}")
                    result = None
                    return False
            
            return True

if __name__ == "__main__":
    print(main(19906800, 19906809))