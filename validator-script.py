import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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

def main():
    ingespy = IndexerDatabase(db_name='indexerdb', user='postgres', password='postgres', host='45.250.253.23', port='5432')
    ingesrs = IndexerDatabase(db_name='indexerdb', user='postgres', password='postgres', host='45.250.253.23', port='5432')

    logger.info("Starting validator script!")

    #compares data of ingespy against ingesrs
    with ingespy.conn as py_conn, ingesrs.conn as rs_conn:
        with py_conn.cursor(cursor_factory=RealDictCursor) as py_cursor, rs_conn.cursor(cursor_factory=RealDictCursor) as rs_cursor:
            py_cursor.execute("""
                                SELECT *
                                FROM block_metadata
                                ORDER BY height;
                                """)
            py_blocks = py_cursor.fetchall()
            rs_cursor.execute("""
                                SELECT *
                                FROM block_metadata
                                ORDER BY height;
                                """)
            rs_blocks = rs_cursor.fetchall()

            i=0
            j=0
            while i<len(py_blocks) and j<len(rs_blocks):
                if py_blocks[i]['height'] < rs_blocks[j]['height']:
                    logger.error(f"Couldn't find block of height {py_blocks[i]['height']} in block_metadata table of {ingesrs._PG_DB}")
                    i+=1
                elif py_blocks[i]['height'] > rs_blocks[j]['height']:
                    logger.error(f"Couldn't find block of height {rs_blocks[j]['height']} in block_metadata table of {ingespy._PG_DB}")
                    j+=1
                else:
                    for key in py_blocks[i]:
                        if key != 'rowid' and py_blocks[i].get(key) != rs_blocks[j].get(key):
                            logger.error(f"Inconsistent {key} at block height {py_blocks[i]['height']} in block_metadata")
                    i+=1
                    j+=1

            while i<len(py_blocks):
                logger.error(f"Couldn't find block of height {py_blocks[i]['height']} in block_metadata table of {ingesrs._PG_DB}")
                i+=1
            while j<len(rs_blocks):
                logger.error(f"Couldn't find block of height {rs_blocks[j]['height']} in block_metadata table of {ingespy._PG_DB}")
                j+=1

            py_cursor.execute("""
                                SELECT *
                                FROM txn_fact_table
                                ORDER BY height, tx_index;
                                """)
            py_txns = py_cursor.fetchall()
            rs_cursor.execute("""
                                SELECT *
                                FROM txn_fact_table
                                ORDER BY height, tx_index;
                                """)
            rs_txns = rs_cursor.fetchall()

            i=0
            j=0
            while i<len(py_txns) and j<len(rs_txns):
                if (py_txns[i]['height'] < rs_txns[j]['height']) or (py_txns[i]['height'] == rs_txns[j]['height'] and py_txns[i]['tx_index'] < rs_txns[j]['tx_index']):
                    logger.error(f"Couldn't find transaction with block height {py_txns[i]['height']} with tx_index {py_txns[i]['tx_index']} in txn_fact_table of {ingesrs._PG_DB}")
                    i+=1
                elif (py_txns[i]['height'] > rs_txns[j]['height']) or (py_txns[i]['height'] == rs_txns[j]['height'] and py_txns[i]['tx_index'] > rs_txns[j]['tx_index']):
                    logger.error(f"Couldn't find transaction with block height {rs_txns[j]['height']} with tx_index {rs_txns[j]['tx_index']} in txn_fact_table of {ingespy._PG_DB}")
                    j+=1
                else:
                    for key in py_txns[i]:
                        if key != 'rowid' and py_txns[i].get(key) != rs_txns[j].get(key):
                            logger.error(f"Inconsistent {key} at block height {py_txns[i]['height']} and tx_index {py_txns[j]['tx_index']} in txn_fact_table")
                    i+=1
                    j+=1
            
            while i<len(py_txns):
                logger.error(f"Couldn't find transaction with block height {py_txns[i]['height']} and tx_index {py_txns[i]['tx_index']} in txn_fact_table of {ingesrs._PG_DB}")
                i+=1
            while j<len(rs_txns):
                logger.error(f"Couldn't find transaction with block height {rs_txns[j]['height']} and tx_index {rs_txns[j]['tx_index']} in txn_fact_table of {ingespy._PG_DB}")
                j+=1
            
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

                i=0
                j=0
                while i<len(py_msgs) and j<len(rs_msgs):
                    if py_msgs[i]['msg_index'] < rs_msgs[j]['msg_index']:
                        logger.error(f"Couldn't find message with tx_hash {py_msgs[i]['tx_hash']} and msg_index {py_msgs[i]['msg_index']} in msg_fact_table of {ingesrs._PG_DB}")
                        i+=1
                    elif py_msgs[i]['msg_index'] > rs_msgs[j]['msg_index']:
                        logger.error(f"Couldn't find message with tx_hash {rs_msgs[j]['tx_hash']} and msg_index {rs_msgs[j]['msg_index']} in msg_fact_table of {ingespy._PG_DB}")
                        j+=1
                    else:
                        for key in py_msgs[i]:
                            if key != 'msg_rowid' and py_msgs[i].get(key) != rs_msgs[j].get(key):
                                logger.error(f"Inconsistent {key} for tx_hash {py_msgs[i]['tx_hash']} and msg_index{py_msgs[i]['msg_index']} in msg_fact_table")
                        i+=1
                        j+=1

                while i<len(py_msgs):
                    logger.error(f"Couldn't find message with tx_hash {py_msgs[i]['tx_hash']} and msg_index {py_msgs[i]['msg_index']} in msg_fact_table of {ingesrs._PG_DB}")
                    i+=1
                while j<len(rs_msgs):
                    logger.error(f"Couldn't find message with tx_hash {rs_msgs[j]['tx_hash']} and msg_index {rs_msgs[j]['msg_index']} in msg_fact_table of {ingespy._PG_DB}")
                    j+=1
                
            for msg_type in tx_msg_type:
                py_cursor.execute(f"""
                                SELECT *
                                FROM {msg_type}
                                ORDER BY height, hash, index;
                                """)
                py_msg_body = py_cursor.fetchall()
                rs_cursor.execute(f"""
                                    SELECT *
                                    FROM {msg_type}
                                    ORDER BY height, hash, index;
                                    """)
                rs_msg_body = rs_cursor.fetchall()

                i=0
                j=0
                while i<len(py_msg_body) and j<len(rs_msg_body):
                    if (py_msg_body[i]['height'] < rs_msg_body[j]['height']) or \
                        (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] < rs_msg_body[j]['hash']) or \
                        (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] == rs_msg_body[j]['hash'] and py_msg_body[i]['index'] < rs_msg_body[j]['index']): 
                        logger.error(f"Couldn't find message entry with block height {py_msg_body[i]['height']}, tx_hash {py_msg_body[i]['hash']} and msg_index {py_msg_body[i]['index']} in {msg_type} table of {ingesrs._PG_DB}")
                        i+=1

                    elif (py_msg_body[i]['height'] > rs_msg_body[j]['height']) or \
                        (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] > rs_msg_body[j]['hash']) or \
                        (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] == rs_msg_body[j]['hash'] and py_msg_body[i]['index'] > rs_msg_body[j]['index']):
                        logger.error(f"Couldn't find message entry with block height {rs_msg_body[j]['height']}, tx_hash {rs_msg_body[j]['hash']} and msg_index {rs_msg_body[j]['index']} in {msg_type} table of {ingespy._PG_DB}")
                        j+=1

                    else:
                        for key in py_msg_body[i]:
                            if key != 'rowid' and py_msg_body[i].get(key) != rs_msg_body[j].get(key):
                                logger.error(f"Inconsistent {key} at block height {py_msg_body[i]['height']}, tx_hash {py_msg_body[i]['hash']} and tx_index {py_txns[j]['index']} in {msg_type} table")
                        i+=1
                        j+=1

                while i<len(py_msg_body):
                    logger.error(f"Couldn't find message entry with block height {py_msg_body[i]['height']}, tx_hash {py_msg_body[i]['hash']} and msg_index {py_msg_body[i]['index']} in {msg_type} table of {ingesrs._PG_DB}")
                    i+=1
                while j<len(rs_msg_body):
                    logger.error(f"Couldn't find message entry with block height {rs_msg_body[j]['height']}, tx_hash {rs_msg_body[j]['hash']} and msg_index {rs_msg_body[j]['index']} in {msg_type} table of {ingespy._PG_DB}")
                    j+=1
            
            logger.info("Validator script finished!")

if __name__ == "__main__":
    main()