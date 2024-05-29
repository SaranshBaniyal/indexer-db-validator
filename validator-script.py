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

# module_table_query = {
#     "balances": """SELECT *
#                     FROM balances
#                     ORDER BY address, denom;""",
#     "denom_metatdata": """SELECT *
#                     FROM denom_metatdata
#                     ORDER BY denom, decimal;""",
#     "staked": """SELECT *
#                     FROM staked
#                     ORDER BY address, validator_address;""",
#     "unstaking": """SELECT *
#                     FROM unstaking
#                     ORDER BY address, validator_address;""",
#     "validator_metadata": """SELECT *
#                     FROM validator_metadata
#                     ORDER BY validator_address;"""
# }

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


def check_lists(py_list, rs_list, pk, table_name, py_db_name, rs_db_name):
#to check and log inconsistencies in list of dict returned by psycopg2
#pk should be a list of keys which form primary key (in order in which the lists are sorted)
    i=0
    j=0
    while i<len(py_list) and j<len(rs_list):
        py_pk=""
        rs_pk=""
        for key in pk:
            py_pk=py_pk + str(py_list[i][key]) + ","
            rs_pk=rs_pk + str(rs_list[j][key]) + ","

        if py_pk < rs_pk:
            logger.error(f"Couldn't find data corresponding to {pk}: {py_pk} in {table_name} table of {rs_db_name}")
            i+=1
        elif py_pk > rs_pk:
            logger.error(f"Couldn't find data corresponding to {pk}: {rs_pk} in {table_name} table of {py_db_name}")
            j+=1
        else:
            for key in py_list[i]:
                if key != 'rowid' and key != 'msg_rowid' and py_list[i].get(key) != rs_list[j].get(key):
                    logger.error(f"Inconsistent {key} at {pk}: {py_pk} in {table_name} table")
            i+=1
            j+=1

    while i<len(py_list):
        logger.error(f"Couldn't find data corresponding to {pk}: {py_pk} in {table_name} table of {rs_db_name}")
        i+=1
    while j<len(rs_list):
        logger.error(f"Couldn't find data corresponding to {pk}: {rs_pk} in {table_name} table of {py_db_name}")
        j+=1

def main():
    ingespy = IndexerDatabase(db_name='indexerdb', user='postgres', password='postgres', host='localhost', port='5432')
    ingesrs = IndexerDatabase(db_name='indexerdb', user='postgres', password='postgres', host='localhost', port='5432')

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

            check_lists(py_blocks, rs_blocks, ["height"], "block_metadata", ingespy._PG_DB, ingesrs._PG_DB)

            # i=0
            # j=0
            # while i<len(py_blocks) and j<len(rs_blocks):
            #     if py_blocks[i]['height'] < rs_blocks[j]['height']:
            #         logger.error(f"Couldn't find block of height {py_blocks[i]['height']} in block_metadata table of {ingesrs._PG_DB}")
            #         i+=1
            #     elif py_blocks[i]['height'] > rs_blocks[j]['height']:
            #         logger.error(f"Couldn't find block of height {rs_blocks[j]['height']} in block_metadata table of {ingespy._PG_DB}")
            #         j+=1
            #     else:
            #         for key in py_blocks[i]:
            #             if key != 'rowid' and py_blocks[i].get(key) != rs_blocks[j].get(key):
            #                 logger.error(f"Inconsistent {key} at block height {py_blocks[i]['height']} in block_metadata")
            #         i+=1
            #         j+=1

            # while i<len(py_blocks):
            #     logger.error(f"Couldn't find block of height {py_blocks[i]['height']} in block_metadata table of {ingesrs._PG_DB}")
            #     i+=1
            # while j<len(rs_blocks):
            #     logger.error(f"Couldn't find block of height {rs_blocks[j]['height']} in block_metadata table of {ingespy._PG_DB}")
            #     j+=1

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

            check_lists(py_txns, rs_txns, ["height", "tx_index"], "txn_fact_table", ingespy._PG_DB, ingesrs._PG_DB)

            # i=0
            # j=0
            # while i<len(py_txns) and j<len(rs_txns):
            #     if (py_txns[i]['height'] < rs_txns[j]['height']) or (py_txns[i]['height'] == rs_txns[j]['height'] and py_txns[i]['tx_index'] < rs_txns[j]['tx_index']):
            #         logger.error(f"Couldn't find transaction with block height {py_txns[i]['height']} with tx_index {py_txns[i]['tx_index']} in txn_fact_table of {ingesrs._PG_DB}")
            #         i+=1
            #     elif (py_txns[i]['height'] > rs_txns[j]['height']) or (py_txns[i]['height'] == rs_txns[j]['height'] and py_txns[i]['tx_index'] > rs_txns[j]['tx_index']):
            #         logger.error(f"Couldn't find transaction with block height {rs_txns[j]['height']} with tx_index {rs_txns[j]['tx_index']} in txn_fact_table of {ingespy._PG_DB}")
            #         j+=1
            #     else:
            #         for key in py_txns[i]:
            #             if key != 'rowid' and py_txns[i].get(key) != rs_txns[j].get(key):
            #                 logger.error(f"Inconsistent {key} at block height {py_txns[i]['height']} and tx_index {py_txns[j]['tx_index']} in txn_fact_table")
            #         i+=1
            #         j+=1
            
            # while i<len(py_txns):
            #     logger.error(f"Couldn't find transaction with block height {py_txns[i]['height']} and tx_index {py_txns[i]['tx_index']} in txn_fact_table of {ingesrs._PG_DB}")
            #     i+=1
            # while j<len(rs_txns):
            #     logger.error(f"Couldn't find transaction with block height {rs_txns[j]['height']} and tx_index {rs_txns[j]['tx_index']} in txn_fact_table of {ingespy._PG_DB}")
            #     j+=1
            
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

                check_lists(py_msgs, rs_msgs, ["msg_index"], "msg_fact_table", ingespy._PG_DB, ingesrs._PG_DB)

                # i=0
                # j=0
                # while i<len(py_msgs) and j<len(rs_msgs):
                #     if py_msgs[i]['msg_index'] < rs_msgs[j]['msg_index']:
                #         logger.error(f"Couldn't find message with tx_hash {py_msgs[i]['tx_hash']} and msg_index {py_msgs[i]['msg_index']} in msg_fact_table of {ingesrs._PG_DB}")
                #         i+=1
                #     elif py_msgs[i]['msg_index'] > rs_msgs[j]['msg_index']:
                #         logger.error(f"Couldn't find message with tx_hash {rs_msgs[j]['tx_hash']} and msg_index {rs_msgs[j]['msg_index']} in msg_fact_table of {ingespy._PG_DB}")
                #         j+=1
                #     else:
                #         for key in py_msgs[i]:
                #             if key != 'msg_rowid' and py_msgs[i].get(key) != rs_msgs[j].get(key):
                #                 logger.error(f"Inconsistent {key} for tx_hash {py_msgs[i]['tx_hash']} and msg_index{py_msgs[i]['msg_index']} in msg_fact_table")
                #         i+=1
                #         j+=1

                # while i<len(py_msgs):
                #     logger.error(f"Couldn't find message with tx_hash {py_msgs[i]['tx_hash']} and msg_index {py_msgs[i]['msg_index']} in msg_fact_table of {ingesrs._PG_DB}")
                #     i+=1
                # while j<len(rs_msgs):
                #     logger.error(f"Couldn't find message with tx_hash {rs_msgs[j]['tx_hash']} and msg_index {rs_msgs[j]['msg_index']} in msg_fact_table of {ingespy._PG_DB}")
                #     j+=1
                
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

                check_lists(py_msg_body, rs_msg_body, ["height", "hash", "index"], msg_type, ingespy._PG_DB, ingesrs._PG_DB)

                # i=0
                # j=0
                # while i<len(py_msg_body) and j<len(rs_msg_body):
                #     if (py_msg_body[i]['height'] < rs_msg_body[j]['height']) or \
                #         (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] < rs_msg_body[j]['hash']) or \
                #         (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] == rs_msg_body[j]['hash'] and py_msg_body[i]['index'] < rs_msg_body[j]['index']): 
                #         logger.error(f"Couldn't find message entry with block height {py_msg_body[i]['height']}, tx_hash {py_msg_body[i]['hash']} and msg_index {py_msg_body[i]['index']} in {msg_type} table of {ingesrs._PG_DB}")
                #         i+=1

                #     elif (py_msg_body[i]['height'] > rs_msg_body[j]['height']) or \
                #         (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] > rs_msg_body[j]['hash']) or \
                #         (py_msg_body[i]['height'] == rs_msg_body[j]['height'] and py_msg_body[i]['hash'] == rs_msg_body[j]['hash'] and py_msg_body[i]['index'] > rs_msg_body[j]['index']):
                #         logger.error(f"Couldn't find message entry with block height {rs_msg_body[j]['height']}, tx_hash {rs_msg_body[j]['hash']} and msg_index {rs_msg_body[j]['index']} in {msg_type} table of {ingespy._PG_DB}")
                #         j+=1

                #     else:
                #         for key in py_msg_body[i]:
                #             if key != 'rowid' and py_msg_body[i].get(key) != rs_msg_body[j].get(key):
                #                 logger.error(f"Inconsistent {key} at block height {py_msg_body[i]['height']}, tx_hash {py_msg_body[i]['hash']} and tx_index {py_txns[j]['index']} in {msg_type} table")
                #         i+=1
                #         j+=1

                # while i<len(py_msg_body):
                #     logger.error(f"Couldn't find message entry with block height {py_msg_body[i]['height']}, tx_hash {py_msg_body[i]['hash']} and msg_index {py_msg_body[i]['index']} in {msg_type} table of {ingesrs._PG_DB}")
                #     i+=1
                # while j<len(rs_msg_body):
                #     logger.error(f"Couldn't find message entry with block height {rs_msg_body[j]['height']}, tx_hash {rs_msg_body[j]['hash']} and msg_index {rs_msg_body[j]['index']} in {msg_type} table of {ingespy._PG_DB}")
                #     j+=1




            #for checking state tables of different modules at the latest height
            # py_cursor.execute("""
            #                 SELECT *
            #                 FROM module_state
            #                 ORDER BY module_name;
            #                 """)
            # py_module_state = py_cursor.fetchall()
            # rs_cursor.execute("""
            #                 SELECT *
            #                 FROM module_state
            #                 ORDER BY module_name;
            #                 """)
            # rs_module_state = rs_cursor.fetchall()

            # i=0
            # j=0
            
            # while i<len(py_module_state) and j<len(rs_module_state):
            #     if py_module_state[i]['module_name'] == rs_module_state[j]['module_name']:
            #         if py_module_state[i]['last_update_height'] == rs_module_state[j]['last_update_height']:
            #             if py_module_state[i]['module_name']=="balances":
            #                 pass
            #             elif py_module_state[i]['module_name']=="denom_metadata":
            #                 pass
            #             elif py_module_state[i]['module_name']=="staked":
            #                 pass
            #             elif py_module_state[i]['module_name']=="unstaking":
            #                 pass
            #             elif py_module_state[i]['module_name']=="validator_metadata":
            #                 pass
            #         else:
            #             logger.error(f"Inconsistent last_update_height for {py_module_state[i]['module_name']}: {ingespy._PG_DB}: {py_module_state[i]['last_update_height']}, {ingesrs._PG_DB}: {rs_module_state[j]['last_update_height']}")
            #         i+=1
            #         j+=1

            #     elif py_module_state[i]['module_name'] < rs_module_state[j]['module_name']:
            #         logger.error(f"Couldn't find {py_module_state[i]['module_name']} in module_state table of {ingesrs._PG_DB}")
            #         i+=1


            #     elif py_module_state[i]['module_name'] > rs_module_state[j]['module_name']:
            #         logger.error(f"Couldn't find {rs_module_state[j]['module_name']} in module_state table of {ingespy._PG_DB}")
            #         j+=1

            # while i<len(py_module_state):
            #     logger.error(f"Couldn't find {py_module_state[i]['module_name']} in module_state table of {ingesrs._PG_DB}")
            #     i+=1
            # while j<len(rs_module_state):
            #     logger.error(f"Couldn't find {rs_module_state[j]['module_name']} in module_state table of {ingespy._PG_DB}")
            #     j+=1



            # py_cursor.execute("""
            #                 SELECT *
            #                 FROM account_txns
            #                 ORDER BY address;
            #                 """)
            # py_account_txns = py_cursor.fetchall()
            # rs_cursor.execute("""
            #                 SELECT *
            #                 FROM account_txns
            #                 ORDER BY address;
            #                 """)
            # rs_account_txns = rs_cursor.fetchall()

            # i=0
            # j=0

            # while i<len(py_account_txns) and j<len(rs_account_txns):
            #     if py_account_txns[i]['address'] < rs_account_txns[j]['address']:
            #         logger.error(f"Couldn't find the address {py_account_txns[i]['address']} in account_txns table of {ingesrs._PG_DB}")
            #         i+=1
            #     elif py_account_txns[i]['address'] > rs_account_txns[j]['account']:
            #         logger.error(f"Couldn't find the address {rs_account_txns[j]['account']} in account_txns table of {ingespy._PG_DB}")
            #         j+=1
            #     else:
            #         for key in py_account_txns[i]:
            #             if key != 'rowid' and py_account_txns[i].get(key) != rs_account_txns[j].get(key):
            #                 logger.error(f"Inconsistent {key} for address {py_account_txns[i]['address']} in account_txns table")
            #         i+=1
            #         j+=1

            # while i<len(py_account_txns):
            #     logger.error(f"Couldn't find the address {py_account_txns[i]['address']} in account_txns table of {ingesrs._PG_DB}")
            #     i+=1
            # while j<len(rs_account_txns):
            #     logger.error(f"Couldn't find the address {rs_account_txns[j]['account']} in account_txns table of {ingespy._PG_DB}")
            #     j+=1

            logger.info("Validator script finished!")

if __name__ == "__main__":
    main()