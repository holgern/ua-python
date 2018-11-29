# -*- coding: utf-8 -*-
from beem.blockchain import Blockchain
from beem.block import Block
from beem.account import Account, Accounts
from beem.utils import formatTimeString, addTzInfo
from beem.nodelist import NodeList
from beemapi.exceptions import ApiNotSupported
from beembase.operationids import operations, getOperationNameForId
from beem import Steem
from beem.instance import set_shared_steem_instance, shared_steem_instance
import time
import pprint
from datetime import datetime, timedelta
import unicodedata
from steemua.block_ops_storage import BlockTrx, AccountTrx, FollowsTrx
from steemua.storage import LockDB
import dataset
import os
import json



if __name__ == "__main__":
    config_file = 'config_pg.json'
    if not os.path.isfile(config_file):
        block_diff_for_db_storage = 1000
        databaseConnector = "sqlite://db.sqlite"
    else:    
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        databaseConnector = config_data["databaseConnector"]
        if "block_diff_for_db_storage" in config_data:
            block_diff_for_db_storage = config_data["block_diff_for_db_storage"]
        else:
            block_diff_for_db_storage = 1000
        if "type_as_string" in config_data:
            type_as_string = config_data["type_as_string"]
        else:
            type_as_string = False
    print("stream new operations")
    db = dataset.connect(databaseConnector)
    
    blockTrxStorage = BlockTrx(db)
    accountTrxStorage = AccountTrx(db)
    followsTrxStorage = FollowsTrx(db)
    lockStorage = LockDB(db)
    
    use_mongodb = True
    if use_mongodb:
        print("build_ua_lock %s" % str(lockStorage.is_locked("build_ua_lock")))
        if lockStorage.is_locked("build_ua_lock"):
            print("stop_stream_block")
            exit()    
        print("top100_lock %s" % str(lockStorage.is_locked("top100_lock")))
        if lockStorage.is_locked("top100_lock"):
            print("stop_stream_block")
            exit()          
        print("read_lock %s" % str(lockStorage.is_locked("read_lock")))
        if lockStorage.is_locked("read_lock"):
            print("stop set_trusted_account")
            exit()        
    newBlockTrxStorage = False
    if not blockTrxStorage.exists_table():
        newBlockTrxStorage = True
        blockTrxStorage.create_table()

    newAccountsTrxStorage = False
    if not accountTrxStorage.exists_table():
        newAccountsTrxStorage = True
        accountTrxStorage.create_table()
    if accountTrxStorage.get_latest_index() is None:
        accountTrxStorage.add({"block_num": 0, "op_num": 0, "type": 14, "account_index": 0, "account_name": "miners", "timestamp": "1970-01-01 00:00:00"})
        accountTrxStorage.add({"block_num": 0, "op_num": 1, "type": 14, "account_index": 1, "account_name": "null", "timestamp": "1970-01-01 00:00:00"})
        accountTrxStorage.add({"block_num": 0, "op_num": 2, "type": 14, "account_index": 2, "account_name": "temp", "timestamp": "1970-01-01 00:00:00"})
        accountTrxStorage.add({"block_num": 0, "op_num": 3, "type": 14, "account_index": 3, "account_name": "initminer", "timestamp": "1970-01-01 00:00:00"})

    newFollowsTrxStorage = False
    if not followsTrxStorage.exists_table():
        newFollowsTrxStorage = True
        followsTrxStorage.create_table()
    stream_round = 0
    if False:
        max_batch_size = 50
        threading = False
        wss = False
        https = True
        normal = False
        appbase = True
    elif True:
        max_batch_size = None
        threading = False
        wss = False
        https = True
        normal = True
        appbase = True        
    else:
        max_batch_size = None
        threading = True
        wss = True
        https = False
        normal = True
        appbase = True
    data_available = True
    while data_available:
    
        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        nodes.update_nodes()
        node_list = nodes.get_nodes(normal=normal, appbase=appbase, wss=wss, https=https)
        stm = Steem(node=node_list, num_retries=5, call_num_retries=3, timeout=15)
        
        print(stm)
        print("Round: %d - %s" %(stream_round, str(stm)))
        
        set_shared_steem_instance(stm)
    
        b = Blockchain(steem_instance=stm)
        blocks_per_day = 20 * 60 * 24
        if newBlockTrxStorage:
            start_block = 1091
            trx_id_list = []
        else:
            start_block = blockTrxStorage.get_latest_block_num()
            trx_id_list = blockTrxStorage.get_block_trx_id(start_block)
        if start_block is None:
            start_block = 1091
            trx_id_list = []
        end_block = b.get_current_block_num()
        end_block_round = int(start_block) + blocks_per_day - 1

        cnt = 0
        sb = Block(start_block, steem_instance=stm)
        print("start: %d - %s" % (start_block, str(sb["timestamp"])))
        end_date = datetime(sb["timestamp"].year, sb["timestamp"].month, sb["timestamp"].day, 23, 59, 50) + timedelta(seconds = 60 * 60 * 24)
        if (addTzInfo(end_date) - sb["timestamp"]).total_seconds() / 60 / 60 / 24 > 1.1:
            end_date = datetime(sb["timestamp"].year, sb["timestamp"].month, sb["timestamp"].day, 23, 59, 50)
        #end_block_round = b.get_estimated_block_num(end_date) - 1
        print("end: %d - %s" % (end_block_round, str(end_date)))
        next_account_id = accountTrxStorage.get_latest_index() + 1
        
        if end_block_round > end_block:
            end_block_round = end_block
            data_available = False
        db_data = []
        db_account = []
        db_follow = []
        accounts = accountTrxStorage.get_accounts()
        last_block_num = None
        new_accounts_found = 0
        new_follow_found = 0
        last_trx_id = '0' * 40
        op_num = 0
        print("start_streaming")
        for op in b.stream(start=int(start_block), stop=int(end_block_round), opNames=["account_create", "account_create_with_delegation", "pow", "pow2", "custom_json"], max_batch_size=max_batch_size, threading=threading, thread_num=8):
            block_num = op["block_num"]
            # print(block_num)
            if last_block_num is None:
                new_accounts_found = 0
                new_follow_found = 0
                start_time = time.time()
                last_block_num = block_num
            if op["trx_id"] == last_trx_id:
                op_num += 1
            else:
                op_num = 0
            if "trx_num" in op:
                trx_num = op["trx_num"]
            else:
                trx_num = 0
            if type_as_string:
                type_id = op["type"]
            else:
                type_id = operations[op["type"]]
            data = {"block_num": block_num, "trx_id": op["trx_id"], "trx_num": trx_num, "op_num": op_num, "timestamp": formatTimeString(op["timestamp"]), "type": type_id}
            if op["trx_id"] in trx_id_list:
                continue
            if op["block_num"] > start_block:
                trx_id_list = []
            if op["type"] == "custom_json":
                if op["id"] != "follow":
                    continue
                # data["id"] = "follow"
                try:
                    json_field = json.loads(op["json"])
                except:
                    continue

                if isinstance(json_field, dict):
                    data["follower"] = json_field["follower"][:18]
                    data["following"] = json_field["following"][:18]
                    if len(json_field["what"]) > 0:
                        data["what"] = json_field["what"][0]
                    else:
                        data["what"] = ""
                    # no self follow
                    if data["follower"] != data["following"]:
                        new_follow_found += 1
                        db_data.append(data)
                elif isinstance(json_field, list):
                    json_type = json_field[0]
                    if isinstance(json_type, list):
                        for jf in json_field:
                            if type_as_string:
                                type_id = op["type"]
                            else:
                                type_id = operations[op["type"]]                            
                            data = {"block_num": block_num, "trx_id": op["trx_id"], "trx_num": trx_num, "op_num": op_num, "timestamp": formatTimeString(op["timestamp"]), "type": type_id}
                            op_num += 1
                            # data["id"] = "follow"
                            json_type = jf[0]
                            json_data = jf[1]  # only the first entry is processed
                            if json_type == "reblog":
                                continue
                            elif json_type == "follow":
                                if "following" not in json_data:
                                    continue
                                if "what" not in json_data:
                                    continue
                                if isinstance(json_data["following"], int):
                                    continue                                
                                data["follower"] = json_data["follower"][:18]
                                data["following"] = json_data["following"][:18]
                                if len(json_data["what"]) > 0 and len(json_data["what"][0]) > 20 :
                                    data["what"] = json_data["what"][0][:20]
                                elif len(json_data["what"]) > 0:
                                    data["what"] = json_data["what"][0]
                                else:
                                    data["what"] = ""
                                # no self follow
                                if data["follower"] != data["following"]:
                                    db_data.append(data)
                                    new_follow_found += 1
                            else:
                                print(json_field)
                                raise AssertionError("Unhandled json_type")
                    elif json_type == "reblog":
                        continue
                    elif json_type == "follow":
                        json_data = json_field[1]  # only the first entry is processed
                        if "following" not in json_data:
                            continue
                        if "what" not in json_data:
                            continue
                        if json_data["following"] is None:
                            continue
                        if isinstance(json_data["following"], int):
                            continue
                        data["follower"] = json_data["follower"][:18]
                        data["following"] = json_data["following"][:18]
                        if len(json_data["what"]) > 0 and json_data["what"][0] is not None and len(json_data["what"][0]) > 20:
                            data["what"] = json_data["what"][0][:20]
                        elif len(json_data["what"]) > 0 and json_data["what"][0] is not None:
                            data["what"] = json_data["what"][0]
                        else:
                            data["what"] = ""
                        if data["follower"] != data["following"]:
                            new_follow_found += 1
                            db_data.append(data)                    
                    else:
                        print(json_field)
                        raise AssertionError("Unhandled json_type")
                else:
                    print(json_field)
                
                # db_follow.append(data)
                    
            elif op["type"] == "pow" or  op["type"] == "pow2":
                if op["type"] == "pow":
                    account_name = op["worker_account"]
                else:
                    if isinstance(op["work"], dict):
                        account_name = op['work']["value"]['input']['worker_account']
                    else:
                        account_name = op['work'][1]['input']['worker_account']
                if account_name in accounts:
                    continue
                new_accounts_found += 1
                # data["id"] = "new_account"
                if type_as_string:
                    type_id = op["type"]
                else:
                    type_id = operations[op["type"]]                  
                account_data = {"block_num": block_num, "type": type_id, "account_index": next_account_id, "account_name": account_name}
                db_account.append(account_data)
                accounts[account_name] = next_account_id
                #if op["type"] == "pow2":
                #    print(account_data)
                data["new_account_name"] = account_name
                data["new_account_id"] = next_account_id
                next_account_id += 1
                db_data.append(data)
            elif op["type"] == "account_create" or op["type"] == "account_create_with_delegation":
                # data["id"] = "new_account"
                new_accounts_found += 1
                account_name = op["new_account_name"]
                if type_as_string:
                    type_id = op["type"]
                else:
                    type_id = operations[op["type"]]                    
                account_data = {"block_num": block_num, "type": type_id, "account_index": next_account_id, "account_name": account_name}
                db_account.append(account_data)
                accounts[account_name] = next_account_id     
                # print(account_data)
                data["new_account_name"] = account_name
                data["new_account_id"] = next_account_id
                next_account_id += 1
                db_data.append(data)
            else:
                print(op)
                continue
            
            last_trx_id = op["trx_id"]
            if (block_num - last_block_num) > block_diff_for_db_storage:
                
                
                account_index = len(db_data) - 1
                latest_account = db_data[-1]
                while account_index >= 0 and not ("new_account_name" in latest_account and latest_account["new_account_name"] is not None):
                    account_index -= 1
                    latest_account = db_data[account_index]
                if ("new_account_name" in latest_account and latest_account["new_account_name"] is not None):
                    acc = Account(latest_account["new_account_name"], steem_instance=stm)
                    if False and acc["id"] != latest_account["new_account_id"]:
                        raise ValueError("%d != %d for %s" % ( acc["id"], latest_account["new_account_id"], latest_account["new_account_name"]))
                
                time_for_blocks = time.time() - start_time
                print("\n---------------------\n")
                percentage_done = (block_num - start_block) / (end_block - start_block) * 100
                percentage_done_round = (block_num - start_block) / (end_block_round - start_block) * 100
                print("Block %d -- Datetime %s -- %.2f %% finished (%.2f %% in this round)" % (block_num, op["timestamp"], percentage_done, percentage_done_round))
                running_days = (end_block - block_num) * time_for_blocks / block_diff_for_db_storage / 60 / 60 / 24
                running_hours_round = (end_block_round - block_num) * time_for_blocks / block_diff_for_db_storage / 60 / 60
                print("Duration for %d blocks: %.2f s (%.3f s per block) -- %.2f days to go (%.2f hours in this round)" % (block_diff_for_db_storage, time_for_blocks, time_for_blocks / block_diff_for_db_storage, running_days, running_hours_round))
                print("%d  new accounts - %d in total" % (new_accounts_found, next_account_id - 1))
                print("%d  new follows" % new_follow_found)
                start_time = time.time()
                n_database = len(db_data)
                db = dataset.connect(databaseConnector)
                blockTrxStorage.db = db
                accountTrxStorage.db = db
                followsTrxStorage.db = db
                blockTrxStorage.add_batch(db_data)
                db_data = []
                accountTrxStorage.add_batch(db_account)
                time_for_database = time.time() - start_time
                print("Duration to write %d entries to DB: %.2f s" % (n_database, time_for_database))
                db_account = []
                start_time = time.time()
                last_block_num = block_num
                new_accounts_found = 0
                new_follow_found = 0
                cnt += 1
        stream_round += 1
