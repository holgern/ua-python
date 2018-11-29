from beem.blockchain import Blockchain
from beem.block import Block
from beem.account import Account, Accounts
from beem.utils import formatTimeString
from beem.nodelist import NodeList
from beem import Steem
from beembase.operationids import operations, getOperationNameForId
from datetime import datetime
from beem.instance import set_shared_steem_instance
import time
import pprint
from steemua.block_ops_storage import BlockTrx, AccountTrx, Account2Trx, FollowsTrx
from steemua.storage import LockDB
import dataset
import os
import json
from pymongo import MongoClient, UpdateOne

if __name__ == "__main__":
    
    config_file = 'config_pg.json'
    if not os.path.isfile(config_file):
        return
    else:    
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        #print(config_data)
        databaseConnector = config_data["databaseConnector"]
        if "follow_db_batch_size" in config_data:
            follow_db_batch_size = config_data["follow_db_batch_size"]
        else:
            follow_db_batch_size = 1000
        if "fetch_account_ids" in config_data:
            fetch_account_ids = config_data["fetch_account_ids"]
        else:
            fetch_account_ids = False

        with open("config_mongo.json") as json_data_file:
            config_data_mongo = json.load(json_data_file)
        databaseConnector_mongo = config_data_mongo["databaseConnector"]    
    print("build follow db")
    use_mongodb = True
    
    db = dataset.connect(databaseConnector) 
    
    blockTrxStorage = BlockTrx(db)
    accountTrxStorage = AccountTrx(db)
    account2TrxStorage = Account2Trx(db)
    followsTrxStorage = FollowsTrx(db)
    lockStorage = LockDB(db)
    
    if use_mongodb:
        client = MongoClient(databaseConnector_mongo)
        db_mongo = client['steemua']
        accounts2_col = db_mongo["accounts2"]
        conf_col = db_mongo["configuration"]

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
            print("stop ua100")
            exit()                  
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    nodes.update_nodes()
    node_list = nodes.get_nodes(appbase=True, normal=False, https=True)
    stm = Steem(node=node_list)
    set_shared_steem_instance(stm)    
    
            
    accounts_list_new = []
    accounts_name_ids = {}
    start_time_reading = time.time()
    for account in accountTrxStorage.get():
        acc_id = account["account_index"]
        acc_name = account["account_name"]
        if acc_id < 965380 or not fetch_account_ids:
            # accounts_list_new.append(account)
            accounts_name_ids[acc_name] = acc_id    
    print("All accounts loaded in %.2f seconds." % (time.time() - start_time_reading))
    if account2TrxStorage.get_latest_index() is None and True:
        account2TrxStorage.add({"id": 0, "name": "miners", "created_at": datetime(1970,1,1), "block_num": 0,  "followers": 0, "following": 0, "cached_at": datetime.utcnow()})
        account2TrxStorage.add({"id": 1, "name": "null", "created_at": datetime(1970,1,1), "block_num": 0,  "followers": 0, "following": 0, "cached_at": datetime.utcnow()})
        account2TrxStorage.add({"id": 2, "name": "temp", "created_at": datetime(1970,1,1), "block_num": 0,  "followers": 0, "following": 0,  "cached_at": datetime.utcnow()})
        account2TrxStorage.add({"id": 3, "name": "initminer", "created_at": datetime(1970,1,1), "block_num": 0, "followers": 0, "following": 0, "cached_at": datetime.utcnow()})

    # last_account_id = 3
    if not followsTrxStorage.exists_table():
        followsTrxStorage.create_table()

    start_block = blockTrxStorage.get_latest_block_num()
    trx_list = blockTrxStorage.get_block_trx_id(start_block)
    if start_block is None:
        start_block = 1091
        trx_list = []
        
    cnt = 0
    continue_cnt = 1
    print(start_block)
    start_time_reading = time.time()
    last_account_id = account2TrxStorage.get_latest_index()
    if last_account_id is None:
        last_account_id = -1
    last_account_block_num= account2TrxStorage.get_latest_block_num()
 
    db_data = []
    db_account = []
    db_follows = []
    accounts = account2TrxStorage.get_account_ids()
    print("All accounts2 loaded in %.2f seconds." % (time.time() - start_time_reading))

    start_time_reading = time.time()
    latest_created_at = followsTrxStorage.get_latest_created_at()
    print("Latest follow op loaded in %.2f seconds." % (time.time() - start_time_reading))

    print(latest_created_at)
    print(last_account_block_num)

    
    bc= Blockchain()
    if last_account_block_num is not None and latest_created_at is not None:
        block1 = (last_account_block_num)
        block2 = latest_created_at

        next_block_num = min([block1, block2]) + 1
    elif last_account_block_num is not None:
        block1 = (last_account_block_num)
        if block1 < 0:
            block1 = 1091
        next_block_num = block1 + 1
    else:
        next_block_num = 1091 + 1
    print(next_block_num)
    print("start")

    blocks = []
    
    last_block_num = 0
    api_calls = 0
    limit = 10000
    block_num = next_block_num - 1
    trx_num_available = False
    data_available = True
    offset = 0
    last_block = next_block_num
    while data_available:
        #for ops_data in blockTrxStorage.get_block(next_block_num):
        blocks_range = {}
        for ops_data in blockTrxStorage.get_block_range(last_block, follow_db_batch_size):
            #print(ops_data["block_num"])
            
            if continue_cnt % 10000 == 0:
                print("skipping ...  %d - offset %d" % (continue_cnt, offset))
                print(ops_data["timestamp"])
            
            block_num = ops_data["block_num"]
            last_block = block_num
            trx_id = ops_data["trx_id"]
            trx_num = ops_data["trx_num"]
            # print(ops_data["timestamp"])

            if last_account_block_num is not None and ops_data["block_num"] < last_account_block_num:
                continue_cnt += 1
                continue
            if block_num in blocks_range:
                blocks_range[block_num].append(ops_data)
            else:
                blocks_range[block_num] = [ops_data]
        print("blocks_range %d - %d" % (list(blocks_range.keys())[0], list(blocks_range.keys())[-1]))
        if list(blocks_range.keys())[0] == list(blocks_range.keys())[-1]:
            data_available = False
            continue
        for br in sorted(list(blocks_range.keys())):
            # print(br)
            if br < last_block:
                blocks = blocks_range[br]
                next_block_num = br
            else:
                blocks = []
            if len(blocks) > 0:
                # print("new blocks")
                trx_num_needed = True
                
                follower_names_1 = []
                following_names_1 = []
                follower_names_0 = []
                following_names_0 = []                
                what_list = []
                new_acc_names = []
                new_acc_index = 0
                new_acc = 0
                if False and blocks[0]["block_num"] < 15355796:
                    for b in blocks:
                        if b["trx_num"] > 0:
                            trx_num_needed = False
                            trx_num_available = True
                            print("trx_num_available %d" % b["block_num"])
                        if b["type"] == "custom_json" and b["id"] == "follow":
                            if b["what"] in ["posts", "blog"]:
                                what_list.append(1)
                                follower_names_1.append(b["follower"])
                                following_names_1.append(b["following"])                            
                            elif not bool(b["what"]):
                                what_list.append(0)      
                                follower_names_0.append(b["follower"])
                                following_names_0.append(b["following"])
                        elif b["type"] != "custom_json":
                            new_acc += 1
                            new_acc_names.append(b["new_account_name"])
                            new_acc_index = b["new_account_id"]
                    if trx_num_needed:
                        if len(what_list) <= 1 and len(what_list) <= 1: # and new_acc <= 1:
                            trx_num_needed = False
                        elif len(follower_names_1) == 0 or len(follower_names_0) == 0: # and new_acc <= 1:
                            trx_num_needed = False
                        elif len(following_names_1 + following_names_0) == len(set(following_names_1 + following_names_0)):
                            trx_num_needed = False
                            
                    if trx_num_needed:
                        if len(blocks) < 2:
                            trx_num_needed = False
                    if trx_num_needed:
                        block = Block(blocks[0]["block_num"])
                        api_calls += 1
                        trx_ids = block["transaction_ids"]
                        # print("%d - %d blocks" % (last_block_num, len(blocks)))
                        for i in range(len(blocks)):
                            trx_num = trx_ids.index(blocks[i]["trx_id"])
                            blocks[i]["trx_num"] = trx_num
                            # blockTrxStorage.update_trx_num(block["block_num"], block["trx_id"], block["op_num"], trx_num)
                    elif new_acc > 1 and new_acc_index < 965300:
                        sorted_blocks = sorted(blocks, key=lambda x:  (x["new_account_id"] is None, x["new_account_id"]))
                        trx_ids = []
                        for block in sorted_blocks:
                            trx_ids.append(block["trx_id"])
                        for i in range(len(blocks)):
                            trx_num = trx_ids.index(blocks[i]["trx_id"])
                            blocks[i]["trx_num"] = trx_num            
                    elif not trx_num_available:
                        for i in range(len(blocks)):
                            blocks[i]["trx_num"] = i
                
                sorted_blocks = sorted(blocks, key=lambda x: x["trx_num"])
                for data in sorted_blocks:
                    if data["type"] == "custom_json" or (isinstance(data["type"], int) and getOperationNameForId(data["type"]) == "custom_json"):
                        
    
                        if latest_created_at is not None and data["block_num"] < latest_created_at:
                            continue_cnt += 1
                            continue
                        if len(data["follower"]) > 16:
                            print("%s has a length %d, skipping" % (data["follower"], len(data["follower"])))
                            continue_cnt += 1
                            continue
                        
                        if data["following"] not in accounts:
                            continue_cnt += 1
                            print("%s not in accounts, %s - %d" % (data["following"], data["follower"], data["block_num"]))
                            # print("continue")
                            continue
                        follower = accounts[data["follower"]]
                        following = accounts[data["following"]]
                        if data["following"] not in accounts:
                            continue_cnt += 1
                            # print("continue")
                            continue
            
                        if data["what"] in ["posts", "blog"]:
                            state = 1
                        elif data["what"] in ["ignore"]:
                            state = -1                            
                        elif not bool(data["what"]):
                            state = 0
                        else:
                            state = 0
                            #continue_cnt += 1
                            # print("continue")
                            #continue
                        follow_data = {"follower": follower, "following": following, "state": state, "block_num": data["block_num"]}
                        db_follows.append(follow_data)
                        
                    else:
                        account_name = data["new_account_name"]
                        if not fetch_account_ids or last_account_id < 965379:
                            next_account_id = accounts_name_ids[account_name]
                        else:
                            acc = Account(account_name, full=False)
                            api_calls += 1
                            next_account_id = acc["id"]
  
                        if next_account_id == last_account_id:
                            print("same account id")
                            continue
                        if next_account_id - last_account_id != 1:
                            raise ValueError("Missing data acc %s id %d to %d - block id %d" % (account_name, last_account_id, next_account_id, block_num))
                        timestamp = data["timestamp"]
                        if isinstance(timestamp, str):
                            timestamp = formatTimeString(timestamp)
                        account2_data = {"id":
                                         next_account_id, "name": account_name, "created_at":
                                         timestamp, "block_num":
                                         data["block_num"], "followers": 0,
                                         "following": 0, 
                                         # "active_at": formatTimeString(data["timestamp"]),
                                         "cached_at": timestamp}
                        accounts[account_name] = next_account_id
                        db_account.append(account2_data)
                        last_account_id = next_account_id
                    if cnt % follow_db_batch_size == 0 and cnt > 0:
                        print(data["timestamp"])
                        print("api_calls %d" % api_calls)

                        start_time = time.time()
                        followsTrxStorage.add_batch_pg(db_follows)
                        time_for_database = time.time() - start_time
                        print("Duration to write %d follow entries to DB: %.2f s" % (len(db_follows), time_for_database))                        
                        start_time = time.time()
                        account2TrxStorage.add_batch(db_account)
                        time_for_database = time.time() - start_time
                        print("Duration to write %d account entries to DB: %.2f s" % (len(db_account), time_for_database))
                        if use_mongodb:
                            start_time = time.time()
                            for i in range(len(db_account)):
                                db_account[i]["_id"] = db_account[i]["id"]
                                db_account[i]["follower"] = []
                                db_account[i]["following"] = []
                                db_account[i]["followers_count"] = 0
                                db_account[i]["following_count"] = 0
                                db_account[i]["trust_score"] = 0.0
                                db_account[i]["last_trust_score"] = 0.0
                                db_account[i]["rank"] = 0
                                db_account[i]["trust_score_norm"] = 0.0
                                db_account[i]["trust_score_n"] = 0.0                                     
                            if len(db_account) > 0:
                                accounts2_col.insert_many(db_account)
                            print("Duration to write %d account entries to Mongo DB: %.2f s" % (len(db_account), time_for_database))
                            if False:
                                start_time = time.time()
                                for op in db_follows:
                                    follower = accounts2_col.find_one({"_id": op["follower"]})
                                    following = accounts2_col.find_one({"_id": op["following"]})                            
                                    if op["state"] == 1:
                                        if following["followers"] is None:
                                            following["followers"] = [op["follower"]]
                                            following["followers_count"] = 1                                            
                                        elif op["follower"] not in following["followers"]:
                                            following["followers"].append(op["follower"])
                                            following["followers_count"] += 1

                                        if follower["following"] is None:
                                            follower["following"] = [op["following"]]
                                            follower["following_count"] = 1                                            
                                        elif op["following"] not in follower["following"]:
                                            follower["following"].append(op["following"])
                                            follower["following_count"] += 1
                                    else:
                                        if following["followers"] is None:
                                            following["followers"] = []
                                            following["followers_count"] = 0
                                        elif op["follower"] in following["followers"]:
                                            following["followers"].remove(op["follower"])
                                            following["followers_count"] -= 1
                                            
                                        if follower["following"] is None:
                                            follower["following"] = []
                                            follower["following_count"] = 0
                                        elif op["following"] in follower["following"]:
                                            follower["following"].remove(op["following"])
                                            follower["following_count"] -= 1
                                    accounts2_col.update_one({"_id": op["following"]}, {"$set": {"followers": following["followers"], "followers_count": following["followers_count"]}})
                                    accounts2_col.update_one({"_id": op["follower"]}, {"$set": {"following": follower["following"], "following_count": follower["following_count"]}})
                                print("Duration to write %d follow entries to Mongo DB: %.2f s" % (len(db_follows), time_for_database))
                        start_time = time.time()
                        if len(accounts_list_new) > 0:
                            #accountTrxStorage.add_batch(accounts_list_new)
                            accounts_list_new = []
                        db_follows = []
                        db_account = []            
                        api_calls = 0
                    cnt += 1
                blocks = []
        last_block_num = block_num
            
                
    
            # cnt += 1
        offset += limit
        next_block_num += 1
        if next_block_num > start_block:
            data_available = False


    if not use_mongodb and ((len(db_follows) > 0 or len(db_account) > 0)):
        print(data["timestamp"])

        
        if use_mongodb:
            for i in range(len(db_account)):
                db_account[i]["_id"] = db_account[i]["id"]
                db_account[i]["followers_count"] = 0
                db_account[i]["following_count"] = 0
            accounts2_col.insert_many(db_account)
            if False:
                for op in db_follows:
                    follower = accounts2_col.find_one({"_id": op["follower"]})
                    following = accounts2_col.find_one({"_id": op["following"]})                            
                    if op["state"] == 1:
                        if op["follower"] not in following["followers"]:
                            following["followers"].append(op["follower"])
                            following["followers_count"] += 1
                        if op["following"] not in follower["following"]:
                            follower["following"].append(op["following"])
                            follower["following_count"] += 1
                    else:
                        if op["follower"] in following["followers"]:
                            following["followers"].remove(op["follower"])
                            following["followers_count"] -= 1
                        if op["following"] in follower["following"]:
                            follower["following"].remove(op["following"])
                            follower["following_count"] -= 1
                    accounts2_col.update_one({"_id": op["following"]}, {"$set": {"followers": following["followers"], "followers_count": following["followers_count"]}})
                    accounts2_col.update_one({"_id": op["follower"]}, {"$set": {"following": follower["following"], "following_count": follower["following_count"]}})        
        
        if len(accounts_list_new) > 0:
            accounts_list_new = []        
        db_follows = []
        db_account = []
    
