from beem.blockchain import Blockchain
from beem.account import Account, Accounts
from beem.utils import formatTimeString
from beem.nodelist import NodeList
from beem import Steem
from datetime import datetime, timedelta
from beem.instance import set_shared_steem_instance
import time
import pprint
from steemua.block_ops_storage import BlockTrx, AccountTrx, Account2Trx, FollowsTrx
from steemua.storage import LockDB
import dataset
import os
import json
from pymongo import MongoClient, UpdateOne, InsertOne, DeleteMany, ReplaceOne

if __name__ == "__main__":
    
    config_file = 'config.json'
    
    config_file = 'config_pg.json'
    if not os.path.isfile(config_file):
        block_diff_for_db_storage = 1000
        databaseConnector = "sqlite://db.sqlite"
    else:    
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        databaseConnector = config_data["databaseConnector"]
        if "follow_db_batch_size" in config_data:
            follow_db_batch_size = config_data["follow_db_batch_size"]
        else:
            follow_db_batch_size = 1000
        with open("config_mongo.json") as json_data_file:
            config_data_mongo = json.load(json_data_file)
        databaseConnector_mongo = config_data_mongo["databaseConnector"]        
    use_mongodb = True
    db = dataset.connect(databaseConnector)    
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
            print("stop stream_block")
            exit()                  
    else:
        account2TrxStorage = Account2Trx(db)
        accountTrxStorage = AccountTrx(db)
    
        blockTrxStorage = BlockTrx(db)
    
    
    followsTrxStorage = FollowsTrx(db)        
    if not followsTrxStorage.exists_table():
        followsTrxStorage.create_table()        

    cnt = 0
    if use_mongodb:
        accounts = {}
        last_id = -1
        for f in accounts2_col.find({}, {"_id": 1, "id": 1, "name": 1, "cached_at": 1}).sort("_id"):
            accounts[f["name"]] = f
            if f["id"] - last_id != 1:
                print("warning jump from %d  to %d" % (last_id, f["id"]))
            cnt += 1
            last_id = f["id"]
            if cnt > 0 and cnt % 100000 == 0:
                print("%d loaded" % cnt)
                
    else:
        accounts = account2TrxStorage.get_accounts()
    print("all accounts2 loaded!")
    sybil_list = []
    if True:
        with open('sybil_list.json', 'r') as f:
            sybil_list = json.load(f)

    print("Account loaded!")
    batch_size = 10000
    d = 0.85
    N = len(accounts)
    if use_mongodb and False:
        print("start account maintainance")
        old_date = datetime(1970, 1, 1, 0, 0, 0)
        updated_accounts = []
        cnt = 0
        for account in accounts:
            account_id = accounts[account]["id"]    
            updated_accounts.append(UpdateOne({"_id": account_id}, {"$set": {"last_vote_time": old_date, "last_post": old_date, "last_root_post": old_date}}))
            cnt += 1
            if cnt > 0 and cnt % batch_size == 0:
                print("%d of %d" %(cnt, N))
                accounts2_col.bulk_write(updated_accounts)
                updated_accounts = []
        accounts2_col.bulk_write(updated_accounts)
        print("account maintainance finished")
    
    
    UA_old = {}
    UA = {}
    update_ua_count = 0
    update_date = datetime.utcnow() - timedelta(days=10)
    
    reverse_id = {}
    updated_accounts = []
    cnt = 0
    
    write_lock_post = conf_col.find_one({"key": "write_lock_post"})
    # print("read_lock %s" % str(write_lock["value"]))
    while write_lock_post["value"]:
        print("sleeping 600 s now")
        time.sleep(600)
        write_lock_post = conf_col.find_one({"key": "write_lock_post"})     
    
    start_time = time.time()
    write_lock = conf_col.find_one({"key": "write_lock"})
    conf_col.update({"_id": write_lock['_id']}, {"$set": {"value": True}})    
    for account in accounts:
        account_id = accounts[account]["id"]
        reverse_id[account_id] = account
        new_account = {}
        if not use_mongodb:
            new_account["id"] = account_id
        if use_mongodb:
            acc = None
            for f in accounts2_col.find({"_id": account_id}):
                acc = f       
        new_account["trust_seed"] = 0.0
        if len(sybil_list) > 0 and account in sybil_list:
            new_account["sybil"] = 1
        elif len(sybil_list) > 0:
            new_account["sybil"] = 0

        if use_mongodb and acc is not None:
            if isinstance(acc["followers"], int):
                
                new_account["followers"] = [acc["followers"]]
                new_account["followers_count"] = len(new_account["followers"])
            elif acc["followers"] is None:
                
                new_account["followers"] = []
                new_account["followers_count"] = len(new_account["followers"])
            else:
                new_account["followers_count"] = len(acc["followers"])
            if isinstance(acc["following"], int):
                new_account["following"] = [acc["following"]]
                new_account["following_count"] = len(new_account["following"])
            elif acc["following"] is None:
                new_account["following"] = []
                new_account["following_count"] = len(new_account["following"])
            else:            
                new_account["following_count"] = len(acc["following"])
            
            if "trust_score" not in acc:
                new_account["trust_score"] = 0.0
            elif acc["trust_score"] == 0:
                new_account["trust_score"] = 0.0
            if "last_trust_score" not in acc:
                new_account["last_trust_score"] = 0.0
            if "rank" not in acc:
                new_account["rank"] = 0
            if "trust_score_norm" not in acc:
                new_account["trust_score_norm"] = 0.0
            if "trust_score_n" not in acc:
                new_account["trust_score_n"] = 0.0            
                
            if isinstance(acc["created_at"], str):
                new_account["created_at"] = formatTimeString(f["created_at"]).replace(tzinfo=None)
            
            if False and (isinstance(accounts[account]["cached_at"], str) or (accounts[account]["cached_at"] < update_date and update_ua_count < 10000)):
                try:
                    stm_acc = Account(account)
                    new_account["last_vote_time"] = stm_acc["last_vote_time"]
                    new_account["last_post"] = stm_acc["last_post"]
                    new_account["last_root_post"] = stm_acc["last_root_post"]
                    new_account["cached_at"] = datetime.utcnow()
                    update_ua_count += 1
                except:
                    print("Could not get info about %s" % account)
                    # accounts2_col.update({"_id": account_id}, {"$set": {"following_count": new_account["following_count"], "followers_count": new_account["followers_count"]}})
            updated_accounts.append(UpdateOne({"_id": account_id}, {"$set": new_account}))
        else:
            new_account["followers"] = followsTrxStorage.get_follower_count(account_id)
            new_account["following"] = followsTrxStorage.get_following_count(account_id)
        
        if not use_mongodb:
            updated_accounts.append(new_account)
        
        if cnt % batch_size == 0 and cnt > 0:
            
            proc_time = time.time() - start_time
            remain_time = (N - cnt) / batch_size * proc_time
            print("%d of %d in %.2f seconds - %.2f remaining" %(cnt, N, proc_time, remain_time))
            if not use_mongodb:
                account2TrxStorage.update_batch(updated_accounts)
            else:
                read_lock = conf_col.find_one({"key": "read_lock"})
                # print("read_lock %s" % str(write_lock["value"]))
                while read_lock["value"]:
                    print("sleeping 600 s now")
                    time.sleep(600)
                    read_lock = conf_col.find_one({"key": "read_lock"})     

                accounts2_col.bulk_write(updated_accounts)
            updated_accounts = []
            start_time = time.time()
        cnt += 1
                        
    if len(updated_accounts) > 0:


        if not use_mongodb:
            account2TrxStorage.update_batch(updated_accounts)
        else:
            read_lock = conf_col.find_one({"key": "read_lock"})
            # print("read_lock %s" % str(write_lock["value"]))
            while read_lock["value"]:
                print("sleeping 600 s now")
                time.sleep(600)
                read_lock = conf_col.find_one({"key": "read_lock"})            
            accounts2_col.bulk_write(updated_accounts)            
        updated_accounts = []
    
    time.sleep(60)
    write_lock = conf_col.find_one({"key": "write_lock"})
    conf_col.update({"_id": write_lock['_id']}, {"$set": {"value": False}})