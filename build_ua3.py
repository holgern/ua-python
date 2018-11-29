from beem.blockchain import Blockchain
from beem.account import Account, Accounts
from beem.utils import formatTimeString
from beem.nodelist import NodeList
from beem import Steem
from datetime import datetime
from beem.instance import set_shared_steem_instance
import time
import math
import pprint
from steemua.block_ops_storage import BlockTrx, AccountTrx, Account2Trx, FollowsTrx
from steemua.storage import LockDB
import dataset
import os
import json
from pymongo import MongoClient, UpdateOne, InsertOne, DeleteMany, ReplaceOne


def basic_ua1(d, ua_sum, N):
    ua = (1 - d) + d * ua_sum
    return ua

def basic_ua2(d, ua_sum, N):
    ua = ((1 - d) / N) + d * ua_sum
    return ua


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
    update_stats = False
    use_mongodb = True
    
    db = dataset.connect(databaseConnector)    
    print("Start ua calculation")
    blockTrxStorage = BlockTrx(db)
    accountTrxStorage = AccountTrx(db)
    account2TrxStorage = Account2Trx(db)
    followsTrxStorage = FollowsTrx(db)
    lockStorage = LockDB(db)
    lockStorage.unlock("write_lock")
    
    if not followsTrxStorage.exists_table():
        followsTrxStorage.create_table()

    if use_mongodb:
        client = MongoClient(databaseConnector_mongo)
        db_mongo = client['steemua']
        accounts2_col = db_mongo["accounts2"]    
        conf_col = db_mongo["configuration"]
        print("top100_lock %s" % str(lockStorage.is_locked("top100_lock")))
        if lockStorage.is_locked("top100_lock"):
            print("stop_stream_block")

            exit()         

    nodes = NodeList()
    # nodes.update_nodes()
    node_list = nodes.get_nodes(appbase=True, normal=False, https=True, wss=True)
    stm = Steem(node=node_list)
        
    cnt = 0
    continue_cnt = 1
  
    db_data = []
    db_account = []
    db_follows = []
    
    if use_mongodb:
        accounts = {}
        for f in accounts2_col.find({}, {"followers": 0, "following": 0, "history": 0}):
            f["following"] = f["following_count"]
            f["followers"] = f["followers_count"]
            if "trust_score" not in f:
                f["trust_score"] = 0
            if "trust_score_norm" not in f:
                f["trust_score_norm"] = 0
            if "trust_score_n" not in f:
                f["trust_score_n"] = 0   
            if "created_at" not in f:
                f["created_at"] = None
            accounts[f["name"]] = f
    else:
        accounts = account2TrxStorage.get_accounts()
    print("read all accounts")
    if use_mongodb:
        latest_created_at = account2TrxStorage.get_latest_timestamp()
    else:          
        latest_created_at = followsTrxStorage.get_latest_created_at()
    print(latest_created_at)
    if latest_created_at is not None and isinstance(latest_created_at, str):
        latest_created_at = formatTimeString(latest_created_at)
    
    d = 0.85

    trust_seed = {}
    trust_score = {}
    trust_score_old = {}
    sybil = {}
    sybil_list = []
    N = len(accounts)
    trust_sum = 0
    trust_seed_sum = 0
    
    reverse_id = {}
    date_now = datetime.utcnow()
    last_activity_days = {}
    for account in accounts:
        account_id = int(accounts[account]["id"])
        reverse_id[account_id] = account
        trust_seed[account_id] = accounts[account]["trust_seed"]
        trust_seed_sum += trust_seed[account_id]
        trust_sum += accounts[account]["trust_score"]
        trust_score[account_id] = accounts[account]["trust_score"]
        trust_score_old[account_id] = trust_score[account_id]
        
        last_activity_days[account_id] = 0
    
    trust_score_diff = 1
    cnt = 0
    sybil_count = 0
    zero_followings = []
    for a in accounts:
        account_id = accounts[a]["id"]
        trust_seed[account_id] /= trust_seed_sum
        if accounts[a]["following"] == 0:
            zero_followings.append(account_id)
        elif accounts[a]["sybil"] > 0:
            zero_followings.append(account_id)
            sybil_count += 1
            
 
    trust_omega = 1 - trust_sum
    if trust_omega < 0:
        trust_omega = 0
    trust_omega_old = trust_omega    
 
        #elif SD[a] == 0:
    UA_sum = N
    rep_update_accounts = []
    print("account following nobody: %d (incl. sybils: %d)" % (len(zero_followings), sybil_count))
    all_follower_dict = {}
    all_follower_max = N + 1
    do_round = False
    if not lockStorage.is_locked("build_ua_lock"):
        lockStorage.upsert("build_ua_lock", {"value": True, "value_int": 0, "value_float": 1})
        trust_score_diff = 1
        cnt =  0
    else:
        lock_data = lockStorage.get("build_ua_lock")
        trust_score_diff = lock_data["value_float"]
        cnt =  lock_data["value_int"]
    while trust_score_diff > 1e-8 and cnt < 20:
    
    
        trust_sum = 0
        for account in accounts:
            account_id = accounts[account]["id"]
            trust_score_old[account_id] = trust_score[account_id]
            trust_sum += trust_score[account_id]

        print(" %d - diff %.6g - trust_sum %.3f - trust_omega %.3f - %d N - trust_score+omega %.6f" % (cnt, trust_score_diff, trust_sum, trust_omega, N, trust_sum+trust_omega))
        trust_omega_old = trust_omega
        follower_duration = 0

        account_cnt = 0
        omega_inactivity_share = 0
        for a in accounts:

            trust_a = 0
            followers_start_time = time.time()
            account_id = accounts[a]["id"]
            if accounts[a]["followers"] > 0:
                if account_id in all_follower_dict:
                    all_follower = all_follower_dict[account_id]
                else:
                    start_get_follower = time.time()
                    if use_mongodb:
                        for ret in accounts2_col.find({"_id": account_id}, {"followers": 1}):
                            if isinstance(ret, dict):
                                all_follower = ret["followers"]
                                if isinstance(all_follower, int):
                                    all_follower = [all_follower]
                            else:
                                print(account_id)
                                print(ret)
                                all_follower = []
                    else:
                        all_follower = followsTrxStorage.get_follower(account_id)
                    if len(all_follower) > 250000:
                        print("%d with %d followers needs %.3f s" % (account_id, len(all_follower), time.time()-start_get_follower))
                
                for follow_a in all_follower:
                    if not use_mongodb:
                        follow_a = follow_a["follower"]
                    if accounts[reverse_id[follow_a]]["following"] > 0 and accounts[reverse_id[follow_a]]["sybil"] == 0:
                        c = accounts[reverse_id[follow_a]]["following"]

                        trust_a += trust_score_old[follow_a] / c
               
            follower_duration += (time.time() - followers_start_time)
                
            trust_score[account_id] = (1 - d) * trust_seed[account_id] + d * trust_a
          
                
            account_cnt += 1
            if account_cnt % 100000 == 0:
                print("%d: %d/%d - duration %.2f seconds" %(cnt, account_cnt, N, follower_duration))
            if account_cnt > int(1e6):
                lockStorage.lock("write_lock")

        trust_a = 0
        for follow_a in zero_followings:
            c = 1
            trust_a += trust_score_old[follow_a] / c
        trust_a += trust_omega_old

        trust_omega = (1 - d) * 0 + d * trust_a
        print("omega_inactivity_share: %.3f" % omega_inactivity_share)
        print("%d - duration %.2f seconds" % (cnt, follower_duration))
        cnt += 1

        trust_score_diff = 0
        for account in accounts:
            account_id = accounts[account]["id"]
            if abs(trust_score[account_id] - trust_score_old[account_id]) > trust_score_diff:
                trust_score_diff = abs(trust_score[account_id] - trust_score_old[account_id])
    
        cnt2 = 0

        updated_accounts = []
        min_value_trust = min(list(trust_score.values()))
        max_value_trust = max(list(trust_score.values()))
        
        lockStorage.lock("write_lock")
        for account in accounts:    
            account_id = accounts[account]["id"]
            new_account = {}
            new_account["id"] = account_id
            new_account["cached_at"] = latest_created_at
            new_account["trust_score"] = trust_score[account_id]
            new_account["trust_score_norm"] = (trust_score[account_id] - min_value_trust) / (max_value_trust - min_value_trust)  
            new_account["trust_score_n"] = trust_score[account_id] * N
            if use_mongodb:
                updated_accounts.append(UpdateOne({"_id": account_id}, {"$set": new_account}))
            else:
                updated_accounts.append(new_account)
            
            if cnt2 % 100000 == 0 and cnt2 > 0:
                print("%d of %d" %(cnt2, N))
                start_write_time = time.time()
                if use_mongodb:
                    while lockStorage.is_locked("read_lock"):
                        print("sleeping 10 s now")
                        time.sleep(10)
             
                    accounts2_col.bulk_write(updated_accounts)
                    time.sleep(10)
                else:
                    account2TrxStorage.update_batch(updated_accounts)
                print("writing %d accounts takes %.2f seconds" % (len(updated_accounts), time.time() - start_write_time))
                updated_accounts = []
            cnt2 += 1
                            
        if len(updated_accounts) > 0:
            start_write_time = time.time()
            if use_mongodb:
                #

                accounts2_col.bulk_write(updated_accounts)
                time.sleep(10)
            else:
                account2TrxStorage.update_batch(updated_accounts)
            print("writing %d accounts takes %.2f seconds" % (len(updated_accounts), time.time() - start_write_time))
            updated_accounts = []
        lockStorage.unlock("write_lock")
        
        lockStorage.upsert("build_ua_lock", {"value": True, "value_int": cnt, "value_float": trust_score_diff})
    else:
        lockStorage.upsert("build_ua_lock", {"value": False, "value_int": cnt, "value_float": trust_score_diff})
        lockStorage.lock("top100_lock")

    
