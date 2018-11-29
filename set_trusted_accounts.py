from beem.blockchain import Blockchain
from beem.account import Account, Accounts
from beem.utils import formatTimeString
from beem.nodelist import NodeList
from beem.witness import WitnessesRankedByVote, Witnesses, ListWitnesses
from beem.account import Account
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
    
    use_postgresql = False
    
    db = dataset.connect(databaseConnector)   
    lockStorage = LockDB(db)
    
    blockTrxStorage = BlockTrx(db)
    accountTrxStorage = AccountTrx(db)
    account2TrxStorage = Account2Trx(db)
    followsTrxStorage = FollowsTrx(db)
    
    if not followsTrxStorage.exists_table():
        followsTrxStorage.create_table()

    if not use_postgresql:
        client = MongoClient(databaseConnector_mongo)
        db_mongo = client['steemua']
        accounts2_col = db_mongo["accounts2"]
        conf_col = db_mongo["configuration"]
        print("build_ua_lock %s" % str(lockStorage.is_locked("build_ua_lock")))
        if lockStorage.is_locked("build_ua_lock"):
            print("stop set_trusted_account")
            exit()             
        
        print("top100_lock %s" % str(lockStorage.is_locked("top100_lock")))
        if lockStorage.is_locked("top100_lock"):
            print("stop set_trusted_account")
            exit()             
        print("read_lock %s" % str(lockStorage.is_locked("read_lock")))
        if lockStorage.is_locked("read_lock"):
            print("stop set_trusted_account")
            exit()      
    nodes = NodeList()
    nodes.update_nodes()
    node_list = nodes.get_nodes(appbase=True, normal=False, https=True, wss=True)
    stm = Steem(node=node_list)
        
    cnt = 0
    continue_cnt = 1
  
    db_data = []
    db_account = []
    db_follows = []
    accounts = account2TrxStorage.get_accounts()


    
    d = 0.85
    
    UA_old = {}
    UA = {}
    trust_seed = {}
    trust_score = {}
    trust_score_old = {}
    N = len(accounts)
    UA_sum = 0
    trust_sum = 0
    spam_sum = 0
    reverse_id = {}
    latest_active_at = None
    oldest_active_at = None
    
    for account in accounts:
        account_id = accounts[account]["id"]
        reverse_id[account_id] = account
    if False:
        print("resetting trust and spam")
        updated_accounts = []
        cnt2 = 0
        for account in accounts:
            new_account = {}
            new_account["id"] = accounts[account]["id"]
            new_account["trust_seed"] = 0
            new_account["trust_score"] = 0
            new_account["trust_score_norm"] = 0
            new_account["trust_score_n"] = 0       
            updated_accounts.append(new_account)
            cnt2 += 1
            if cnt2 % 250000 == 0:
                print("%d of %d" %(cnt2, N))
                account2TrxStorage.update_batch(updated_accounts)
                updated_accounts = []        
        account2TrxStorage.update_batch(updated_accounts)
        updated_accounts = []
        print("done")
    w = Witnesses()
    witness_count = w.witness_count
    
    
    witness_owner = {}
    witness_owner["smooth.witness"] = ["smooth"]
    witness_owner["noisy.witness"] = ["noisy"]
    witness_owner["ats-witness"] = ["ats-david"]
    witness_owner["steemgigs"] = ["surpassinggoogle"]
    witness_owner["steemcommunity"] = ["paulag", "abh12345"]
    witness_owner["ocd-witness"] = ["acidyo"]
    witness_owner["castellano"] = ["moisesmcardona", "nnnarvaez"]
    witness_owner["exnihilo.witness"] = ["elmetro", "siavach"]
    witness_owner["steemitboard"] = ["arcange"]
    witness_owner["busy.witness"] = ["busy.org"]
    witness_owner["cloh76.witness"] = ["cloh76"]
    witness_owner["steemhq.witness"] = ["steemhq"]
    witness_owner["fyrst-witness"] = ["fyrstikken"]
    witness_owner["noblewitness"] = ["gmuxx", "rhondak", "sircork", "anarcho-andrei"]
    witness_owner["jacor-witness"] = ["jacor"]
    witness_owner["swisswitness"] = ["wolfje", "felander", "sirwries"]
    witness_owner["humanheart"] = ["safedeposit", "justyy"]
    witness_owner["ro-witness"] = ["catalincernat", "alexvan"]
    witness_owner["intelliwitness"] = ["intelliguy"]
    witness_owner["adsactly-witness"] = ["adsactly"]
    witness_owner["lukestokes.mhth"] = ["lukestokes"]
    witness_owner["chainsquad.com"] = ["chainsquad"]
    witness_owner["lux-witness"] = ["sorin.cristescu", "pstaiano"]
    witness_owner["c0ff33a"] = ["c0ff33a", "derangedvisions"]
    
    
    
    i = 0
    ww_list = ListWitnesses(limit=1000)
    i = 1000
    while i < witness_count - 1:
        print("witness %d/%d" % (i, witness_count))
        if witness_count - i > 1000:
            ww = ListWitnesses(from_account=ww_list[-1]["owner"], limit=1000)
            ww_list.extend(ww[1:])
            i += len(ww) - 1
        else:
            ww = ListWitnesses(from_account=ww_list[-1]["owner"], limit=witness_count - i)
            ww_list.extend(ww[1:])
            i += len(ww) - 1   
    
    updated_accounts = []
    i = 0
    out = 0
    trusted_acc = 0
    version_list = []
    seed_accounts = {}
    
    if not use_postgresql: 
        lockStorage.lock("write_lock")
    while i < len(ww_list):
        witness = ww_list[i]
        i += 1
        
        if not (witness.is_active or witness["running_version"] >= "0.20.0"):
            continue
        
        if witness["running_version"] < "0.20.0":
            
            if witness["running_version"] not in version_list:
                version_list.append(witness["running_version"])
                print("skipping version %s" % witness["running_version"])
            continue
        account_name = witness["owner"]
        
        if account_name not in accounts:
            continue
        if account_name in witness_owner:
            account_names = witness_owner[account_name]
        else:
            account_names = [account_name]
        
        for account_name in account_names:
            if account_name not in seed_accounts:
                seed_accounts[account_name] = witness["votes"] / 1e15 / len(account_names)
            else:
                seed_accounts[account_name] += witness["votes"] / 1e15 / len(account_names)
            if use_postgresql:
                out += accounts[account_name]["following"]
                
                new_account = {}
                new_account["id"] = accounts[account_name]["id"]
                new_account["trust_seed"] = seed_accounts[account_name]
                updated_accounts.append(new_account)
            
                print("%s: %.3f" % (account_name, new_account["trust_seed"]))
            else:
                new_account = accounts2_col.find_one({"_id": accounts[account_name]["id"]})
                new_account["id"] = accounts[account_name]["id"]
                new_account["trust_seed"] = seed_accounts[account_name]
                out += accounts[account_name]["following"]
                updated_accounts.append(UpdateOne({"_id": accounts[account_name]["id"]}, {"$set": {"trust_seed": seed_accounts[account_name]}}))
                print("%s: %.3f" % (account_name, new_account["trust_seed"]))

    print("outgoing follows %d" % out)

    if use_postgresql:
        if len(updated_accounts) > 0:
            account2TrxStorage.update_batch(updated_accounts)
            updated_accounts = []
    else:
        if len(updated_accounts) > 0:
            accounts2_col.bulk_write(updated_accounts)
            

    if not use_postgresql:
        time.sleep(60)
        lockStorage.unlock("write_lock")

