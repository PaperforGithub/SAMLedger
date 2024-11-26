#include "global.h"
#include "message.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "timer.h"
#include "chain.h"

#if NEW_SHARPER
/**
 * Processes an incoming client batch and sends a Pre-prepare message to al replicas.
 *
 * This function assumes that a client sends a batch of transactions and 
 * for each transaction in the batch, a separate transaction manager is created. 
 * Next, this batch is forwarded to all the replicas as a BatchRequests Message, 
 * which corresponds to the Pre-Prepare stage in the PBFT protocol.
 *
 * @param msg Batch of Transactions of type CientQueryBatch from the client.
 * @return RC
 */
RC WorkerThread::process_client_batch(Message *msg)
{
    #if STRONG_SERIAL
        //cout << "test_v4:primary locked\n";
        sem_wait(&consensus_lock);
        //cout << "test_v4:primary unlocked\n";
    #endif
    ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;
    //DEBUG("process_client_batch! requests.size: %ld requests_writeset.size: %ld\n",clbtch->cqrySet[0]->requests.size(), clbtch->cqrySet[0]->requests_writeset.size());
    //cout << "test_v3:process_client_batch(Message *msg)::test_v3:msg->txn_id = " << msg->txn_id << "\n";
    //cout << "test_v3:msg->batch_id = " << msg->batch_id << "\n";
    //cout << "test_v8:msg->is_cross_shard = " << msg->is_cross_shard << "\n";
    #if KDK_DEBUG1
    printf("ClientQueryBatch: %ld, THD: %ld :: CL: %ld :: RQ: %ld\n", msg->txn_id, get_thd_id(), msg->return_node_id, clbtch->cqrySet[0]->requests[0]->key);
    fflush(stdout);
    #endif
// if((ZS==false) && (TxAllo==false) && (ZSShardBase == false) && (ZSShardPlus==false) && (SHARD_SCHEDULER==false)){
#if !ZS && !TxAllo && !ZSShardBase  && !ZSShardPlus && !SHARD_SCHEDULER
    cout << endl<<"ZS-@-SHARPER"<<endl;
#else
    // if(ZS==true && TxAllo==true && ZSShardBase == true && ZSShardPlus==true && SHARD_SCHEDULER==false){
    if(ZS && TxAllo && ZSShardBase && ZSShardPlus && ZSShardM && !SHARD_SCHEDULER){
        if(g_dag.cnt_batch<10){
            cout << endl<<"ZS-@-ZSShardPlus"<<endl;
        }
    }
    if(ZS && TxAllo && ZSShardBase && !ZSShardPlus && !ZSShardM && !SHARD_SCHEDULER){
        if(g_dag.cnt_batch<10){
            cout << endl<<"ZS-@-ZSShardM"<<endl;
        }
    }
    // if(ZS==true && TxAllo==true && ZSShardBase == false && ZSShardPlus==false && SHARD_SCHEDULER==false){
    if(ZS && TxAllo && !ZSShardBase && !ZSShardPlus && !SHARD_SCHEDULER){
        if(g_dag.cnt_batch<10){
            cout << endl<<"ZS-@-TxAllo"<<endl;
        }
    }
    // if(ZS==true && TxAllo==true && ZSShardBase == true && ZSShardPlus==false && SHARD_SCHEDULER==false){
    if(ZS && TxAllo && ZSShardBase && ZSShardM && !ZSShardPlus && !SHARD_SCHEDULER){
        if(g_dag.cnt_batch<10){
            cout << endl<<"ZS-@-ZSShardBase"<<endl;
        }
    }
    // if(ZS==true && TxAllo==true && ZSShardBase == true && ZSShardPlus==true && SHARD_SCHEDULER==true){
    if(ZS && TxAllo && ZSShardBase && ZSShardPlus && SHARD_SCHEDULER){
        if(g_dag.cnt_batch<10){
            cout << endl<<"ZS-@-SHARD_SCHEDULER"<<endl;
        }
    }
#endif





#if SHARD_SCHEDULER
if(clbtch->do_or_pending==0){
    char *buf = create_msg_buffer(clbtch);
    Message *deepMsg = deep_copy_msg(buf, clbtch);
    ClientQueryBatch *deepclbtch = (ClientQueryBatch *)deepMsg;
    deepclbtch->do_or_pending=clbtch->do_or_pending=1;
    if(g_dag.remote_shard_id==NODE_CNT){
        g_dag.remote_shard_id=0;
    }
    deepclbtch->leader_id=g_dag.remote_shard_id;
    g_dag.remote_shard_id+=g_shard_size;
    if (deepclbtch->leader_id==g_node_id){
            work_queue.enqueue(get_thd_id(), deepclbtch, false);
            return RCOK;
    }else{
        if(deepclbtch->involved_shards[deepclbtch->leader_id/g_shard_size]==false){
            deepclbtch->involved_shards[deepclbtch->leader_id/g_shard_size]=true;
            deepclbtch->involved_shards[g_node_id/g_shard_size]=false;
        }
        vector<uint64_t> dest;
        dest.push_back(deepclbtch->leader_id);
        msg_queue.enqueue(get_thd_id(), deepclbtch, dest);
        dest.clear();
        return RCOK;
    }
}
#endif
#if ZS && TxAllo && !ZSShardBase



        // if(clbtch->do_or_pending==2){

        // }
    // cout <<endl<<"ZS process CLi batchid="<<msg->batch_id
    //     <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;

        if(clbtch->do_or_pending==0){
            g_dag.cnt_batch++;
            char *buf = create_msg_buffer(clbtch);
            Message *deepMsg = deep_copy_msg(buf, clbtch);
            ClientQueryBatch *deepclbtch = (ClientQueryBatch *)deepMsg;

            #if TxAllo
                // cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" bef updateClientQueryBatch txid="<<deepclbtch->txn_id
                //     <<" DoP="<<deepclbtch->do_or_pending
                //     <<" iscross="<<deepclbtch->is_cross_shard <<"leader_id="<<deepclbtch->leader_id;
                //                     for(uint32_t i=0;i<g_shard_cnt;i++){
                //     cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
                // }
                // cout<<endl;
                bool old_is_cross=deepclbtch->is_cross_shard;
                g_dag.updateClientQueryBatch(deepclbtch);
                if(deepclbtch->is_cross_shard!=old_is_cross){
                    cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" after updateClientQueryBatch txid="<<deepclbtch->txn_id
                        <<" DoP="<<deepclbtch->do_or_pending<<" old_iscross="<<old_is_cross
                        <<" newiscross="<<deepclbtch->is_cross_shard <<"leader_id="<<deepclbtch->leader_id;
                    for(uint32_t i=0;i<g_shard_cnt;i++){
                        cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
                    }
                    cout <<endl;
                }


                deepclbtch->do_or_pending=clbtch->do_or_pending=1;
                if(deepclbtch->leader_id==g_node_id){
                    work_queue.enqueue(get_thd_id(), deepclbtch, false);
                    // cout<<" process in this node"<<endl;
                }else{
                    vector<uint64_t> dest;
                    dest.push_back(deepclbtch->leader_id);
                    msg_queue.enqueue(get_thd_id(), deepclbtch, dest);
                    dest.clear();
                    // cout<<endl<<" send to other node:"<<deepclbtch->leader_id<<endl;
                }
            #endif

            if(g_node_id==0&&g_dag.cnt_batch%DAG_SIZE==0&&g_dag.flag_BR==1){
                BatchRequests *BR = g_dag.getmodelBR();
                char *buf = create_msg_buffer(BR);
                Message *deepMsg = deep_copy_msg(buf, BR);
                BatchRequests *breq=(BatchRequests *)deepMsg;
                breq->flag_shard=1;
                breq->shardDag_id=g_dag.cnt_batch/DAG_SIZE;
                cout <<endl<< "ZS: DataMigration Step1: bef_q shardDag_id="<<breq->shardDag_id<<" cnt_b="<< g_dag.cnt_batch<<endl;
                g_dag.storePendingBatch(breq->shardDag_id);
                vector<uint64_t> dest;
                for (uint64_t i = g_shard_size; i < g_node_cnt; i=i+g_shard_size)
                {
                    dest.push_back(i);
                    cout << " dest="<<i<<endl;
                }
                // for (uint64_t i = 0; i < g_node_cnt; i++)
                // {
                //     if(i!=0 && i%g_shard_size==0){
                //         dest.push_back(i);
                //         cout << "ZS1-2: BQ dest="<<i<<endl;
                //     }
                // }
                // cout << "ZS2: BQ shardDag_id="<<breq->shardDag_id
                //     <<"flag_shard=1 send at node:"<<g_node_id<<" cnt_b="<< g_dag.cnt_batch<<endl;
                msg_queue.enqueue(get_thd_id(), breq, dest);
                cout << "ZS: DataMigration Step1: shardDag_id="<<breq->shardDag_id<<" cnt_b="<< g_dag.cnt_batch<<" send by node:"<<g_node_id<<endl;
                dest.clear();
            }
            return RCOK;





                        //     if(((ClientQueryBatch *)msg)->validate()){
                        //             cout <<endl<<"ZS process CLi Vtrue batchid="<<msg->batch_id
                        // <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;
                        //     }else{
                        //                             cout <<endl<<"ZS process CLi Vfalse batchid="<<msg->batch_id
                        // <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;
                        //     }





                            // clbtch->do_or_pending=1;

                            // char *buf = create_msg_buffer(clbtch);
                            // Message *deepMsg = deep_copy_msg(buf, clbtch);
                            // ClientQueryBatch *deepclbtch = (ClientQueryBatch *)deepMsg;
                            // g_dag.cnt_batch++;
                            // if(g_node_id==0&&(g_dag.cnt_batch%10==0||g_dag.cnt_batch%10==1)){
                            //     if(g_dag.cnt_batch%10==0)
                            //     {
                            //         deepclbtch->involved_shards[0]=true;
                            //         deepclbtch->involved_shards[1]=true;
                            //         deepclbtch->is_cross_shard=true;
                            //     }else if(g_dag.cnt_batch%10==1){
                            //         deepclbtch->involved_shards[0]=false;
                            //         deepclbtch->involved_shards[1]=true;
                            //         deepclbtch->is_cross_shard=false;
                            //     }
                            //     deepclbtch->return_node_id=deepclbtch->return_node;
                            //     vector<uint64_t> dest;
                            //     dest.push_back(4);
                            //     msg_queue.enqueue(get_thd_id(), deepclbtch, dest);
                            //     dest.clear();
                            //     cout<<endl<<" process client batch send to other node"<<endl;

                            // }else{
                            //     work_queue.enqueue(get_thd_id(), deepclbtch, false);
                            //     cout<<endl<<" process client batch process in this node"<<endl;
                            // }



        //     cout <<endl<<"ZS process CLi befoer 2V batchid="<<deepclbtch->batch_id
        // <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;


        //                 if(((ClientQueryBatch *)deepclbtch)->validate()){
        //                         cout <<endl<<"ZS process CLi 2Vtrue batchid="<<deepclbtch->batch_id
        //             <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;
        //                 }else{
        //                                         cout <<endl<<"ZS process CLi 2Vfalse batchid="<<deepclbtch->batch_id
        //             <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;
        //                 }

        //                 cout <<endl<<"ZS process CLi after 2V batchid="<<deepclbtch->batch_id
        //                     <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;

                            //         cout << "ZS send to 4"<<endl;

                            // cout <<endl<<"ZS process CLi after enqueue batchid="<<deepclbtch->batch_id
                            // <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;

                            //             if(((ClientQueryBatch *)deepclbtch)->validate()){
                            //             cout <<endl<<"ZS process CLi 3Vtrue batchid="<<deepclbtch->batch_id
                            // <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;
                            //     }else{
                            //                             cout <<endl<<"ZS process CLi 3Vfalse batchid="<<deepclbtch->batch_id
                            // <<" txid="<<deepclbtch->txn_id<<" return_node_id="<<deepclbtch->return_node_id<<" return_node="<<deepclbtch->return_node<<endl;
                            //     } 

            // g_dag.cnt_batch++;
            // work_queue.enqueue(get_thd_id(), deepclbtch, false);

            // cout<<" wq_enq"<<endl;
        }
        if(clbtch->do_or_pending==1){
            g_dag.addDoClbEdge(clbtch);
        }

#endif

#if ZSShardBase

        // cout <<endl<<"test 1"<<endl;
        if(clbtch->do_or_pending==0){

             //cout <<endl<<"test 2"<<endl;
            // g_dag.cnt_batch++;
            char *buf = create_msg_buffer(clbtch);
            Message *deepMsg = deep_copy_msg(buf, clbtch);
            ClientQueryBatch *deepclbtch = (ClientQueryBatch *)deepMsg;
            if(g_dag.flag_BR==0){
                deepclbtch->do_or_pending=clbtch->do_or_pending=1;
                work_queue.enqueue(get_thd_id(), deepclbtch, false);
                return RCOK;
            }   
            

            #if ZSShardM
            // if(g_node_id!=0){
            //         cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" before updateClientQueryBatch txid="<<deepclbtch->txn_id
            //             <<" DoP="<<deepclbtch->do_or_pending
            //             <<" newiscross="<<deepclbtch->is_cross_shard <<"leader_id="<<deepclbtch->leader_id;
            //         for(uint32_t i=0;i<g_shard_cnt;i++){
            //             cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
            //         }
            //         cout <<endl;
            // }
            // cout <<endl<<"test 3"<<endl;
            // bool old_is_cross=deepclbtch->is_cross_shard;
                //     cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" before updateClientQueryBatch txid="<<deepclbtch->txn_id
                //         <<" DoP="<<deepclbtch->do_or_pending<<" old_iscross="<<old_is_cross
                //         <<" newiscross="<<deepclbtch->is_cross_shard <<"leader_id="<<deepclbtch->leader_id;
                //     for(uint32_t i=0;i<g_shard_cnt;i++){
                //         cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
                //     }
                //     cout <<endl;
                // cout <<endl<<"test 4"<<endl;
            g_dag.updateClientQueryBatch(deepclbtch);
            // cout <<endl<<"test 5"<<endl;
            //         cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" after updateClientQueryBatch txid="<<deepclbtch->txn_id
            //             <<" DoP="<<deepclbtch->do_or_pending<<" old_iscross="<<old_is_cross
            //             <<" newiscross="<<deepclbtch->is_cross_shard <<"leader_id="<<deepclbtch->leader_id;
            //         for(uint32_t i=0;i<g_shard_cnt;i++){
            //             cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
            //         }
            //         cout <<endl;
            if(deepclbtch->is_cross_shard==false){
                // cout <<endl<<"test 6"<<endl;
                deepclbtch->do_or_pending=clbtch->do_or_pending=1;
                // if(g_node_id!=0){
                if(g_node_id<100000){
                    // cout <<endl<<"test 7"<<endl;
                    work_queue.enqueue(get_thd_id(), deepclbtch, false);
                    return RCOK;
                    // cout <<endl<<"test 7.1"<<endl;
                }else{
                    // cout <<endl<<"test 8"<<endl;

                    if(g_dag.remote_shard_id==NODE_CNT){
                        g_dag.remote_shard_id=0;
                    }
                    deepclbtch->leader_id=g_dag.remote_shard_id;
                    g_dag.remote_shard_id+=g_shard_size;

                    // deepclbtch->leader_id=4;

                    if(deepclbtch->leader_id==0){
                        work_queue.enqueue(get_thd_id(), deepclbtch, false);
                    }else{
                                for (uint32_t i = 0; i < g_shard_cnt; ++i) {
                                    deepclbtch->involved_shards[i]=false;
                                }
                                deepclbtch->involved_shards[deepclbtch->leader_id/g_shard_size]=true;
                        vector<uint64_t> dest;
                        dest.push_back(deepclbtch->leader_id);
                        // cout <<endl<<"test 9"<<endl;
                        //     cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" bef q txid="<<deepclbtch->txn_id
                        //         <<" DoP="<<deepclbtch->do_or_pending<<" old_iscross="<<old_is_cross
                        //         <<" newiscross="<<deepclbtch->is_cross_shard <<"leader_id="<<deepclbtch->leader_id;
                        //     for(uint32_t i=0;i<g_shard_cnt;i++){
                        //         cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
                        //     }
                        //     cout <<endl;
                        msg_queue.enqueue(get_thd_id(), deepclbtch, dest);
                        // cout <<endl<<"test 10"<<endl;
                        dest.clear();
                        // cout<<endl<<"ZS batchid="<<deepclbtch->batch_id<<" after updateClientQueryBatch txid="<<deepclbtch->txn_id
                        //     <<" DoP="<<deepclbtch->do_or_pending<<" old_iscross="<<old_is_cross
                        //     <<" newiscross="<<deepclbtch->is_cross_shard <<"old leader_id="<<0;
                        //     cout << "g_dag.remote_shard_id="<<deepclbtch->leader_id;
                        // for(uint32_t i=0;i<g_shard_cnt;i++){
                        //     cout <<" s["<<i<<"]="<<deepclbtch->involved_shards[i];
                        // }
                        // cout <<endl;
                    }
                    return RCOK;

                }
                return RCOK;
            }else
            #endif
            {
                g_dag.addpendingBatch(deepclbtch);
                g_dag.addDoClbEdge(deepclbtch);
                if(g_node_id==0&&g_dag.cnt_batch%DAG_SIZE==0&&g_dag.flag_BR==1){
                    BatchRequests *BR = g_dag.getmodelBR();
                    char *buf = create_msg_buffer(BR);
                    Message *deepMsg = deep_copy_msg(buf, BR);
                    BatchRequests *breq=(BatchRequests *)deepMsg;
                    breq->flag_shard=1;
                    breq->shardDag_id=g_dag.cnt_batch/DAG_SIZE;
                    cout <<endl<< "ZS: DataMigration Step1: bef_q shardDag_id="<<breq->shardDag_id<<" cnt_b="<< g_dag.cnt_batch<<endl;
                    g_dag.storePendingBatch(breq->shardDag_id);
                    vector<uint64_t> dest;
                    for (uint64_t i = g_shard_size; i < g_node_cnt; i=i+g_shard_size)
                    {
                        dest.push_back(i);
                        cout << " dest="<<i<<endl;
                    }
                    msg_queue.enqueue(get_thd_id(), breq, dest);
                    cout << "ZS: DataMigration Step1: shardDag_id="<<breq->shardDag_id<<" cnt_b="<< g_dag.cnt_batch<<" send by node:"<<g_node_id<<endl;
                    dest.clear();
                }
                return RCOK;
            }
        }
#endif




    // Authenticate the client signature.
    //cout << "test_v1:begin validate_msg(clbtch)\n";
    validate_msg(clbtch);
                    //     cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" Dopppp=1 txid="<<clbtch->txn_id
                    //     <<" DoP="<<clbtch->do_or_pending
                    //     <<" newiscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
                    // for(uint32_t i=0;i<g_shard_cnt;i++){
                    //     cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
                    // }
                    // cout <<endl;

#if VIEW_CHANGES
    // If message forwarded to the non-primary.
    #if MULTI_ON
    if (!(get_primary(clbtch->txn_id%get_totInstances()) == g_node_id))
    #else
    if (g_node_id != get_current_view(get_thd_id()))
    #endif
    {
        client_query_check(clbtch);
        cout << "returning...   " << get_current_view(get_thd_id()) << endl;
        return RCOK;
    }

    // Partial failure of Primary 0.
    //fail_primary(msg, 10 * BILLION);
#endif

    // Initialize all transaction mangers and Send BatchRequests message.
    //cout << "test_v8:process_client_batch, txn_id = " << msg->txn_id << "\n";
    if (clbtch->is_cross_shard)
        create_and_send_batchreq_cross(clbtch, clbtch->txn_id);
    else
        create_and_send_batchreq(clbtch, clbtch->txn_id);

    return RCOK;
}

void WorkerThread::create_and_send_batchreq_cross(ClientQueryBatch *msg, uint64_t tid)
{
    // Creating a new BatchRequests Message.
    Message *bmsg = Message::create_message(SUPER_PROPOSE);
    SuperPropose *breq = (SuperPropose *)bmsg;
    breq->init(get_thd_id());
    breq->is_cross_shard = true;
    breq->batch_id = msg->batch_id;
    for (uint64_t i = 0; i < g_shard_cnt; i++)
        breq->involved_shards[i] = msg->involved_shards[i];

    // Starting index for this batch of transactions.
    next_set = tid;

// #if SHARPER_ZS1
//     cout << "SHARPER_ZS1"<<g_sharper_zs1[0]<<"\n";
// #endif

// #if SHARPER_ZS2
//     cout << "SHARPER_ZS2\n";
// #endif

// #if SHARPER_ZS3
//     cout << "SHARPER_ZS3\n";
// #endif

// #if SHARPER_ZS4
//     cout << "SHARPER_ZS4\n";
// #endif

    // String of transactions in a batch to generate hash.
    string batchStr;

    // Allocate transaction manager for all the requests in batch.
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        uint64_t txn_id = get_next_txn_id() + i;
        //cout << "test_v8:create_and_send_batchreq_cross, txn_id = " << txn_id << "source=breq" << get_thd_id() << "\n";

        //cout << "Txn: " << txn_id << " :: Thd: " << get_thd_id() << "\n";
        //fflush(stdout);
        txn_man = get_transaction_manager(g_net_id, txn_id, breq->batch_id);

        // Unset this txn man so that no other thread can concurrently use.
        unset_ready_txn(txn_man);

        txn_man->net_id = g_net_id;
        txn_man->register_thread(this);
        txn_man->return_id = msg->return_node;

        // Fields that need to updated according to the specific algorithm.
        algorithm_specific_update(msg, i);

        init_txn_man(msg->cqrySet[i]);

        // Append string representation of this txn.
        batchStr += msg->cqrySet[i]->getString();

        // Setting up data for BatchRequests Message.
        breq->copy_from_txn(txn_man, msg->cqrySet[i]);

    #if ISEOV
        // cout << "test_v5:create_and_send_batchreq::before_simulate\n";
        #if PRE_EX
        txn_man->simulate_txn(breq->readSet[i], breq->writeSet[i], *speculateSet);
        #else
            txn_man->simulate_txn(breq->readSet[i], breq->writeSet[i]);
        #endif
    #endif

        // Reset this txn manager.
        bool ready = txn_man->set_ready();
        assert(ready);
    }

    // Now we need to unset the txn_man again for the last txn of batch.
    unset_ready_txn(txn_man);

    // Generating the hash representing the whole batch in last txn man.
    txn_man->set_hash(calculateHash(batchStr));
    txn_man->hashSize = txn_man->hash.length();

    breq->copy_from_txn(txn_man);

    // Storing the BatchRequests message.
    txn_man->set_primarybatch(breq);

    if (breq->is_cross_shard)
    {
        digest_directory.add(breq->hash, breq->txn_id / get_batch_size());
        // cout << "digest setted " << digest_directory.get(breq->hash) << endl;
        txn_man->is_cross_shard = true;
        for (uint64_t i = 0; i < g_shard_cnt; i++)
        {
            txn_man->involved_shards[i] = breq->involved_shards[i];
        }
    }

    // Storing all the signatures.
    vector<uint64_t> dest;

    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
        // Do not send to itself and not involved shards
        // 如果节点是当前节点，或者节点不在涉及的分片中，则跳过；如shardsize=4，如果当前事务跨AC片，节点是0，跳过0或4567
        if (i == g_node_id || !msg->involved_shards[get_shard_number(i)])
        {
            continue;
        }
        // Do not send to other shards replicas
        if (!is_in_same_shard(i, g_node_id) && !is_primary_node(get_thd_id(), i))
        {
            continue;
        }
        dest.push_back(i);
        breq->sign(i);
    }
        // cout<< "test_v8:create_and_send_batchreq_cross, txn_id"<< breq->txn_id << "involved_shards: ";
        // for(uint64_t i = 0; i < g_shard_cnt; i++) {
        //     if(breq->involved_shards[i]) {
        //         cout << i << " ";
        //     }
        // }
        // cout << "source: " << breq->requestMsg[0]->inputs[0] << " dest: " << breq->requestMsg[0]->inputs[2];
        // cout << "\n";

    msg_queue.enqueue(get_thd_id(), breq, dest);

}
/**
 * Process incoming BatchRequests message from the Primary.
 *
 * This function is used by the non-primary or backup replicas to process an incoming
 * BatchRequests message sent by the primary replica. This processing would require 
 * sending messages of type PBFTPrepMessage, which correspond to the Prepare phase of 
 * the PBFT protocol. Due to network delays, it is possible that a repica may have 
 * received some messages of type PBFTPrepMessage and PBFTCommitMessage, prior to 
 * receiving this BatchRequests message.
 *
 * @param msg Batch of Transactions of type BatchRequests from the primary.
 * @return RC
 */
RC WorkerThread::process_batch(Message *msg)
{
    uint64_t cntime = get_sys_clock();

    BatchRequests *breq = (BatchRequests *)msg;
    #if KDK_DEBUG1
    printf("BatchRequests: TID:%ld : VIEW: %ld : THDB: %ld\n",breq->txn_id, breq->view, get_thd_id());
    fflush(stdout);
    #endif
#if ZS
    if(g_dag.flag_BR==0){
        g_dag.setmodelBR(breq);
    }
#endif


#if ZS
        if(msg->rtype==BATCH_REQ){
            BatchRequests *breq = (BatchRequests *)msg;
            if (breq->flag_shard==1){
                cout << "ZS: BQ shardDag_id="<<breq->shardDag_id
                    <<"flag_shard=1 rec at node:"<<g_node_id<<endl;
                return RCOK;
                // Message *bmsg = Message::create_message(BATCH_REQ);
                // BatchRequests *response = (BatchRequests *)bmsg;
                // response->init(get_thd_id());
                // response->flag_shard=2;
                // response->shardDag_id=breq->shardDag_id;
                // response->size_ShardDAG=response->ShardDAG.size();
                // vector<uint64_t> dest;
                // dest.push_back(0);
                // msg_queue.enqueue(get_thd_id(), response, dest);
                // dest.clear();
                // cout << "ZS: BQ shardDag_id="<<breq->shardDag_id
                //     <<"flag_shard=2 send at node:"<<g_node_id<<endl;
                // Message::release_message(msg);
                // continue;
            }
            // if(g_node_id==0 && breq->flag_shard==2){
            //     cout << "ZS: BQ shardDag_id="<<breq->shardDag_id
            //         <<"flag_shard=2 rec at node:"<<g_node_id<<endl;
            //     Message *bmsg = Message::create_message(BATCH_REQ);
            //     BatchRequests *response = (BatchRequests *)bmsg;
            //     response->init(get_thd_id());
            //     response->flag_shard=3;
            //     response->shardDag_id=breq->shardDag_id;
            //     response->size_ShardDAG=response->ShardDAG.size();
            //     vector<uint64_t> dest;
            //     for (uint64_t i = g_shard_size; i < g_node_cnt; i=i+g_shard_size)
            //     {
            //         dest.push_back(i);
            //     }
            //     msg_queue.enqueue(get_thd_id(), response, dest);
            //     dest.clear();
            //     cout << "ZS: BQ shardDag_id="<<breq->shardDag_id
            //         <<"flag_shard=3 send at node:"<<g_node_id<<endl;
            //     Message::release_message(msg);
            //     continue;
            // }
            // if(breq->flag_shard==3){
            //     cout << "ZS: BQ shardDag_id="<<breq->shardDag_id
            //         <<"flag_shard=3 rec at node:"<<g_node_id<<endl;
            //     Message::release_message(msg);
            //     continue;
            // }
        }
#endif

    // Assert that only a non-primary replica has received this message.
    #if MULTI_ON
    assert(!isPrimary(g_node_id) || !(breq->view == g_node_id));
    #else
    assert(g_node_id != get_current_view(get_thd_id()));
    #endif

    // Check if the message is valid.
    validate_msg(breq);

#if VIEW_CHANGES
    // Store the batch as it could be needed during view changes.
    store_batch_msg(breq);
#endif

    // Allocate transaction managers for all the transactions in the batch.
    set_txn_man_fields(breq, breq->batch_id, g_net_id);
    //cout << "test_v3:process_batch:set_txn_man_fields(breq, breq->batch_id);,  batch_id = "<< breq->batch_id << "\n";

#if TIMER_ON
    // The timer for this client batch stores the hash of last request.
    add_timer(breq, txn_man->get_hash());
#endif

    if (breq->is_cross_shard)
    {
        digest_directory.add(breq->hash, breq->txn_id / get_batch_size());
        // cout << "digest setted " << digest_directory.get(breq->hash) << endl;
        txn_man->is_cross_shard = true;
        for (uint64_t i = 0; i < g_shard_cnt; i++)
        {
            txn_man->involved_shards[i] = breq->involved_shards[i];
        }
    }


    txn_man->set_primarybatch(breq);
    //DEBUG("test_v5:txn_man->set_primarybatch(breq)::txn_man->txnid == %ld\n", txn_man->get_txn_id());
// #if ISEOV
//     // Storing the BatchRequests message.
//     DEBUG("test_v5:print process_batch_readSet\n");
//     uint64_t process_count = 0;
//     for(const auto &item:txn_man->batchreq->readSet){
//         DEBUG("test_v5:print process_batch_readSet[%ld]\n", process_count++);
//         for(const auto &item2:item){
//             DEBUG("test_v5:key = %ld, value = %ld\n", item2.first, item2.second);
//         }
//     }
// #endif

    // Send Prepare messages.
    txn_man->send_pbft_prep_msgs();

    // End the counter for pre-prepare phase as prepare phase starts next.
    double timepre = get_sys_clock() - cntime;
    INC_STATS(get_thd_id(), time_pre_prepare, timepre);

    // Only when BatchRequests message comes after some Prepare message.
    for (uint64_t i = 0; i < txn_man->info_prepare.size(); i++)
    {
        // Decrement.
        if (breq->is_cross_shard)
        {
            txn_man->decr_prep_rsp_cnt(get_shard_number(txn_man->info_prepare[i]));
            bool i_prep = true;
            for (uint64_t i = 0; i < g_shard_cnt; i++)
            {
                if (txn_man->prep_rsp_cnt_arr[i] != 0)
                    i_prep = false;
            }
            if (i_prep)
            {
                txn_man->set_prepared();
                break;
            }
        }
        else
        {
            // Decrement.
            uint64_t num_prep = txn_man->decr_prep_rsp_cnt();
            if (num_prep == 0)
            {
                txn_man->set_prepared();
                break;
            }
        }
    }

    // If enough Prepare messages have already arrived.
    if (txn_man->is_prepared())
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        double timeprep = get_sys_clock() - txn_man->txn_stats.time_start_prepare - timepre;
        INC_STATS(get_thd_id(), time_prepare, timeprep);
        double timediff = get_sys_clock() - cntime;

        // Check if any Commit messages arrived before this BatchRequests message.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            uint64_t num_comm = txn_man->decr_commit_rsp_cnt();
            if (num_comm == 0)
            {
                txn_man->set_committed();
                break;
            }
        }

        // If enough Commit messages have already arrived.
        if (txn_man->is_committed())
        {
#if TIMER_ON
            // End the timer for this client batch.
            remove_timer(txn_man->hash);
#endif
#if P2P_BROADCAST && MULTI_ON
            Message *bmsg = Message::create_message(BROADCAST_BATCH);
            BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)bmsg;
            bbmsg->init();
            bbmsg->batch_id = txn_man->get_batch_id();
            bbmsg->txn_id = txn_man->get_txn_id();
            bbmsg->net_id = g_net_id;
            
            char *buf = create_msg_buffer(txn_man->batchreq);
            Message *deepMsg = deep_copy_msg(buf, txn_man->batchreq);

            bbmsg->add_batch((BatchRequests *)deepMsg);
            for(auto &item:txn_man->commit_msgs)
            {
                bbmsg->add_commit_msg(item);
            }
            vector<uint64_t> dest;

	        for (uint64_t i = 0; i < g_node_cnt; i++)
	        {
            	if (i % g_net_cnt == g_net_id || (i / g_net_cnt != g_net_id / g_net_cnt))
	        	{
	        		continue;
	        	}
                dest.push_back(i);
            }
            msg_queue.enqueue(get_thd_id(), bbmsg, dest);
#endif
            // Proceed to executing this batch of transactions.
            //cout << "test_v8:process_batch::before send_execute_msg() = txn_id = " <<  txn_man->get_txn_id() <<"\n";
            send_execute_msg();

            // End the commit counter.
            INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit - timediff);
        }
    }
    else
    {
        // Although batch has not prepared, still some commit messages could have arrived.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            txn_man->decr_commit_rsp_cnt();
        }
    }

    // Release this txn_man for other threads to use.
    bool ready = txn_man->set_ready();
    assert(ready);

    // UnSetting the ready for the txn id representing this batch.
    txn_man = get_transaction_manager(g_net_id, msg->txn_id, msg->batch_id);
    unset_ready_txn(txn_man);

    return RCOK;
}

RC WorkerThread::process_super_propose(Message *msg)
{

    SuperPropose *breq = (SuperPropose *)msg;
    assert(breq->is_cross_shard);
    assert(breq->involved_shards[get_shard_number(g_node_id)]);
    // printf("SuperPropose: %ld : from: %ld\n", breq->txn_id, breq->return_node_id);
    // fflush(stdout);

    // Assert that only a non-primary replica has received this message.
    assert(is_primary_node(get_thd_id(), g_node_id));
    assert(breq->is_cross_shard);
    assert(breq->involved_shards[get_shard_number(g_node_id)]);
    // Check if the message is valid.
    ///////////////////////////////////////

    // Starting index for this batch of transactions.
    next_set = breq->txn_id;
    breq->index.release();
    breq->index.init(get_batch_size());

    // Allocate transaction manager for all the requests in batch.
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        uint64_t txn_id = get_next_txn_id() + i;
        //cout << "test_v8:process_super_propose, txn_id = " << txn_id << "\n";

        //cout << "Txn: " << txn_id << " :: Thd: " << get_thd_id() << "\n";
        //fflush(stdout);
        txn_man = get_transaction_manager(g_net_id, txn_id, breq->batch_id);

        // Unset this txn man so that no other thread can concurrently use.
        unset_ready_txn(txn_man);

        txn_man->register_thread(this);
        txn_man->return_id = msg->return_node_id;

        // Fields that need to updated according to the specific algorithm.
        algorithm_specific_update(msg, i);

        init_txn_man(breq->requestMsg[i]);
        breq->index.add(txn_man->get_txn_id());

        // Reset this txn manager.
        bool ready = txn_man->set_ready();
        assert(ready);
    }

    // Now we need to unset the txn_man again for the last txn of batch.
    unset_ready_txn(txn_man);

    // Generating the hash representing the whole batch in last txn man.
    txn_man->set_hash(breq->hash);
    txn_man->hashSize = txn_man->hash.length();

    // Storing the BatchRequests message.
    txn_man->set_primarybatch((BatchRequests *)(breq));

    if (breq->is_cross_shard)
    {
        txn_man->is_cross_shard = true;
        for (uint64_t i = 0; i < g_shard_cnt; i++)
        {
            txn_man->involved_shards[i] = breq->involved_shards[i];
        }
    }

    //////////////////////////////////////
    breq->txn_id = txn_man->get_txn_id() - 2;
    digest_directory.add(breq->hash, breq->txn_id / get_batch_size());
    // cout << "digest setted " << digest_directory.get(breq->hash) << endl;
    char *buf = create_msg_buffer(breq);
    Message *deepMsg = deep_copy_msg(buf, breq);
    BatchRequests *batch_req_message = (BatchRequests *)deepMsg;
    batch_req_message->rtype = BATCH_REQ;
    delete_msg_buffer(buf);
    vector<uint64_t> dest;

    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
        if (i == g_node_id || !is_in_same_shard(i, g_node_id))
        {
            continue;
        }
        dest.push_back(i);

        batch_req_message->sign(i);
    }
    msg_queue.enqueue(get_thd_id(), batch_req_message, dest);
    return RCOK;
}

/**
 * Processes incoming Prepare message.
 *
 * This functions precessing incoming messages of type PBFTPrepMessage. If a replica 
 * received 2f identical Prepare messages from distinct replicas, then it creates 
 * and sends a PBFTCommitMessage to all the other replicas.
 *
 * @param msg Prepare message of type PBFTPrepMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_prep_msg(Message *msg)
{
    #if KDK_DEBUG1
    // cout << "PBFTPrepMessage: TID: " << msg->txn_id << " FROM: " << msg->return_node_id << " THDP: " << get_thd_id() << endl;
    fflush(stdout);
    #endif
    // Start the counter for prepare phase.
#if MINI_NET
    if (txn_man->prep_rsp_cnt == g_min_invalid_nodes)
#else
    if (txn_man->prep_rsp_cnt == 2 * g_min_invalid_nodes)
#endif
    {
        txn_man->txn_stats.time_start_prepare = get_sys_clock();
    }

    // Check if the incoming message is valid.
    PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
    //cout << "test_v8:process_pbft_prep_msg, txn_id = " << pmsg->txn_id << "\n";
    validate_msg(pmsg);

    // Check if sufficient number of Prepare messages have arrived.
    if (prepared(pmsg))
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        // End the prepare counter.
        INC_STATS(get_thd_id(), time_prepare, get_sys_clock() - txn_man->txn_stats.time_start_prepare);
    }

    return RCOK;
}

/**
 * Checks if the incoming PBFTCommitMessage can be accepted.
 *
 * This functions checks if the hash and view of the commit message matches that of 
 * the Pre-Prepare message. Once 2f+1 messages are received it returns a true and 
 * sets the `is_committed` flag for furtue identification.
 *
 * @param msg PBFTCommitMessage.
 * @return bool True if the transactions of this batch can be executed.
 */
bool WorkerThread::committed_local(PBFTCommitMessage *msg)
{
    //cout << "test_v8:committed_local, txn_id = " << txn_man->get_txn_id() << "\n";
    //cout << "Check Commit: TID: " << txn_man->get_txn_id() << "\n";
    //fflush(stdout);

    // Once committed is set for this transaction, no further processing.
    if (txn_man->is_committed())
    {
        return false;
    }

    // If BatchRequests messages has not arrived, then hash is empty; return false.
    if (txn_man->get_hash().empty())
    {
        //cout << "hash empty: " << txn_man->get_txn_id() << "\n";
        //fflush(stdout);
        txn_man->info_commit.push_back(msg->return_node);
        return false;
    }
    else
    {
        if (!checkMsg(msg))
        {
            // If message did not match.
            //cout << txn_man->get_hash() << " :: " << msg->hash << "\n";
            //cout << get_current_view(get_thd_id()) << " :: " << msg->view << "\n";
            //fflush(stdout);
            return false;
        }
    }

    if (msg->is_cross_shard)
    {
        // cout << "counting cross shard commits " << msg->return_node_id << "  " << msg->txn_id << endl;
        txn_man->decr_commit_rsp_cnt(get_shard_number(msg->return_node_id));
        for (uint64_t i = 0; i < g_shard_cnt; i++)
        {
            if (txn_man->commit_rsp_cnt_arr[i] != 0 && txn_man->involved_shards[i])
                return false;
        }
        txn_man->set_committed();
        return true;
    }
    uint64_t comm_cnt = txn_man->decr_commit_rsp_cnt();
    if (comm_cnt == 0 && txn_man->is_prepared())
    {
        txn_man->set_committed();
        return true;
    }

    return false;
}

/**
 * Processes incoming Commit message.
 *
 * This functions precessing incoming messages of type PBFTCommitMessage. If a replica 
 * received 2f+1 identical Commit messages from distinct replicas, then it asks the 
 * execute-thread to execute all the transactions in this batch.
 *
 * @param msg Commit message of type PBFTCommitMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_commit_msg(Message *msg)
{
    #if KDK_DEBUG1
    // cout << "PBFTCommitMessage: TID " << msg->txn_id << " FROM: " << msg->return_node_id << " THDC: " << get_thd_id() << "\n";
    fflush(stdout);
    #endif
#if MINI_NET
    if (txn_man->commit_rsp_cnt == g_min_invalid_nodes + 1)
#else
    if (txn_man->commit_rsp_cnt == 2 * g_min_invalid_nodes + 1)
#endif
    {
        txn_man->txn_stats.time_start_commit = get_sys_clock();
    }

    // Check if message is valid.
    PBFTCommitMessage *pcmsg = (PBFTCommitMessage *)msg;
    //cout << "test_v4:process_pbft_commit_msg::pcmsg->batch_id = "<<pcmsg->batch_id<<"\n";
    validate_msg(pcmsg);

    txn_man->add_commit_msg(pcmsg);

    // Check if sufficient number of Commit messages have arrived.
    if (committed_local(pcmsg))
    {
        #if STRONG_SERIAL
            if(isPrimary(g_node_id)){
                sem_post(&consensus_lock);
            }
            // else{
            //     cout <<"test_v4:not primary!!\n";
            // }
        #endif
        //cout << "test_v4:process_pbft_commit_msg::committed_local:pcmsg->batch_id = "<<pcmsg->batch_id<<"\n";
#if TIMER_ON
        // End the timer for this client batch.
        remove_timer(txn_man->hash);
#endif

        // if node is the pri of this batch, build BBmsg and sendl
#if NET_BROADCAST && MULTI_ON
#if !P2P_BROADCAST
        if (priconsensus[msg->batch_id] == true)
#else 
        if(true)
#endif
        {

            Message *bmsg = Message::create_message(BROADCAST_BATCH);
            BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)bmsg;
            bbmsg->init();
            bbmsg->batch_id = pcmsg->batch_id;
            bbmsg->txn_id = pcmsg->txn_id;
            bbmsg->net_id = g_net_id;
            
            char *buf = create_msg_buffer(txn_man->batchreq);
            Message *deepMsg = deep_copy_msg(buf, txn_man->batchreq);

            bbmsg->add_batch((BatchRequests *)deepMsg);
            //cout << "test_v4: bbmsg->add_batch(txn_man->batchreq); batchreq_id = " << bbmsg->breq->batch_id <<"\n";
            for(auto &item:txn_man->commit_msgs)
            {
                bbmsg->add_commit_msg(item);
            }
            vector<uint64_t> dest;

	        for (uint64_t i = 0; i < g_node_cnt; i++)
	        {
            #if P2P_BROADCAST
            	if (i % g_net_cnt == g_net_id || (i / g_net_cnt != g_node_id / g_net_cnt))
	        	{
	        		continue;
	        	}
                //DEBUG_V1("test_v6: send bbmsg to %ld \n", i);
                dest.push_back(i);
            #else
	        	if (i % g_net_cnt == g_net_id)
	        	{
	        		continue;
	        	}
                dest.push_back(i);
            #endif
            }
            //cout << "test_v4:send_broadcast_batch::bbmsg->txn_id = " << bbmsg->txn_id<< ", batch_id = " << bbmsg->batch_id << ", net_id = "<< bbmsg->net_id << ", return_node_id = "<< bbmsg->return_node_id<< ", dest_size = "<< dest.size() << "\n";
            //cout << "test_v4:send_broadcast_batch::, breq->get_size() = " <<  bbmsg->breq->get_size() <<"\n";
            msg_queue.enqueue(get_thd_id(), bbmsg, dest);

            //cout << "test_v4:process_pbft_commit_msg::leader::before send_execute_msg() = txn_id = " <<  pcmsg->txn_id <<"\n";
            send_execute_msg();
        }
        else
#endif 
        {
            //cout << "test_v4:process_pbft_commit_msg::before send_execute_msg() = txn_id = " <<  pcmsg->txn_id <<"\n";
            send_execute_msg();
        }

        // Add this message to execute thread's queue.
        //send_execute_msg();

        INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit);
    }

    return RCOK;
}


#if NET_BROADCAST
RC WorkerThread::process_broadcast_batch(Message *msg)
{
    BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)msg;
    //cout << "test_v4:process_broadcast_batch::bbmsg->txn_id = " << bbmsg->txn_id<< ", batch_id = " << bbmsg->batch_id << ", net_id = "<< bbmsg->net_id << ", return_node_id = "<< bbmsg->return_node_id << "\n";
    //cout << "test_v4:process_broadcast_batch::, breq->get_size() = " <<  bbmsg->breq->get_size() <<"\n";
    validate_msg(bbmsg);

    set_txn_man_fields(bbmsg->breq, bbmsg->batch_id, bbmsg->net_id);
    //txn_man->set_ready();
    //cout << "test_v4:process_broadcast_batch::before set_primarybatch::txn_man->net_id = " << txn_man->net_id << ", txn_id = "<< txn_man->get_txn_id() << "\n";

    txn_man->set_primarybatch(bbmsg->breq);
    //cout << "test_v4:process_broadcast_batch::before send_execute_msg()::bbmsg->txn_id = " << bbmsg->txn_id<< ", batch_id = " << bbmsg->batch_id << ", net_id = "<< bbmsg->net_id << ", return_node_id = "<< bbmsg->return_node_id << "\n";
    send_execute_msg();

    return RCOK;
}
#endif
#endif