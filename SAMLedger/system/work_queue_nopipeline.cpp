#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include <boost/lockfree/queue.hpp>

#if !ENABLE_PIPELINE

// If pipeline is disabled then only worker thread 0 and execution thread
// are working at primary replica.
void QWorkQueue::enqueue(uint64_t thd_id, Message *msg, bool busy)
{
    uint64_t starttime = get_sys_clock();
    assert(msg);
#if ZS

    if (msg->rtype == SUPER_PROPOSE && is_primary_node(thd_id, g_node_id))
    {
        // SuperPropose *breq = (SuperPropose *)msg;
            
        // cout<<endl<<"ZS batchid="<<msg->batch_id<<" txid="<<msg->txn_id
        // <<" iscross="<<msg->is_cross_shard;
        // for(uint32_t i=0;i<g_shard_cnt;i++){
        //     cout <<" s["<<i<<"]="<<breq->involved_shards[i];
        // }
        // cout<<" sp bef_getid"<<endl;

        msg->txn_id = get_and_inc_next_idx();

        // cout<<endl<<"ZS batchid="<<msg->batch_id<<" txid="<<msg->txn_id
        // <<" iscross="<<msg->is_cross_shard;
        // for(uint32_t i=0;i<g_shard_cnt;i++){
        //     cout <<" s["<<i<<"]="<<breq->involved_shards[i];
        // }
        // cout<<" sp aft_getid"<<endl;
    }
    if (msg->rtype == CL_BATCH )
    {
    // cout <<endl<<"ZS wkqueue CLi batchid="<<msg->batch_id
    //     <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<endl;
        ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;
        // cout <<endl<<"ZS queue before V batchid="<<msg->batch_id
        // <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;

        if (clbtch->do_or_pending==1)
        {
        //                 if(((ClientQueryBatch *)msg)->validate()){
        //             cout <<endl<<"ZS queue Vtrue batchid="<<msg->batch_id
        // <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;
        //     }else{
        //                             cout <<endl<<"ZS queue Vfalse batchid="<<msg->batch_id
        // <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;
        //     }




            msg->txn_id = get_and_inc_next_idx();
            msg->batch_id = msg->txn_id;
            // cout<<endl<<"ZS batchid="<<msg->batch_id<<" txid="<<msg->txn_id
            // <<" DoP="<<clbtch->do_or_pending
            // <<" iscross="<<msg->is_cross_shard;
            // for(uint32_t i=0;i<g_shard_cnt;i++){
            //     cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
            // }
            // cout<<" getid"<<endl;

            // ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;
        //         cout <<endl<<"ZS process CLi batchid="<<msg->batch_id
        // <<" txid="<<msg->txn_id<<" return_node_id="<<msg->return_node_id<<" return_node="<<clbtch->return_node<<endl;
        }else if(clbtch->do_or_pending==0){
            // cout<<endl<<"ZS batchid="<<msg->batch_id<<" txid="<<msg->txn_id
            // <<" DoP="<<clbtch->do_or_pending
            // <<" iscross="<<msg->is_cross_shard;
            // for(uint32_t i=0;i<g_shard_cnt;i++){
            //     cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
            // }
            // cout<<" ungetid"<<endl;
        }
    }
#endif
    DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
    work_queue_entry *entry = (work_queue_entry *)mem_allocator.align_alloc(sizeof(work_queue_entry));
    entry->msg = msg;
    entry->rtype = msg->rtype;
    entry->txn_id = msg->txn_id;
    entry->batch_id = msg->batch_id;
    entry->starttime = get_sys_clock();
    assert(ISSERVER || ISREPLICA);
    //DEBUG("Work Enqueue (%ld,%ld) %d\n", entry->txn_id, entry->batch_id, entry->rtype);
#if ZS
if(msg->rtype==BATCH_REQ){
    BatchRequests *breq = (BatchRequests *)msg;
    if (breq->flag_shard!=0){
        cout <<endl<<"ZS iothread BR batchid="<<breq->batch_id
        <<" txid="<<breq->txn_id<<" flag_shard="<<breq->flag_shard
        <<" shardDag_id="<<breq->shardDag_id<<endl;
    }
}
#endif
#if SHARPER || NEW_SHARPER
    if (msg->rtype == CL_QRY || msg->rtype == CL_BATCH)
    {
        if (g_node_id == view_to_primary(get_current_view(thd_id)))
#else
    if (msg->rtype == CL_QRY || msg->rtype == CL_BATCH)
    {
        //if (g_node_id == get_current_view(thd_id))
        if (isPrimary(g_node_id))
#endif
        {
            while (!new_txn_queue->push(entry) && !simulation->is_done())
            {
            }
        }
        else
        {
            while (!work_queue[0]->push(entry) && !simulation->is_done())
            {
            }
        }
    }
    else
    {
        assert(entry->rtype < 100);
        if (msg->rtype == EXECUTE_MSG)
        {
            ExecuteMessage *exec_msg = (ExecuteMessage *)msg;
            uint64_t bid = ((msg->txn_id+2) - get_batch_size()) / get_batch_size();
            uint64_t qid = ((bid * g_net_cnt + exec_msg->net_id) % indexSize) + 1;
            //DEBUG_V1("EXECUTE_MSG Enqueue (%ld,%ld) %d\n", entry->txn_id, entry->batch_id, entry->rtype)
            while(!work_queue[qid]->push(entry) && !simulation->is_done()) {}

            // while (!work_queue[indexSize + 1]->push(entry) && !simulation->is_done())
            // {
            // }

        }
        else if (msg->rtype == PBFT_CHKPT_MSG)
        {
            while(!work_queue[indexSize + 1]->push(entry) && !simulation->is_done()) {}
            cout << "test_v4:err: get unexpected PBFT_CHKPT_MSG\n";
            assert(0);
        }
        else if (msg->rtype == BROADCAST_BATCH)
        {
            while(!work_queue[indexSize + 2]->push(entry) && !simulation->is_done()) {}
        }
        else
        {
            while (!work_queue[0]->push(entry) && !simulation->is_done())
            {
            }
        }
    }

    INC_STATS(thd_id, work_queue_enqueue_time, get_sys_clock() - starttime);
    INC_STATS(thd_id, work_queue_enq_cnt, 1);
}

Message *QWorkQueue::dequeue(uint64_t thd_id)
{
    uint64_t starttime = get_sys_clock();
    assert(ISSERVER || ISREPLICA);
    Message *msg = NULL;
    work_queue_entry *entry = NULL;

    bool valid = false;

    // Thread 0 only looks at work queue
    if (thd_id == 0)
    {
        valid = work_queue[0]->pop(entry);
    }
    if (thd_id == 1)
    {
        valid = new_txn_queue->pop(entry);
    }

#if EXECUTION_THREAD
#if DISABLE_CHECKPOINT
    UInt32 tcount = g_thread_cnt - g_execute_thd - 1;
#else
    UInt32 tcount = g_thread_cnt - g_checkpointing_thd - g_execute_thd - 1;
#endif
    if (thd_id >= tcount && thd_id < tcount + g_execute_thd)
    {
        uint64_t bid = ((get_expectedExecuteCount()+2) - get_batch_size()) /get_batch_size();
        uint64_t qid = ((bid * g_net_cnt + get_expectedExecuteNetId()) % indexSize) + 1;
        valid = work_queue[qid]->pop(entry);
    }
#if !DISABLE_CHECKPOINT
    if (thd_id >= tcount + g_execute_thd && thd_id < tcount + g_execute_thd + g_checkpointing_thd)
    {
        valid = work_queue[indexSize + 1]->pop(entry);
    }
#endif
#if NET_BROADCAST
#if DISABLE_CHECKPOINT
    if (thd_id >= tcount + g_execute_thd && thd_id < tcount + g_execute_thd + 1)
#else
    if (thd_id >= tcount + g_execute_thd + g_checkpointing_thd && thd_id < tcount + g_execute_thd + g_checkpointing_thd + 1)
#endif
    {
        valid = work_queue[indexSize + 1 + 1]->pop(entry);
    }
#endif
#endif

    if (!valid)
    {
        // Allowing new transactions to be accessed by batching threads.
    }

    if (valid)
    {
        //DEBUG_V1("test_v7:thread %ld valid\n", thd_id);
        msg = entry->msg;
        assert(msg);
        uint64_t queue_time = get_sys_clock() - entry->starttime;
        INC_STATS(thd_id, work_queue_wait_time, queue_time);
        INC_STATS(thd_id, work_queue_cnt, 1);

        msg->wq_time = queue_time;
        DEBUG("Work Dequeue (%ld,%ld)\n", entry->txn_id, entry->batch_id);
        mem_allocator.free(entry, sizeof(work_queue_entry));
        INC_STATS(thd_id, work_queue_dequeue_time, get_sys_clock() - starttime);
    }

    return msg;
}

#endif // ENABLE_PIPELINE == false
