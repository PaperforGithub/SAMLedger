#include "txn.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "array.h"

void TxnStats::init()
{
    starttime = 0;
    wait_starttime = get_sys_clock();
    total_process_time = 0;
    process_time = 0;
    //time_span = 0;
    total_local_wait_time = 0;
    local_wait_time = 0;
    total_remote_wait_time = 0;
    remote_wait_time = 0;
    write_cnt = 0;
    abort_cnt = 0;

    total_work_queue_time = 0;
    work_queue_time = 0;
    total_work_queue_cnt = 0;
    work_queue_cnt = 0;
    total_msg_queue_time = 0;
    msg_queue_time = 0;
    total_abort_time = 0;
    time_start_pre_prepare = 0;
    time_start_prepare = 0;
    time_start_commit = 0;
    time_start_execute = 0;

    clear_short();
}

void TxnStats::clear_short()
{

    work_queue_time_short = 0;
    cc_block_time_short = 0;
    cc_time_short = 0;
    msg_queue_time_short = 0;
    process_time_short = 0;
    network_time_short = 0;
}

void TxnStats::reset()
{
    wait_starttime = get_sys_clock();
    total_process_time += process_time;
    process_time = 0;
    total_local_wait_time += local_wait_time;
    local_wait_time = 0;
    total_remote_wait_time += remote_wait_time;
    remote_wait_time = 0;
    write_cnt = 0;

    total_work_queue_time += work_queue_time;
    work_queue_time = 0;
    total_work_queue_cnt += work_queue_cnt;
    work_queue_cnt = 0;
    total_msg_queue_time += msg_queue_time;
    msg_queue_time = 0;

    clear_short();
}

void TxnStats::abort_stats(uint64_t thd_id)
{
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_work_queue_time += work_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_work_queue_cnt += work_queue_cnt;
    assert(total_process_time >= process_time);
}

void TxnStats::commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long,
                            uint64_t timespan_short)
{
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_work_queue_time += work_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_work_queue_cnt += work_queue_cnt;
    //time_span += timespan_short;
    assert(total_process_time >= process_time);

    if (IS_LOCAL(txn_id))
    {
        PRINT_LATENCY("lat_s %ld %ld %f %f %f %f\n", txn_id, work_queue_cnt, (double)timespan_short / BILLION, (double)work_queue_time / BILLION, (double)msg_queue_time / BILLION, (double)process_time / BILLION);
    }
    else
    {
        PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f\n", txn_id, work_queue_cnt, (double)timespan_short / BILLION, (double)total_work_queue_time / BILLION, (double)total_msg_queue_time / BILLION, (double)total_process_time / BILLION);
    }

    if (!IS_LOCAL(txn_id))
    {
        return;
    }
}

void Transaction::init()
{
    txn_id = UINT64_MAX;
    batch_id = UINT64_MAX;

    reset(0);
}

void Transaction::reset(uint64_t pool_id)
{
    rc = RCOK;
}

void Transaction::release(uint64_t pool_id)
{
    DEBUG("Transaction release\n");
}

void TxnManager::init(uint64_t pool_id, Workload *h_wl)
{
    if (!txn)
    {
        DEBUG_M("Transaction alloc\n");
        txn_pool.get(pool_id, txn);
    }
#if !BANKING_SMART_CONTRACT
    if (!query)
    {
        DEBUG_M("TxnManager::init Query alloc\n");
        qry_pool.get(pool_id, query);
        // this->query->init();
    }
#endif
    sem_init(&rsp_mutex, 0, 1);
    return_id = UINT64_MAX;

    this->h_wl = h_wl;

    txn_ready = true;

    prepared = false;
    committed_local = false;
#if MINI_NET
    prep_rsp_cnt = g_min_invalid_nodes;
    commit_rsp_cnt = g_min_invalid_nodes+1;
#else
    prep_rsp_cnt = 2 * g_min_invalid_nodes;
    commit_rsp_cnt = prep_rsp_cnt + 1;
#endif
    chkpt_cnt = 1;
    chkpt_flag = false;

#if RING_BFT
    this->is_cross_shard = false;
#endif
#if SHARPER || NEW_SHARPER
    this->is_cross_shard = false;
    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        prep_rsp_cnt_arr[i] = 2 * g_min_invalid_nodes;
        commit_rsp_cnt_arr[i] = 2 * g_min_invalid_nodes;
    }
#endif

    batchreq = NULL;

    txn_stats.init();
}

// reset after abort
void TxnManager::reset()
{
    rsp_cnt = 0;
    aborted = false;
    return_id = UINT64_MAX;
    //twopl_wait_start = 0;

    assert(txn);
#if BANKING_SMART_CONTRACT
    //assert(smart_contract);
#else
    assert(query);
#endif
    txn->reset(get_thd_id());

    // Stats
    txn_stats.reset();
}

void TxnManager::release(uint64_t pool_id)
{

#if RING_BFT
    if (!this->hash.empty() && this->is_cross_shard)
    {
        // cout << ccm_directory.size() << "   " << rcm_directory.size() << endl;
        // digest_directory.remove(hash);
        CommitCertificateMessage *temp1 = ccm_directory.pop(hash);
        if (temp1)
            Message::release_message(temp1);
    }
#endif

    uint64_t tid = get_txn_id();
#if BANKING_SMART_CONTRACT
    delete this->smart_contract;
#else
    qry_pool.put(pool_id, query);
    query = NULL;
#endif
    txn_pool.put(pool_id, txn);
    txn = NULL;

    txn_ready = true;

    hash.clear();
    prepared = false;
#if MINI_NET
    prep_rsp_cnt = g_min_invalid_nodes;
    commit_rsp_cnt = g_min_invalid_nodes+1;
#else
    prep_rsp_cnt = 2 * g_min_invalid_nodes;
    commit_rsp_cnt = prep_rsp_cnt + 1;
#endif
    chkpt_cnt = 1;
    chkpt_flag = false;
    release_all_messages(tid);

#if RING_BFT
    this->is_cross_shard = false;
    this->client_id = -1;
#endif
#if SHARPER || NEW_SHARPER
    this->is_cross_shard = false;
    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        prep_rsp_cnt_arr[i] = 2 * g_min_invalid_nodes;
        commit_rsp_cnt_arr[i] = prep_rsp_cnt_arr[i];
    }
#endif
    txn_stats.init();
}

void TxnManager::reset_query()
{
#if !BANKING_SMART_CONTRACT
    ((YCSBQuery *)query)->reset();
#endif
}

RC TxnManager::commit()
{
    DEBUG("Commit %ld\n", get_txn_id());

    commit_stats();
    return Commit;
}

RC TxnManager::start_commit()
{
    RC rc = RCOK;
    DEBUG("%ld start_commit RO?\n", get_txn_id());
    return rc;
}

int TxnManager::received_response(RC rc)
{
    assert(txn->rc == RCOK);
    if (txn->rc == RCOK)
        txn->rc = rc;

    --rsp_cnt;

    return rsp_cnt;
}

bool TxnManager::waiting_for_response()
{
    return rsp_cnt > 0;
}

void TxnManager::commit_stats()
{
    uint64_t commit_time = get_sys_clock();
    uint64_t timespan_short = commit_time - txn_stats.restart_starttime;
    uint64_t timespan_long = commit_time - txn_stats.starttime;
    INC_STATS(get_thd_id(), total_txn_commit_cnt, 1);

    if (!IS_LOCAL(get_txn_id()))
    {
        txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long, timespan_short);
        return;
    }
    
    INC_STATS(get_thd_id(), txn_run_time, timespan_long);
    INC_STATS(get_thd_id(), single_part_txn_cnt, 1);
    INC_STATS(get_thd_id(), txn_total_time_span, (double)timespan_short);
    txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long, timespan_short);
}

void TxnManager::register_thread(Thread *h_thd)
{
    this->h_thd = h_thd;
}

void TxnManager::set_txn_id(txnid_t txn_id)
{
    txn->txn_id = txn_id;
}

txnid_t TxnManager::get_txn_id()
{
    return txn->txn_id;
}

Workload *TxnManager::get_wl()
{
    return h_wl;
}

uint64_t TxnManager::get_thd_id()
{
    if (h_thd)
        return h_thd->get_thd_id();
    else
        return 0;
}

BaseQuery *TxnManager::get_query()
{
#if !BANKING_SMART_CONTRACT
    return query;
#else
    return NULL;
#endif
}

void TxnManager::set_query(BaseQuery *qry)
{
#if !BANKING_SMART_CONTRACT
    query = qry;
#endif
}

uint64_t TxnManager::incr_rsp(int i)
{
    uint64_t result;
    sem_wait(&rsp_mutex);
    result = ++this->rsp_cnt;
    sem_post(&rsp_mutex);
    return result;
}

uint64_t TxnManager::decr_rsp(int i)
{
    uint64_t result;
    sem_wait(&rsp_mutex);
    result = --this->rsp_cnt;
    sem_post(&rsp_mutex);
    return result;
}

RC TxnManager::validate()
{
    return RCOK;
}

/* Generic Helper functions. */

string TxnManager::get_hash()
{
    return hash;
}

void TxnManager::set_hash(string hsh)
{
    hash = hsh;
    hashSize = hash.length();
}

uint64_t TxnManager::get_hashSize()
{
    return hashSize;
}

void TxnManager::set_primarybatch(BatchRequests *breq)
{
    char *buf = create_msg_buffer(breq);
    Message *deepMsg = deep_copy_msg(buf, breq);
    batchreq = (BatchRequests *)deepMsg;
    delete_msg_buffer(buf);
}

// #if ISEOV
// vector<map<uint64_t,uint64_t>> *TxnManager::get_readSet_vec()
// {
//     return *(batchreq->readSet);
// }

// vector<map<uint64_t,uint64_t>> *TxnManager::get_writeSet_vec()
// {
//     return *(batchreq->writeSet);
// }
// #endif

bool TxnManager::is_chkpt_ready()
{
    return chkpt_flag;
}

void TxnManager::set_chkpt_ready()
{
    chkpt_flag = true;
}

uint64_t TxnManager::decr_chkpt_cnt()
{
    chkpt_cnt--;
    return chkpt_cnt;
}

uint64_t TxnManager::get_chkpt_cnt()
{
    return chkpt_cnt;
}

/* Helper functions for PBFT. */
void TxnManager::set_prepared()
{
    prepared = true;
}

bool TxnManager::is_prepared()
{
    //cout << "test_v4:TxnManager::set_prepared:txn->batch_id = "<<txn->batch_id<<"\n";
    return prepared;
}

uint64_t TxnManager::decr_prep_rsp_cnt()
{
    //cout << "test_v4:TxnManager::decr_prep_rsp_cnt()\n";
    prep_rsp_cnt--;
    return prep_rsp_cnt;
}

uint64_t TxnManager::get_prep_rsp_cnt()
{
    return prep_rsp_cnt;
}

/************************************/

/* Helper functions for PBFT. */

void TxnManager::set_committed()
{
    committed_local = true;
    //cout << "test_v4:TxnManager::set_committed:txn->batch_id = "<<txn->batch_id<<"\n";
}

bool TxnManager::is_committed()
{
    return committed_local;
}

void TxnManager::add_commit_msg(PBFTCommitMessage *pcmsg)
{
    char *buf = create_msg_buffer(pcmsg);
    Message *deepMsg = deep_copy_msg(buf, pcmsg);
    commit_msgs.push_back((PBFTCommitMessage *)deepMsg);
    delete_msg_buffer(buf);
}

uint64_t TxnManager::decr_commit_rsp_cnt()
{
    //cout << "test_v4:TxnManager::decr_commit_rsp_cnt()\n";
    commit_rsp_cnt--;
    return commit_rsp_cnt;
}

uint64_t TxnManager::get_commit_rsp_cnt()
{
    return commit_rsp_cnt;
}

/*****************************/
/* Helper functions for SHARPER. */
#if SHARPER || NEW_SHARPER
uint64_t TxnManager::decr_prep_rsp_cnt(int shard)
{
    if (prep_rsp_cnt_arr[shard])
        prep_rsp_cnt_arr[shard]--;
    return prep_rsp_cnt_arr[shard];
}

uint64_t TxnManager::get_prep_rsp_cnt(int shard)
{
    return prep_rsp_cnt_arr[shard];
}

uint64_t TxnManager::decr_commit_rsp_cnt(int shard)
{
    if (commit_rsp_cnt_arr[shard])
        commit_rsp_cnt_arr[shard]--;
    return commit_rsp_cnt_arr[shard];
}

uint64_t TxnManager::get_commit_rsp_cnt(int shard)
{
    return commit_rsp_cnt_arr[shard];
}
#endif

/*****************************/

//broadcasts prepare message to all nodes
void TxnManager::send_pbft_prep_msgs()
{
    //printf("%ld Send PBFT_PREP_MSG message to %d nodes\n", get_txn_id(), g_node_cnt - 1);
    //fflush(stdout);

    Message *msg = Message::create_message(this, PBFT_PREP_MSG);
    PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
    //cout << "test_v3:PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg,  batch_id = "<< pmsg->batch_id << "\n";

#if LOCAL_FAULT == true || VIEW_CHANGES
    if (get_prep_rsp_cnt() > 0)
    {
        decr_prep_rsp_cnt();
    }
#endif

    vector<uint64_t> dest;
    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
#if SHARPER || NEW_SHARPER
        if (this->is_cross_shard)
        {
            pmsg->is_cross_shard = true;
            if (!this->involved_shards[get_shard_number(i)])
            {
                continue;
            }
        }
        else if (!is_in_same_shard(i, g_node_id))
        {
            continue;
        }
#elif RING_BFT
        if (!is_in_same_shard(i, g_node_id))
        {
            continue;
        }
#endif
        if (i == g_node_id)
        {
            continue;
        }
        if (i % g_net_cnt == g_net_id)
        {
            dest.push_back(i);
        }
        //dest.push_back(i);
    }

#if MULTI_INSTANCES > 1
    if(instance_id % MESSAGE_PER_BUFFER == 0){
        pmsg->force = true;
    }
#endif

    msg_queue.enqueue(get_thd_id(), pmsg, dest);
    dest.clear();
}

//broadcasts commit message to all nodes
void TxnManager::send_pbft_commit_msgs()
{
    //cout << "Send PBFT_COMMIT_MSG messages " << get_txn_id() << "\n";
    //fflush(stdout);

    Message *msg = Message::create_message(this, PBFT_COMMIT_MSG);
    PBFTCommitMessage *cmsg = (PBFTCommitMessage *)msg;

#if LOCAL_FAULT == true || VIEW_CHANGES
    if (get_commit_rsp_cnt() > 0)
    {
        decr_commit_rsp_cnt();
    }
#endif

#if RING_BFT
    if (this->is_cross_shard)
        cmsg->is_cross_shard = true;
    else
        cmsg->is_cross_shard = false;
#endif

    vector<uint64_t> dest;
    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
#if SHARPER || NEW_SHARPER
        if (this->is_cross_shard)
        {
            cmsg->is_cross_shard = true;
            if (!this->involved_shards[get_shard_number(i)])
            {
                continue;
            }
        }
        else if (!is_in_same_shard(i, g_node_id))
        {
            continue;
        }
#elif RING_BFT
        if (!is_in_same_shard(i, g_node_id))
        {
            continue;
        }
#endif
        if (i == g_node_id)
        {
            continue;
        }
        if (i % g_net_cnt == g_net_id)
        {
            dest.push_back(i);
        }
        //dest.push_back(i);
    }

    #if MULTI_INSTANCES > 1
    if(instance_id % MESSAGE_PER_BUFFER == 0){
        cmsg->force = true;
    }
#endif

    msg_queue.enqueue(get_thd_id(), cmsg, dest);
    dest.clear();
}

#if !TESTING_ON

void TxnManager::release_all_messages(uint64_t txn_id)
{
    if ((txn_id + 1) % get_batch_size() == 0)
    {
        info_prepare.clear();
        info_commit.clear();
#if RING_BFT
        if (!this->is_cross_shard)
            Message::release_message(batchreq);

#else
        // Message::release_message(batchreq);
        if(batchreq){
            Message::release_message(batchreq);
        }
#endif

        PBFTCommitMessage *cmsg;
        while (commit_msgs.size() > 0)
        {
            cmsg = (PBFTCommitMessage *)this->commit_msgs[0];
            commit_msgs.erase(commit_msgs.begin());
            Message::release_message(cmsg);
        }
    }
}

#endif // !TESTING

//broadcasts checkpoint message to all nodes
void TxnManager::send_checkpoint_msgs()
{
    DEBUG("%ld Send PBFT_CHKPT_MSG message to %d\n nodes", get_txn_id(), g_node_cnt - 1);

    Message *msg = Message::create_message(this, PBFT_CHKPT_MSG);
    CheckpointMessage *ckmsg = (CheckpointMessage *)msg;

//     vector<uint64_t> dest;
//     for (uint64_t i = 0; i < g_node_cnt; i++)
//     {
// #if RING_BFT || SHARPER
//         if (!is_in_same_shard(i, g_node_id))
//         {
//             continue;
//         }
// #endif
//         if (i == g_node_id)
//         {
//             continue;
//         }
//         dest.push_back(i);
//     }

//     msg_queue.enqueue(get_thd_id(), ckmsg, dest);
//     dest.clear();
    work_queue.enqueue(get_thd_id(), ckmsg, false);
}
