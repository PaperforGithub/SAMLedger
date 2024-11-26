#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include "global.h"
#include "array.h"
#include <mutex>
#include <map>
// #include <metis.h>

class ycsb_request;
class ycsb_request_writeset;
class LogRecord;
struct Item_no;

class Message
{
public:
    virtual ~Message() {
        this->dest.clear();
    }
    static Message *create_message(char *buf);
    static Message *create_message(BaseQuery *query, RemReqType rtype);
    static Message *create_message(TxnManager *txn, RemReqType rtype);
    static Message *create_message(uint64_t txn_id, RemReqType rtype);
    static Message *create_message(uint64_t txn_id, uint64_t batch_id, RemReqType rtype);
    static Message *create_message(LogRecord *record, RemReqType rtype);
    static Message *create_message(RemReqType rtype);
    static std::vector<Message *> *create_messages(char *buf);
    static void release_message(Message *msg);
    RemReqType rtype;
    uint64_t txn_id;
    uint64_t batch_id;
    uint64_t return_node_id;

    uint64_t wq_time;
    uint64_t mq_time;
    uint64_t ntwk_time;

    //signature is 768 chars, pubkey is 840
    uint64_t sigSize = 1;
    uint64_t keySize = 1;
    string signature = "0";
    string pubKey = "0";

    static uint64_t string_to_buf(char *buf, uint64_t ptr, string str);
    static uint64_t buf_to_string(char *buf, uint64_t ptr, string &str, uint64_t strSize);

    vector<uint64_t> dest;

    // Collect other stats
    double lat_work_queue_time;
    double lat_msg_queue_time;
    double lat_cc_block_time;
    double lat_cc_time;
    double lat_process_time;
    double lat_network_time;
    double lat_other_time;

    uint64_t mget_size();
    uint64_t get_txn_id() { return txn_id; }
    uint64_t get_batch_id() { return batch_id; }
    uint64_t get_return_id() { return return_node_id; }
    void mcopy_from_buf(char *buf);
    void mcopy_to_buf(char *buf);
    void mcopy_from_txn(TxnManager *txn);
    void mcopy_to_txn(TxnManager *txn);
    RemReqType get_rtype() { return rtype; }

    virtual uint64_t get_size() = 0;
    virtual void copy_from_buf(char *buf) = 0;
    virtual void copy_to_buf(char *buf) = 0;
    virtual void copy_to_txn(TxnManager *txn) = 0;
    virtual void copy_from_txn(TxnManager *txn) = 0;
    virtual void init() = 0;
    virtual void release() = 0;
#if SHARPER || NEW_SHARPER
    bool is_cross_shard = false;
#endif
#if MULTI_INSTANCES > 1
    #if STRONG_SERIAL
        bool force = true;
    #else
        bool force = false;
    #endif
#else
    bool force = true;
#endif
    

};

// Message types
class InitDoneMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
};

class KeyExchange : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    string pkey;
    uint64_t pkeySz;
    uint64_t return_node;
};

class ReadyServer : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
};

class QueryResponseMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    RC rc;
    uint64_t pid;
};

class DoneMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    uint64_t batch_id;
};

class ClientQueryMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_query(BaseQuery *query);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();

    uint64_t pid;
    uint64_t ts;
    uint64_t client_startts;
    uint64_t first_startts;
    Array<uint64_t> partitions;
};

#if BANKING_SMART_CONTRACT
class BankingSmartContractMessage : public ClientQueryMessage
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_query(BaseQuery *query);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    BankingSmartContractMessage();
    ~BankingSmartContractMessage();

    BSCType type; // Type of Banking Smartcontract
    string getString();
    string getRequestString();

    Array<uint64_t> inputs;
// #if ISEOV
//     map<uint64_t,uint64_t> readSet;
//     map<uint64_t,uint64_t> writeSet;
// #endif
};
#else
class YCSBClientQueryMessage : public ClientQueryMessage
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_query(BaseQuery *query);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    YCSBClientQueryMessage();
    ~YCSBClientQueryMessage();

    uint64_t return_node; // node that send this message.
    string getString();
    string getRequestString();

    Array<ycsb_request *> requests;
#if PRE_ORDER
    Array<ycsb_request_writeset *> requests_writeset;
#endif
};
#endif

class ClientResponseMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    void set_net_id(uint64_t id);
    string getString(uint64_t sender);

    RC rc;

#if CLIENT_RESPONSE_BATCH == true
    Array<uint64_t> index;
    Array<uint64_t> client_ts;
    uint64_t net_id;
#else
    uint64_t client_startts;
#endif
// #if RING_BFT || SHARPER || NEW_SHARPER
//     bool is_cross_shard = false;
// #endif
    uint64_t view; // primary node id
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
};

#if CLIENT_BATCH
class ClientQueryBatch : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
#if PRE_ORDER
    void generate_input();
    void generate_output();
#endif

    uint64_t get_size();
    void init();
    void release();

    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    string getString();
#if SHARPER || NEW_SHARPER
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
#elif RING_BFT
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
    bool is_cross_shard = false;
#endif
    uint64_t return_node;
    uint64_t batch_size;
#if BANKING_SMART_CONTRACT
    Array<BankingSmartContractMessage *> cqrySet;
#else
    Array<YCSBClientQueryMessage *> cqrySet;
    #if PRE_ORDER
        uint64_t inputState_size;
        uint64_t outputState_size;
        map<uint64_t,uint64_t> inputState;
        map<uint64_t,uint64_t> outputState;
    #endif
#endif
#if ZS_CLIENT
    uint64_t leader_id = 0;
    uint64_t do_or_pending = 0;
    uint64_t special_id = 0;
#endif
};
#endif

class QueryMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    uint64_t pid;
};

class YCSBQueryMessage : public QueryMessage
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();

    Array<ycsb_request *> requests;
};

/***********************************/

class BatchRequests : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
#if BANKING_SMART_CONTRACT
    void copy_from_txn(TxnManager *txn, BankingSmartContractMessage *clqry);
#else
    void copy_from_txn(TxnManager *txn, YCSBClientQueryMessage *clqry);
#endif
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id);
    void release();
#if PRE_ORDER
    void add_state(ClientQueryBatch *bmsg);
    bool validate_pre_exec();
#endif
// #if ISEOV
//     void simulate_batch();
// #endif
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string getString(uint64_t sender);

    void add_request_msg(int idx, Message *msg);

    uint64_t view; // primary node id
#if SHARPER || NEW_SHARPER
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
#elif RING_BFT
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
    bool is_cross_shard = false;
#endif

    Array<uint64_t> index;
#if BANKING_SMART_CONTRACT
    vector<BankingSmartContractMessage *> requestMsg;
#else
    vector<YCSBClientQueryMessage *> requestMsg;
#endif

#if ISEOV
    vector<map<uint64_t,uint64_t>> readSet;
    vector<map<uint64_t,uint64_t>> writeSet;
#endif
#if PRE_ORDER
    uint64_t inputState_size;
    uint64_t outputState_size;
    map<uint64_t,uint64_t> inputState;
    map<uint64_t,uint64_t> outputState;
#endif
// #if ISEOV
//     uint64_t readSet_size;
//     uint64_t writeSet_size;
//     map<uint64_t,uint64_t> readSet;
//     map<uint64_t,uint64_t> writeSet;
// #endif
    uint64_t hashSize; // Representative hash for the batch.
    string hash;
    uint32_t batch_size;
#if ZS
    //flag_shard:0.非数据划分消息；1.leader发起的dag统计请求；2.follower 的回复；3.leader的回复
    uint64_t flag_shard;
    uint64_t shardDag_id;
    uint64_t avg_workload;
    uint64_t size_ShardDAG,size_StateShard,size_StateValue;
    std::vector<std::array<uint64_t, 3>> ShardDAG;
    std::vector<uint64_t> size_EachStateShard;
    std::map<uint64_t, std::vector<uint64_t>> StateShard;
    // std::map<uint64_t, uint64_t> StateShard;
    std::map<uint64_t, uint64_t> StateValue;

#endif
};

class ExecuteMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    uint64_t view;        // primary node id
    uint64_t index;       // position in sequence of requests
    string hash;          //request message digest
    uint64_t hashSize;    //size of hash (for removing from buf)
    uint64_t return_node; //id of node that sent this message

    uint64_t end_index;
    uint64_t batch_size;
    uint64_t net_id;
};

class CheckpointMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    string toString();
    bool addAndValidate();

    uint64_t index;       // sequence number of last request
    uint64_t return_node; //id of node that sent this message
    uint64_t end_index;
};

// Message Creation methods.
char *create_msg_buffer(Message *msg);
Message *deep_copy_msg(char *buf, Message *msg);
void delete_msg_buffer(char *buf);

class PBFTPrepMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    string toString();

    uint64_t view;        // primary node id
    uint64_t index;       // position in sequence of requests
    string hash;          //request message digest
    uint64_t hashSize;    //size of hash (for removing from buf)
    uint64_t return_node; //id of node that sent this message

    uint64_t end_index;
    uint32_t batch_size;
};

class PBFTCommitMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    string toString();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();

    uint64_t view;        // primary node id
    uint64_t index;       // position in sequence of requests
    string hash;          //request message digest
    uint64_t hashSize;    //size of hash (for removing from buf)
    uint64_t return_node; //id of node that sent this message

    uint64_t end_index;
    uint64_t batch_size;
#if RING_BFT
    bool is_cross_shard = false;
#endif
};

class BroadcastBatchMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    //string getString(uint64_t sender);
    string getString();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    void add_batch(Message *msg);
    void add_commit_msg(PBFTCommitMessage *cmsg);

    uint64_t net_id;
    uint64_t breq_return_id;
    uint64_t cmsgSet_size;
    BatchRequests *breq;
    vector<PBFTCommitMessage *> cmsgSet;

    // uint64_t view;        // primary node id
    // uint64_t index;       // position in sequence of requests
    // string hash;          //request message digest
    // uint64_t hashSize;    //size of hash (for removing from buf)
    // uint64_t return_node; //id of node that sent this message

    // uint64_t end_index;
    // uint64_t batch_size;
};

#if SHARPER || NEW_SHARPER

class SuperPropose : public BatchRequests
{
public:
};
#endif

#if RING_BFT
class CommitCertificateMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release();

    void sign(uint64_t dest_node_id);
    bool validate();
    string toString();

    uint64_t commit_view;  // primary node id
    uint64_t commit_index; // position in sequence of requests
    uint64_t commit_hash_size;
    string commit_hash; //request message digests
    uint64_t forwarding_from = (uint64_t)-1;

    uint64_t hashSize;
    Array<uint64_t> signSize;
    Array<uint64_t> signOwner;
    string hash;
    vector<YCSBClientQueryMessage *> requestMsg;
    vector<string> signatures;
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
};

class RingBFTPrePrepare : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn){};
    void copy_to_txn(TxnManager *txn){};
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id);
    void release();

    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string getString(uint64_t sender);

    uint64_t view; // primary node id
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
    uint64_t hashSize; // Representative hash for the batch.
    string hash;
    uint32_t batch_size;
};

class RingBFTCommit : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release();

    void sign(uint64_t dest_node_id);
    bool validate();
    string toString();

    uint64_t commit_view;  // primary node id
    uint64_t commit_index; // position in sequence of requests
    uint64_t commit_hash_size;
    string commit_hash; //request message digests
    uint64_t forwarding_from = (uint64_t)-1;

    uint64_t hashSize;
    Array<uint64_t> signSize;
    Array<uint64_t> signOwner;
    string hash;
    vector<string> signatures;
};

#endif

/****************************************/
/*	VIEW CHANGE SPECIFIC		*/
/****************************************/

#if VIEW_CHANGES

class ViewChangeMsg : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn) {}
    void copy_to_txn(TxnManager *txn) {}
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id, TxnManager *txn);
    void release();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string toString();
    bool addAndValidate(uint64_t thd_id);

    uint64_t return_node; //id of node that sent this message
    uint64_t view;        // proposed view (v + 1)
    uint64_t index;       //index of last stable checkpoint
    uint64_t numPreMsgs;
    uint64_t numPrepMsgs;

    vector<BatchRequests *> preMsg;

    // Prepare messages <view, index, hash>.
    vector<uint64_t> prepView;
    vector<uint64_t> prepIdx;
    vector<string> prepHash;
    vector<uint64_t> prepHsize;
};

class NewViewMsg : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn) {}
    void copy_to_txn(TxnManager *txn) {}
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id);
    void release();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string toString();

    uint64_t view; // proposed view (v + 1)
    uint64_t numViewChangeMsgs;
    uint64_t numPreMsgs;

    vector<ViewChangeMsg *> viewMsg;
    vector<BatchRequests *> preMsg;
};
/*******************************/

// Entities for handling BatchRequests message during a view change.
extern vector<BatchRequests *> breqStore;
extern std::mutex bstoreMTX;
void storeBatch(BatchRequests *breq);
void removeBatch(uint64_t range);

// Entities for handling ViewChange message.
extern vector<ViewChangeMsg *> view_change_msgs;
void storeVCMsg(ViewChangeMsg *vmsg);
void clearAllVCMsg();

#endif // VIEW_CHANGE

#endif

#if ZS

#ifndef ZSDag
#define ZSDag

class Edge {
public:
    uint64_t u;
    uint64_t v;
    uint64_t w;

    uint64_t get_u() const { return u; }
    uint64_t get_v() const { return v; }
    uint64_t get_w() const { return w; }
};

class ShardDAG{
public:
    std::map<uint64_t, std::vector<uint64_t>> VSet;
    std::map<std::pair<uint64_t, uint64_t>, uint64_t> ESet;
    std::map<uint64_t, uint64_t> StateShard;
    std::map<uint64_t, uint64_t> StateValue;
    uint64_t total_workload,avg_workload;


    // 静态工厂函数
    static ShardDAG* create() {
        return new ShardDAG(); 
    }

    ShardDAG() {
        // 可以在构造函数中执行一些初始化操作
    }

    static bool cmpl(const Edge& x, const Edge& y) { return x.w > y.w; }

    const std::map<uint64_t, std::vector<uint64_t>>& get_VSet() const { return VSet; }
    const std::map<std::pair<uint64_t, uint64_t>, uint64_t>& get_ESet() const { return ESet; }
    const std::map<uint64_t, uint64_t>& get_StateShard() const { return StateShard; }
    const std::map<uint64_t, uint64_t>& get_StateValue() const { return StateValue; }

    // 添加边的方法
    void addEdge(uint64_t u, uint64_t v, uint64_t w) {
        if (u > v) {
            std::swap(u, v);
        }
        std::pair<uint64_t, uint64_t> e = {u, v};
        // 更新 ESet 中的边权重
        if (ESet.find(e) != ESet.end()) {
            ESet[e] += w;
        } else {
            ESet[e] = w;
            // 更新 VSet
            if (VSet.find(u) != VSet.end()) {
                VSet[u].push_back(v);
            } else {
                VSet[u] = {v}; 
            }
        }
    }
    void updateStateShard(uint64_t &resavg_workload, std::map<uint64_t, uint64_t> &g_currentStateShard){
        std::vector<Edge> edges;
        std::cout << "ZS updateStateShard edge in SDAG.update:" << std::endl;
        for (const auto& entry : ESet) {
            const auto& key = entry.first;  // std::pair<uint64_t, uint64_t>
            uint64_t value = entry.second;  // uint64_t
            edges.push_back({key.first, key.second, value});
            // std::cout << "updateStateShard edge: (" << key.first << ", " << key.second << "), w: " << value << std::endl;
        }
        std::sort(edges.begin(), edges.end(), [](const Edge &a, const Edge &b) {
            return a.w > b.w;
        });
        StateShard.clear();
        std::map<uint64_t, uint64_t> ShardCnt;
        uint64_t avg_state_num=(ACCOUNT_NUM/g_shard_cnt)+200;
        uint64_t new_shard=0;
        total_workload=0;
        for(uint64_t i=0;i<g_shard_cnt;i++){
            ShardCnt[i]=0;
        }
        for (const auto &edge : edges){
            // std::cout << "u: " << edge.u <<"in S["<<g_currentStateShard[edge.u]
            // << "], v: " << edge.v <<"in S["<<g_currentStateShard[edge.v]
            // << "], w: " << edge.w<< std::endl;
            total_workload+=edge.w;
            if(StateShard.find(edge.u)==StateShard.end()){
                if(StateShard.find(edge.v)==StateShard.end()){
                    if(g_currentStateShard[edge.u]==g_currentStateShard[edge.v]){
                        StateShard[edge.u]=g_currentStateShard[edge.u];
                        StateShard[edge.v]=g_currentStateShard[edge.v];
                    }else{
                        new_shard=0;
                        while(ShardCnt[new_shard]>avg_state_num&&new_shard<g_shard_cnt){
                            new_shard++;
                        }
                        if(new_shard==g_shard_cnt){
                            new_shard=g_shard_cnt-1;
                        }
                        StateShard[edge.u]=new_shard;
                        StateShard[edge.v]=new_shard;
                        ShardCnt[new_shard]+=2;
                    }
                }else{
                    StateShard[edge.u]=StateShard[edge.v];
                    ShardCnt[new_shard]+=1;
                }
            }else{
                if(StateShard.find(edge.v)==StateShard.end()){
                    StateShard[edge.v]=StateShard[edge.u];
                    ShardCnt[new_shard]+=1;
                }else{
                    // u and v all have newshard
                }
            }
        }
        resavg_workload=avg_workload=total_workload/g_shard_cnt;
        edges.clear();          // 清空所有元素
        edges.shrink_to_fit();  // 释放多余的内存
        for (auto it = VSet.begin(); it != VSet.end(); ++it) {
            it->second.clear();
            it->second.shrink_to_fit();
        }
        VSet.clear();
        ESet.clear();
        std::cout << "ZS updateStateShard edge end in SDAG.update:" << std::endl;
    }
    // void updateStateShard(std::map<uint64_t, uint64_t> &currentStateShard, std::map<uint64_t, uint64_t> &resStateShard,uint64_t &resavg_workload){
    //     // std::cout << "ZS: updateShardDAG ShardDAG "  << std::endl;
    //     // idx_t nvtxs = VSet.size();   // 顶点数
    //     // idx_t ncon = 1;     // 每个顶点的连接数
    //     // std::vector<idx_t> xadj;
    //     // std::vector<idx_t> adjncy;
    //     // std::vector<idx_t> vwgt;
    //     // std::vector<idx_t> adjwgt;
    //     // real_t ubvec = 10000000;  // 分区平衡限制
    //     // idx_t nparts = g_shard_size;  // 分区数
    //     // std::vector<real_t> tpwgts(nparts,0.2/(nparts-1));  // 分配的权重比例
    //     // tpwgts[0]=0.8;
    //     // idx_t options[METIS_NOPTIONS];  // METIS 配置
    //     // // METIS_SetDefaultOptions(options);  // 使用默认设置
    //     // options[METIS_OPTION_OBJTYPE]=METIS_OBJTYPE_CUT;
    //     // options[METIS_OPTION_CTYPE]=METIS_CTYPE_SHEM;
    //     // options[METIS_OPTION_UFACTOR]=10000000;
    //     // options[METIS_OPTION_NUMBERING]=1;
    //     // options[METIS_OPTION_CONTIG]=0;
    //     // options[METIS_OPTION_IPTYPE]=METIS_IPTYPE_EDGE;
    //     // idx_t objval;
    //     // std::vector<idx_t> part(nvtxs);  // 用于存储分区结果

    //     // total_workload=0;


    //     // for (const auto& entry : VSet) {
    //     //     uint64_t key = entry.first;                
    //     //     const std::vector<uint64_t>& value = entry.second; 
    //     //     xadj.push_back(adjncy.size());


    //     //     // 打印键和值
    //     //     // std::cout << "ZS:Key: " << key << " -> Values: ";
    //     //     for (const uint64_t& v : value) {
    //     //         adjncy.push_back(v);
    //     //         std::pair<uint64_t, uint64_t> e = {key, v};
    //     //         adjwgt.push_back(ESet[e]);

    //     //         std::cout << "u:"<<key<<" v:"<<v << " w:"<<ESet[e]<<std::endl; 
    //     //         total_workload+=ESet[e];
    //     //     }
    //     //     xadj.push_back(adjncy.size());
    //     //     std::cout << std::endl;
    //     // }
    //     // total_workload=total_workload/2;
    //     // avg_workload=total_workload/g_shard_cnt;
    //     // resavg_workload=avg_workload;
        
    //     // std::cout << "total_worklaod:"<<total_workload<<" avg_workload:"<<avg_workload<<std::endl;






        
    //     // // 执行分区
    //     // int ret = METIS_PartGraphKway(&nvtxs, &ncon, xadj.data(), adjncy.data(),
    //     //                           NULL, NULL,adjwgt.data(),
    //     //                           &nparts, NULL, &ubvec,
    //     //                           options, &objval, part.data());

    //     // // 错误检查
    //     // std::cout<<"  res:"<<ret<<"edgecut:"<<objval<<std::endl;
    //     // if (ret != METIS_OK) {
    //     //     std::cerr << "Error in METIS_PartGraphKway, return code: " << ret << std::endl;
    //     // } else {
    //     //     std::cout << "Graph partitioned successfully!" << std::endl;
    //     //     for (idx_t i = 0; i < nvtxs; ++i) {
    //     //         currentStateShard[i]=part[i];
    //     //         resStateShard[i]=part[i];
    //     //         StateShard[i]=part[i];
    //     //         std::cout << "Vertex " << i << " is in partition " << part[i] << std::endl;
    //     //     }
    //     // }
    //     std::cout << "ZS: updateShardDAG end ShardDAG  "  << std::endl;
    // }
};

class DAG {
public:
	uint64_t cnt_batch=0;
    uint64_t flag_BR=0;
    uint64_t remote_shard_id=0;
    BatchRequests *model_BR;
    vector<ClientQueryBatch* > currentPendingBatch;
    std::map<uint64_t, std::vector<ClientQueryBatch *>> PendingBatch;

    ShardDAG * currentShardDAG = ShardDAG::create();
    std::map<uint64_t, uint64_t> cntPendingShardDAG;//leader用，收到的i sharddag的回复数目
    std::map<uint64_t, ShardDAG *> PendingShardDAG;

    // std::map<uint64_t, uint64_t>* currentStateShard= new std::map<uint64_t, uint64_t>();
    std::map<uint64_t, uint64_t> currentStateShard;
    std::map<uint64_t, uint64_t> currentStateValue;
    uint64_t currentStateShard_id = 0;

        // 用于保护 PendingBatch 和 PendingShardDAG 的 mutex
    std::mutex pendingBatchMutex;
    std::mutex pendingShardDAGMutex;
    std::mutex model_BRMutex;
    std::mutex currentStateShardMutex;

//     void func1(){
//          std::cout <<endl<< "ZS**********************func1-begin"<<endl;
//         // 示例图的定义
//     idx_t nvtxs = 7;   // 顶点数
//     idx_t ncon = 1;     // 每个顶点的连接数
//     std::vector<idx_t> xadj = {0, 3, 6, 10, 14, 17, 20,22};  // 邻接表
//     // std::vector<idx_t> adjncy = {4,2,1,0,2,3,4,3,1,0,1,2,5,6,0,2,5,4,3,6,5,3};  // 边
//     std::vector<idx_t> adjncy = {4,2,1,
//                                  0,2,3,
//                                  4,3,1,0,
//                                  1,2,5,6,
//                                  0,2,5,
//                                  4,3,6,
//                                  5,3};  // 边
//     std::vector<idx_t> vwgt={502,503,7,505,6,10,506};  // 顶点权重
//     // std::vector<idx_t> adjwgt={1,2,1,1,2,1,3,2,2,2,1,2,5,5,1,3,2,2,2,6,6,5};  // 边权重
//     std::vector<idx_t> adjwgt={1,1,500,
//                                500,2,1,
//                                1,2,2,2,
//                                1,2,2,500,
//                                1,3,2,
//                                2,2,6,
//                                6,500};  // 边权重
    

//     real_t ubvec = 10000000;  // 分区平衡限制
//     idx_t nparts = 4;  // 分区数
//     std::vector<real_t> tpwgts(nparts,0.2/(nparts-1));  // 分配的权重比例
//     tpwgts[0]=0.8;

//     idx_t options[METIS_NOPTIONS];  // METIS 配置
//     // METIS_SetDefaultOptions(options);  // 使用默认设置
//      options[METIS_OPTION_OBJTYPE]=METIS_OBJTYPE_CUT;
//      options[METIS_OPTION_CTYPE]=METIS_CTYPE_SHEM;
//      options[METIS_OPTION_UFACTOR]=10000000;
//      options[METIS_OPTION_NUMBERING]=0;
//      options[METIS_OPTION_CONTIG]=0;
//       options[METIS_OPTION_IPTYPE]=METIS_IPTYPE_EDGE;
//     //   options[2] = 0; 
//     //  options[METIS_OPTION_]=;

//     // for (auto x:options){
//     //     std::cout<<x<<std::endl;
//     // }

//     idx_t objval;
//     std::vector<idx_t> part(nvtxs);  // 用于存储分区结果
// std::cout << std::endl;
//     std::cout <<"2 xadj"<< std::endl;
//     for(uint64_t i=0;i<xadj.size();i++){
//         std::cout <<" "<<xadj[i];
//     }
//     std::cout << std::endl;
//     std::cout <<"2 adjncy"<< std::endl;
//     for(uint64_t i=0;i<adjncy.size();i++){
//         std::cout <<" "<<adjncy[i];
//     }
//     std::cout << std::endl;
//     std::cout <<"2 adjwgt"<< std::endl;
//     for(uint64_t i=0;i<adjwgt.size();i++){
//         std::cout <<" "<<adjwgt[i];
//     }
//     std::cout << std::endl;

//     // 调试：查看初始化的 part 数组
//     std::cout << "Partition array before METIS call: ";
//     for (idx_t i = 0; i < nvtxs; ++i) {
//         std::cout << part[i] << " ";
//     }
//     std::cout << std::endl;


//     // 执行分区
//     int ret = METIS_PartGraphKway(&nvtxs, &ncon, xadj.data(), adjncy.data(),
//                                   NULL, NULL,adjwgt.data(),
//                                   &nparts, NULL, &ubvec,
//                                   options, &objval, part.data());

//     // 错误检查
//     std::cout<<"  res:"<<ret<<"objval"<<objval<<std::endl;
//     if (ret != METIS_OK) {
//         std::cerr << "Error in METIS_PartGraphKway, return code: " << ret << std::endl;
//     } else {
//         std::cout << "Graph partitioned successfully!" << std::endl;
//         for (idx_t i = 0; i < nvtxs; ++i) {
//             std::cout << "Vertex " << i << " is in partition " << part[i] << std::endl;
//         }
//     }
//              std::cout <<endl<< "ZS**********************func1-end"<<endl;

//     }
    void getCurrentShardDAG(uint64_t shardDag_id, std::vector<std::array<uint64_t, 3>> &resShardDAG){
        std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex);
        std::cout << "ZS: getcurrentShardDAG shardDag_id: " << shardDag_id << std::endl;
        for (const auto& entry : currentShardDAG->ESet) {
            const auto& key = entry.first;  // std::pair<uint64_t, uint64_t>
            uint64_t value = entry.second;  // uint64_t
            std::array<uint64_t, 3> newArray = {key.first, key.second, value};
            resShardDAG.push_back(newArray);
            // std::cout << "getShardDAG Key: (" << key.first << ", " << key.second << "), Value: " << value << std::endl;
        }
        // 释放当前指针指向的内存
        if (currentShardDAG != nullptr) {
            delete currentShardDAG;
            currentShardDAG = nullptr;
        }
        currentShardDAG = ShardDAG::create();
    }

    // void getShardDAG(uint64_t shardDag_id, std::vector<std::array<uint64_t, 3>> &resShardDAG){
    //     std::cout << "ZS: getShardDAG shardDag_id: " << shardDag_id << std::endl;
    //     auto it = PendingShardDAG.find(shardDag_id);
    //     if (it != PendingShardDAG.end()) {

    //         for (const auto& entry : it->second->ESet) {
    //         const auto& key = entry.first;  // std::pair<uint64_t, uint64_t>
    //         uint64_t value = entry.second;  // uint64_t
    //         std::array<uint64_t, 3> newArray = {key.first, key.second, value};
    //         resShardDAG.push_back(newArray);
    //         std::cout << "getShardDAG Key: (" << key.first << ", " << key.second << "), Value: " << value << std::endl;
    //         }
    //     }
    // }

    void addDoClbEdge(ClientQueryBatch * clbtch){
            std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex);
            uint64_t u,v,w;
            w=1;
            for (uint64_t i = 0; i < get_batch_size(); i++)
            {
                    #if BANKING_SMART_CONTRACT
                    BankingSmartContractMessage *bsc= clbtch->cqrySet[i];
                    switch (bsc->type)
                    {
                        case BSC_TRANSFER:
                        {
                            u=bsc->inputs[0];
                            v=bsc->inputs[2];
                            break;
                        }
                        case BSC_DEPOSIT:
                        {
                            u=v=bsc->inputs[0];
                            break;
                        }
                        case BSC_WITHDRAW:
                        {
                            u=v=bsc->inputs[0];
                            break;
                        }
                        default:
                            assert(0);
                            break;
                    }
                    #endif
                    if (u > v) {
                        std::swap(u, v);
                    }
                    currentShardDAG->addEdge(u,v,w);
                    // if(i==0){
                    //     cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" txid="<<clbtch->txn_id
                    //         <<" DoP="<<clbtch->do_or_pending
                    //         <<" iscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
                    //     for(uint32_t i=0;i<g_shard_cnt;i++){
                    //         cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
                    //     }
                    //     cout <<" addedge("<<u<<","<<v<<","<<w*get_batch_size()<<")"<<endl;
                    // }
            }
    }
    void updateClientQueryBatch(ClientQueryBatch * clbtch ){
        // if(cnt_batch<3){
        //     cout <<endl<<"ZS METIS"<<endl;
        //     func1();
        //     cout <<endl<<"ZS METIS end"<<endl;
        // }


        // cout <<endl<<"rtest 1"<<endl;
            std::lock_guard<std::mutex> cSSLock(currentStateShardMutex);
            // cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" before update txid="<<clbtch->txn_id
            //                 <<" DoP="<<clbtch->do_or_pending
            //                 <<" iscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
            //             for(uint32_t i=0;i<g_shard_cnt;i++){
            //                 cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
            //             }
            for (uint32_t i = 0; i < g_shard_cnt; ++i) {
                clbtch->involved_shards[i]=false;
            }
            for (uint64_t i = 0; i < get_batch_size(); i++)
            {
                    #if BANKING_SMART_CONTRACT
                    BankingSmartContractMessage *bsc= clbtch->cqrySet[i];
                    switch (bsc->type)
                    {
                        case BSC_TRANSFER:
                        {
                            clbtch->involved_shards[currentStateShard[bsc->inputs[0]]]=true;
                            clbtch->involved_shards[currentStateShard[bsc->inputs[2]]]=true;
                            // TransferMoneySmartContract *tm = new TransferMoneySmartContract();
                            // tm->source_id = bsc->inputs[0];
                            // tm->amount = bsc->inputs[1];
                            // tm->dest_id = bsc->inputs[2];
                            // tm->type = BSC_TRANSFER;
                            break;
                        }
                        case BSC_DEPOSIT:
                        {
                            clbtch->involved_shards[currentStateShard[bsc->inputs[0]]]=true;
                            // DepositMoneySmartContract *dm = new DepositMoneySmartContract();
                            // dm->dest_id = bsc->inputs[0];
                            // dm->amount = bsc->inputs[1];
                            // dm->type = BSC_DEPOSIT;
                            break;
                        }
                        case BSC_WITHDRAW:
                        {
                            clbtch->involved_shards[currentStateShard[bsc->inputs[0]]]=true;
                            // WithdrawMoneySmartContract *wm = new WithdrawMoneySmartContract();
                            // wm->source_id = bsc->inputs[0];
                            // wm->amount = bsc->inputs[1];
                            // wm->type = BSC_WITHDRAW;
                            break;
                        }
                        default:
                            assert(0);
                            break;
                    }
                    #endif
            }
            uint64_t cnt_cross=0;
            uint64_t flag_leader_id=0;
            for (uint32_t i = 0; i < g_shard_cnt; ++i) {
                if(clbtch->involved_shards[i]==true){
                    if(flag_leader_id==0){
                        clbtch->leader_id=i*g_shard_size;
                        flag_leader_id=1;
                    }
                     cnt_cross++;
                }
            }
            if(clbtch->involved_shards[g_node_id/g_shard_size]==true){
                clbtch->leader_id=g_node_id;
            }
            if(cnt_cross>1){
                clbtch->is_cross_shard=true;
            }else{
                clbtch->is_cross_shard=false;
            }
            // cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" after update txid="<<clbtch->txn_id
            //                 <<" DoP="<<clbtch->do_or_pending
            //                 <<" iscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
            //             for(uint32_t i=0;i<g_shard_cnt;i++){
            //                 cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
            //             }
    }
    void init(){
        uint64_t temp_account_num= g_account_num+200;
        // uint64_t single_shard_account_num=temp_account_num/g_shard_cnt;
        currentStateShard_id=0;
        cnt_batch=0;
        flag_BR=0;
        currentShardDAG = ShardDAG::create();
        currentStateShard.clear();
    #if ZS && USE_ETH_ACCOUNT
                for (uint64_t i=0;i<temp_account_num;i++){
            currentStateShard[i]=i%g_shard_cnt;
        }
    #endif
    #if ZS && !USE_ETH_ACCOUNT
            for (uint64_t i=0;i<temp_account_num;i++){
            currentStateShard[i]=i/single_shard_account_num;
        }
    #endif


    }
    void setmodelBR(BatchRequests *BR){
        std::lock_guard<std::mutex> BRLock(model_BRMutex);
        char *buf = create_msg_buffer(BR);
        Message *deepMsg = deep_copy_msg(buf, BR);
        model_BR=(BatchRequests *)deepMsg;
        cout <<endl<<"ZS set modelBR batchid="<<model_BR->batch_id
            <<" txid="<<model_BR->txn_id<<endl;
        flag_BR=1;
    }
    BatchRequests * getmodelBR(){
        std::lock_guard<std::mutex> BRLock(model_BRMutex);
        cout <<endl<<"ZS get modelBR batchid="<<model_BR->batch_id
            <<" txid="<<model_BR->txn_id<<endl;
        return model_BR;
    }

    void addpendingBatch(ClientQueryBatch* batch){
        ClientQueryBatch *clbtch = (ClientQueryBatch *)batch;
        std::lock_guard<std::mutex> batchLock(pendingBatchMutex);
            currentPendingBatch.push_back(clbtch);
            cnt_batch++;
    }
    void addpendingEdge(uint64_t u,uint64_t v,uint64_t w){
        std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex);
        currentShardDAG->addEdge(u,v,w);
    }


    void storePendingBatch(uint64_t shardDag_id){

    // 使用 std::lock 防止死锁
    std::lock(pendingBatchMutex, pendingShardDAGMutex);
    std::lock_guard<std::mutex> batchLock(pendingBatchMutex, std::adopt_lock);
    std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex, std::adopt_lock);

    PendingShardDAG[shardDag_id]=currentShardDAG;
    cntPendingShardDAG[shardDag_id]=1;
    currentShardDAG = ShardDAG::create();

    // auto it = PendingShardDAG.find(shardDag_id);
    // if (it != PendingShardDAG.end()) {

    //     cout <<"ZS storePendingBatch shardDAG:"<<shardDag_id<<endl;
    //     for (const auto& entry : it->second->ESet) {
    //         const auto& key = entry.first;  // std::pair<uint64_t, uint64_t>
    //         uint64_t value = entry.second;  // uint64_t
    //         std::cout << " storePendingBatch Key: (" << key.first << ", " << key.second << "), Value: " << value << std::endl;
    //     }
    // }

#if ZSShardBase
    PendingBatch[shardDag_id]=currentPendingBatch;
    std::vector<ClientQueryBatch *> newCurrentPendingBatch;
    currentPendingBatch = newCurrentPendingBatch;
#endif
    }


    void addShardDAG(uint64_t shardDag_id, std::vector<std::array<uint64_t, 3>> &resShardDAG){
        // std::cout << "ZS: addShardDAG shardDag_id: " << shardDag_id << std::endl;
        std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex);
        auto it = PendingShardDAG.find(shardDag_id);
        if (it != PendingShardDAG.end()) {
            cntPendingShardDAG[shardDag_id]++;
            for (const auto& arr : resShardDAG) {
                it->second->addEdge(arr[0],arr[1],arr[2]);
                // std::cout << "("<<arr[0]<<","<<arr[1]<<","<<arr[2]<<")"<< std::endl;
            }

        }
        resShardDAG.clear();
        // std::cout << "ZS: addShardDAG end shardDag_id: " << shardDag_id << std::endl;
    }

    void updateStateShard(uint64_t shardDag_id,std::vector<uint64_t> &resStateShardSize,std::map<uint64_t, std::vector<uint64_t>> &resStateShard,uint64_t &resavg_workload){
        


        std::cout << "ZS: updateShardDAG before mutexshardDag_id: " << shardDag_id << std::endl;

        std::lock(pendingShardDAGMutex, currentStateShardMutex);
        std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex, std::adopt_lock);
        std::lock_guard<std::mutex> cSSLock(currentStateShardMutex, std::adopt_lock);
        if(g_node_id==0){
            auto it = PendingShardDAG.find(shardDag_id);
            if (it != PendingShardDAG.end()) {
                currentStateShard_id=shardDag_id;
                resStateShard.clear();
                resStateShardSize.clear();
                resavg_workload=0;
                it->second->updateStateShard(resavg_workload,currentStateShard);
                cout <<"ZS  updateStateShard after update shardDag_id:"<<shardDag_id<<endl;

                for (auto itt = it->second->StateShard.begin(); itt != it->second->StateShard.end(); ++itt) {
                    if(currentStateShard[itt->first]!=itt->second){
                        // std::cout << "State: " << itt->first << ", OldS: " << currentStateShard[itt->first] <<" NewS: "<<itt->second<< std::endl;
                        currentStateShard[itt->first]=itt->second;
                        if (resStateShard.find(itt->second) != resStateShard.end()) {
                            resStateShard[itt->second].push_back(itt->first);
                        } else {
                            resStateShard[itt->second] = {itt->first}; 
                        }
                    }
                }
                // cout <<"ZS  updateStateShard 2 after update shardDag_id:"<<shardDag_id<<endl;
                for (uint64_t i=0;i<g_shard_cnt;i++){
                    if (resStateShard.find(i) != resStateShard.end()){
                        resStateShardSize.push_back(resStateShard[i].size()); 
                         cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<<"ZS: updateShardDAG resStateShard:"<<i<<" resStateShardSize:"<<resStateShardSize[i]<<" resStateShard.size()"<<resStateShard[i].size()<<endl;
                    }else{
                        resStateShardSize.push_back(0);
                         cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<<"ZS: updateShardDAG resStateShard:"<<i<<" resStateShardSize:"<<resStateShardSize[i]<<" resStateShard.size() 0" <<endl;
                    }
                }
                 cout <<"ZS  updateStateShard 3 after update shardDag_id:"<<shardDag_id<<endl;

            }else{
                // cout <<"ZS  updateStateShard after update shardDag_id:"<<shardDag_id<<" it == PendingShardDAG.end()"<<endl;
            }
            


            // currentStateShard_id=shardDag_id;
            // std::cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id << std::endl;
            // // currentStateShard.clear();
            // uint64_t temp_account_num= g_account_num+200;
            // // uint64_t single_shard_account_num=temp_account_num/g_shard_cnt;
            // for (uint64_t i=0;i<temp_account_num;i++){
            //     uint64_t newshard=i%g_shard_cnt;
            //     if(currentStateShard[i]!=newshard){
            //         currentStateShard[i]=newshard;

            //         if (resStateShard.find(newshard) != resStateShard.end()) {
            //             resStateShard[newshard].push_back(i);
            //         } else {
            //             resStateShard[newshard] = {i}; 
            //         }
            //     }
            // }
            // for (uint64_t i=0;i<g_shard_cnt;i++){
            //     if (resStateShard.find(i) != resStateShard.end()){
            //         resStateShardSize.push_back(resStateShard[i].size()); 
            //          cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<<"ZS: updateShardDAG resStateShard:"<<i<<" resStateShardSize:"<<resStateShardSize[i]<<" resStateShard.size()"<<resStateShard[i].size()<<endl;
            //     }else{
            //         resStateShardSize.push_back(0);
            //          cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<<"ZS: updateShardDAG resStateShard:"<<i<<" resStateShardSize:"<<resStateShardSize[i]<<" resStateShard.size() 0" <<endl;
            //     }
               
            // }
            // auto it = PendingShardDAG.find(shardDag_id);
            
            // if (it != PendingShardDAG.end()) {
            //     it->second->updateStateShard(currentStateShard,resStateShard,resavg_workload);
            // }
        }else{
             std::cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<< "resStateShard Size "<<resStateShard.size()<<endl;
            for(uint64_t i=0;i<g_shard_cnt;i++){
                std::cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<< "shard= "<<i<<"size="<<resStateShardSize[i];
                if(resStateShardSize[i]!=0){
                    cout <<" stateshard["<<i<<"].size()="<<resStateShard[i].size();
                }
                cout <<endl;
            }


            for (const auto& pair : resStateShard) {
                std::cout << "ZS: updateShardDAG shardDag_id: " << shardDag_id<< "StateShard: " << pair.first << ", Size "<<pair.second.size()<<endl;
                for (uint64_t val : pair.second) {
                    currentStateShard[val]=pair.first;
                }
            }
            // uint64_t temp_account_num= g_account_num+200;
            // for (uint64_t i=0;i<temp_account_num;i++){
            //     currentStateShard[i]=resStateShard[i];
            // }
        }
        std::cout << "ZS: updateShardDAG end shardDag_id: " << shardDag_id << std::endl;
    }
    void releasePendingBatchShardDag_id(uint64_t shardDag_id){
        if (PendingBatch.find(shardDag_id) != PendingBatch.end()) {
            // 获取对应的 vector 并释放其内容
            for (ClientQueryBatch* batch : PendingBatch[shardDag_id]) {
                delete batch; // 释放指针指向的内存
            }
            PendingBatch[shardDag_id].clear(); // 清空 vector
            // 从 map 中移除该项
            PendingBatch.erase(shardDag_id);
        }
    }
    void updatePendingBatchShardDag_id(uint64_t shardDag_id){
        auto it=PendingBatch.find(shardDag_id);
        if (it!=PendingBatch.end()){
            std::lock_guard<std::mutex> cSSLock(currentStateShardMutex);
            for (ClientQueryBatch* clbtch : it->second) {
                // bool old_is_cross=clbtch->is_cross_shard;
                // uint64_t u,v;
                // u=v=0;
            // for (auto* clbtch : it->second){
                // cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" before update txid="<<clbtch->txn_id
                //                 <<" DoP="<<clbtch->do_or_pending
                //                 <<" iscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
                //             for(uint32_t i=0;i<g_shard_cnt;i++){
                //                 cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
                //             }
                for (uint32_t i = 0; i < g_shard_cnt; ++i) {
                    clbtch->involved_shards[i]=false;
                }
                for (uint64_t i = 0; i < get_batch_size(); i++)
                {
                        #if BANKING_SMART_CONTRACT
                        BankingSmartContractMessage *bsc= clbtch->cqrySet[i];
                        switch (bsc->type)
                        {
                            case BSC_TRANSFER:
                            {
                                // u=bsc->inputs[0];
                                // v=bsc->inputs[2];
                                clbtch->involved_shards[currentStateShard[bsc->inputs[0]]]=true;
                                clbtch->involved_shards[currentStateShard[bsc->inputs[2]]]=true;
                                // TransferMoneySmartContract *tm = new TransferMoneySmartContract();
                                // tm->source_id = bsc->inputs[0];
                                // tm->amount = bsc->inputs[1];
                                // tm->dest_id = bsc->inputs[2];
                                // tm->type = BSC_TRANSFER;
                                break;
                            }
                            case BSC_DEPOSIT:
                            {
                                clbtch->involved_shards[currentStateShard[bsc->inputs[0]]]=true;
                                // DepositMoneySmartContract *dm = new DepositMoneySmartContract();
                                // dm->dest_id = bsc->inputs[0];
                                // dm->amount = bsc->inputs[1];
                                // dm->type = BSC_DEPOSIT;
                                break;
                            }
                            case BSC_WITHDRAW:
                            {
                                clbtch->involved_shards[currentStateShard[bsc->inputs[0]]]=true;
                                // WithdrawMoneySmartContract *wm = new WithdrawMoneySmartContract();
                                // wm->source_id = bsc->inputs[0];
                                // wm->amount = bsc->inputs[1];
                                // wm->type = BSC_WITHDRAW;
                                break;
                            }
                            default:
                                assert(0);
                                break;
                        }
                        #endif
                }
                uint64_t cnt_cross=0;
                uint64_t flag_leader_id=0;
                for (uint32_t i = 0; i < g_shard_cnt; ++i) {
                    if(clbtch->involved_shards[i]==true){
                        if(flag_leader_id==0){
                            clbtch->leader_id=i*g_shard_size;
                            flag_leader_id=1;
                        }
                        cnt_cross++;
                    }
                }
                if(clbtch->involved_shards[g_node_id/g_shard_size]==true){
                    clbtch->leader_id=g_node_id;
                }
                if(cnt_cross>1){
                    clbtch->is_cross_shard=true;
                }else{
                    clbtch->is_cross_shard=false;
                }
                // cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" after update txid="<<clbtch->txn_id
                //                 <<" DoP="<<clbtch->do_or_pending
                //                 <<" iscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
                //             for(uint32_t i=0;i<g_shard_cnt;i++){
                //                 cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
                //             }
                // if(clbtch->is_cross_shard!=old_is_cross){
                //     cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" after updatependingBatch diff txid="<<clbtch->txn_id
                //         <<" DoP="<<clbtch->do_or_pending<<" old_iscross="<<old_is_cross
                //         <<" newiscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
                //     for(uint32_t i=0;i<g_shard_cnt;i++){
                //         cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
                //     }
                //     cout <<" u="<<u<<" v="<<v<<endl;
                //     cout <<endl;
                // }else{
                //     cout<<endl<<"ZS batchid="<<clbtch->batch_id<<" after updatependingBatch same txid="<<clbtch->txn_id
                //         <<" DoP="<<clbtch->do_or_pending<<" old_iscross="<<old_is_cross
                //         <<" newiscross="<<clbtch->is_cross_shard <<"leader_id="<<clbtch->leader_id;
                //     for(uint32_t i=0;i<g_shard_cnt;i++){
                //         cout <<" s["<<i<<"]="<<clbtch->involved_shards[i];
                //     }
                //     cout <<" u="<<u<<" v="<<v<<endl;
                //     cout <<endl;
                // }
            }

        }
    }

    // void updatePendingBatchShardDag_id(uint64_t shardDag_id){
    //     auto it=PendingBatch.find(shardDag_id);
    //     if (it!=PendingBatch.end()){
    //         auto itShardDAG=PendingShardDAG.find(shardDag_id);
    //         if (itShardDAG!=PendingShardDAG.end()){
    //             for (auto* clbtch : it->second) {
    //                 for (uint32_t i = 0; i < g_shard_cnt; ++i) {
    //                     clbtch->involved_shards[i]=false;
    //                 }
    //                 for (uint64_t i = 0; i < get_batch_size(); i++)
    //                 {
    //                     #if BANKING_SMART_CONTRACT
    //                     BankingSmartContractMessage *bsc= clbtch->cqrySet[i];
    //                     switch (bsc->type)
    //                     {
    //                     case BSC_TRANSFER:
    //                     {

    //                         clbtch->involved_shards[itShardDAG->second->StateShard[bsc->inputs[0]]]=true;
    //                         clbtch->involved_shards[itShardDAG->second->StateShard[bsc->inputs[2]]]=true;
    //                         // TransferMoneySmartContract *tm = new TransferMoneySmartContract();
    //                         // tm->source_id = bsc->inputs[0];
    //                         // tm->amount = bsc->inputs[1];
    //                         // tm->dest_id = bsc->inputs[2];
    //                         // tm->type = BSC_TRANSFER;
    //                         break;
    //                     }
    //                     case BSC_DEPOSIT:
    //                     {
    //                         clbtch->involved_shards[itShardDAG->second->StateShard[bsc->inputs[0]]]=true;

    //                         // DepositMoneySmartContract *dm = new DepositMoneySmartContract();
    //                         // dm->dest_id = bsc->inputs[0];
    //                         // dm->amount = bsc->inputs[1];
    //                         // dm->type = BSC_DEPOSIT;
    //                         break;
    //                     }
    //                     case BSC_WITHDRAW:
    //                     {
    //                         clbtch->involved_shards[itShardDAG->second->StateShard[bsc->inputs[0]]]=true;
    //                         // WithdrawMoneySmartContract *wm = new WithdrawMoneySmartContract();
    //                         // wm->source_id = bsc->inputs[0];
    //                         // wm->amount = bsc->inputs[1];
    //                         // wm->type = BSC_WITHDRAW;
    //                         break;
    //                     }
    //                     default:
    //                         assert(0);
    //                         break;
    //                     }
                            
    //                     #endif


    //                 }
    //                 uint64_t cnt_cross=0;
    //                 for (uint32_t i = 0; i < g_shard_cnt; ++i) {
    //                     if(clbtch->involved_shards[i]==true){
    //                         cnt_cross++;
    //                     }
    //                 }
    //                 if(cnt_cross>1){
    //                     clbtch->is_cross_shard=true;
    //                 }else{
    //                     clbtch->is_cross_shard=false;
    //                 }
    //             }

    //         }
            
            
    //     }
    // }

    // uint64_t getLeaderId(ClientQueryBatch * clbtch){
    //     uint64_t leaderId=0;
    //     for (uint32_t i = 0; i < g_shard_cnt; ++i) {
    //         if(clbtch->involved_shards[i]==true){
    //             leaderId=i*g_shard_size;
    //             return leaderId;
    //         }
    //     }
    //     return  leaderId;
    // }

    // void setShardDAG(uint64_t shardDag_id,std::map<uint64_t, uint64_t> &resStateShard){
    //     std::lock_guard<std::mutex> shardLock(pendingShardDAGMutex);
    //     std::cout << "ZS:setShardDAG shardDag_id:" << shardDag_id << std::endl;
    //     currentStateShard_id=shardDag_id;
    //     auto itShardDAG=PendingShardDAG.find(shardDag_id);
    //     if (itShardDAG!=PendingShardDAG.end()){
    //         for (const auto& pair : resStateShard) {
    //             uint64_t key = pair.first;
    //             uint64_t value = pair.second;
    //             itShardDAG->second->StateShard[key]=value;
    //             currentStateShard[key]=value;
    //             std::cout << "ZS:Key: " << key << ", Value: " << value << std::endl;
    //         }
    //     }
    //     std::cout << "ZS:setShardDAG end shardDag_id:" << shardDag_id << std::endl;
    // }
    // void releaseShardDAGandBatch(uint64_t shardDag_id){
    //     if (PendingBatch.find(shardDag_id) != PendingBatch.end()) {
    //         for (auto* batch : PendingBatch[1]) {
    //             delete batch; 
    //         }
    //         PendingBatch.erase(shardDag_id); 
    //     }
    //     if (PendingShardDAG.find(shardDag_id) != PendingShardDAG.end()) {
    //         delete PendingShardDAG[shardDag_id];  
    //         PendingShardDAG.erase(shardDag_id); 
    //     }
    // }



    


    // 在析构函数中释放动态分配的内存
    ~DAG() {
        for (auto& pair : PendingBatch) {
            for (auto* batch : pair.second) {
                delete batch;  // 释放 ClientQueryBatch 对象的内存
            }
        }
        for (auto& pair : PendingShardDAG) {
            delete pair.second;  // 释放 ShardDAG 对象的内存
        }
    }



};
#endif // ZSDAG
#endif