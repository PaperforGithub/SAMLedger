#ifndef _CONFIG_H_
#define _CONFIG_H_
// Specify the number of servers or replicas

//******NEED TO MODIFY START******

//net config
#define NODE_CNT 12
#define NET_CNT 1
#define CLIENT_NODE_CNT 2
#define NET_BROADCAST true
#define MULTI_ON false
#define MULTI_INSTANCES 1

//architecture
#define ISEOV true
#define RE_EXECUTE true
#define ABORT_BATCH true
#define MERGE_PERCENT 40
#define PRE_EX true

#define BATCH_SIZE 1000

#define P2P_BROADCAST true
//benchmark
#define BANKING_SMART_CONTRACT true
#define SB_READ_TX true
#define ACCOUNT_NUM 1000000
#define GEN_ZIPF true
#define ZIPF_THETA 0


#define SYNTH_TABLE_SIZE 50000
#define YCSB_COLUMN 10
#define YCSB_WRITE_RATIO 50

//client
#define LIMIT_SEND_RATE false
#define CLIENT_SEND_RATE 8000

//******NEED TO MODIFY END******

#define DONE_TIMER 60 * BILLION
#define WARMUP_TIMER 10 * BILLION

// make clean; make -j8; python3 scripts/ifconfig.py
// python3 scripts/StopSystem.py; python3 scripts/scp_binaries.py; python3 scripts/RunSystem.py
// python3 scripts/StopSystem.py; python3 scripts/scp_results.py
// python3 scripts/results_analysis.py
// Number of worker threads at primary. For RBFT (6) and other algorithms (5) and RCC(NODE_CNT+3).
#define THREAD_CNT 4
#define REM_THREAD_CNT 2
#define SEND_THREAD_CNT 2
#define CL_THD_CNT 3
#define CORE_CNT 8
#define PART_CNT 1
// Specify the number of clients.

#define CLIENT_THREAD_CNT 1
#define CLIENT_REM_THREAD_CNT 12
#define CLIENT_SEND_THREAD_CNT 1
#define CLIENT_RUNTIME false
// Sharding Config
#define RING_BFT false
#define SHARPER false
#define SHARD_SIZE 4
#define CROSS_SHARD_PRECENTAGE 0
#define INVOLVED_SHARDS_NUMBER 0
#define MESSAGE_PER_BUFFER 3

#define LOAD_PER_SERVER 1
#define REPLICA_CNT 0
#define REPL_TYPE AP
#define VIRTUAL_PART_CNT PART_CNT
#define PAGE_SIZE 4096
#define CL_SIZE 64
#define CPU_FREQ 2.2
#define HW_MIGRATE false
#define WARMUP 0
#define WORKLOAD YCSB
#define PRT_LAT_DISTR false
#define STATS_ENABLE true
#define STATS_DETAILED false
#define STAT_BAND_WIDTH_ENABLE false
#define TIME_ENABLE true
#define TIME_PROF_ENABLE false
#define FIN_BY_TIME true
// Number of transactions each client should send without waiting.
// #define MAX_TXN_IN_FLIGHT (400*BATCH_SIZE)
// #define MAX_TXN_IN_FLIGHT (400*BATCH_SIZE)
#define MAX_TXN_IN_FLIGHT ((400*BATCH_SIZE)>40000 ? 40000 :(400*BATCH_SIZE))
// #define MAX_TXN_IN_FLIGHT ((NET_CNT*100*BATCH_SIZE)>NET_CNT*10000 ? NET_CNT*10000 :(NET_CNT*100*BATCH_SIZE))
// #define MAX_TXN_IN_FLIGHT 40000
#define SERVER_GENERATE_QUERIES false
#define MEM_ALLIGN 8
#define THREAD_ALLOC false
#define THREAD_ARENA_SIZE (1UL << 22)
#define MEM_PAD true
#define PART_ALLOC false
#define MEM_SIZE (1UL << 30)
#define NO_FREE false
#define TPORT_TYPE TCP
#define TPORT_PORT 10000
#define TPORT_WINDOW 20000
#define SET_AFFINITY false
#define MAX_TPORT_NAME 128
#define MSG_SIZE 128
#define HEADER_SIZE sizeof(uint32_t) * 2
#define MSG_TIMEOUT 5000000000UL // in ns
#define NETWORK_TEST false
#define NETWORK_DELAY_TEST false
#define NETWORK_DELAY 0UL
#define MAX_QUEUE_LEN NODE_CNT * 2
#define PRIORITY_WORK_QUEUE false
#define PRIORITY PRIORITY_ACTIVE
#define MSG_SIZE_MAX 1048576
#define MSG_TIME_LIMIT 0
#define KEY_ORDER false
#define ENABLE_LATCH false
#define CENTRAL_INDEX false
#define CENTRAL_MANAGER false
#define INDEX_STRUCT IDX_HASH
#define BTREE_ORDER 16
#define TS_TWR false
#define TS_ALLOC TS_CLOCK
#define TS_BATCH_ALLOC false
#define TS_BATCH_NUM 1
#define HIS_RECYCLE_LEN 10
#define MAX_PRE_REQ MAX_TXN_IN_FLIGHT *NODE_CNT
#define MAX_READ_REQ MAX_TXN_IN_FLIGHT *NODE_CNT
#define MIN_TS_INTVL 10 * 1000000UL
#define MAX_WRITE_SET 10
#define PER_ROW_VALID false
#define TXN_QUEUE_SIZE_LIMIT THREAD_CNT
#define SEQ_THREAD_CNT 4
#define MAX_ROW_PER_TXN 64
#define QUERY_INTVL 1UL
#define MAX_TXN_PER_PART 3000000
//#define MAX_TXN_PER_PART 4000
#define FIRST_PART_LOCAL true
#define MAX_TUPLE_SIZE 1024
#define GEN_BY_MPR false
#define SKEW_METHOD ZIPF
#define DATA_PERC 100
#define ACCESS_PERC 0.03
#define INIT_PARALLELISM 1
//#define INIT_PARALLELISM 8
//#define SYNTH_TABLE_SIZE 524288



#define WRITE_PERC 0.9
#define TXN_WRITE_PERC 0.5
#define TUP_WRITE_PERC 0.5
#define SCAN_PERC 0
#define SCAN_LEN 20
#define PART_PER_TXN PART_CNT
#define PERC_MULTI_PART MPR
#define REQ_PER_QUERY 1
#define FIELD_PER_TUPLE 10
#define CREATE_TXN_FILE false
#define SINGLE_THREAD_WL_GEN true
#define STRICT_PPT 1
#define MPR 1.0
#define MPIR 0.01
#define WL_VERB true
#define IDX_VERB false
#define VERB_ALLOC true
#define DEBUG_LOCK false
#define DEBUG_TIMESTAMP false
#define DEBUG_SYNTH false
#define DEBUG_ASSERT false
#define DEBUG_DISTR false
#define DEBUG_ALLOC false
#define DEBUG_RACE false
#define DEBUG_TIMELINE false
#define DEBUG_BREAKDOWN false
#define DEBUG_LATENCY false
#define DEBUG_QUECC false
#define DEBUG_WLOAD false
#define DEBUG_V1_N true
#define MODE NORMAL_MODE
#define DBTYPE REPLICATED
#define IDX_HASH 1
#define IDX_BTREE 2
#define YCSB 1
#define TEST 4
#define TS_MUTEX 1
#define TS_CAS 2
#define TS_HW 3
#define TS_CLOCK 4
#define ZIPF 1
#define HOT 2
#define PRIORITY_FCFS 1
#define PRIORITY_ACTIVE 2
#define PRIORITY_HOME 3
#define AA1 1
#define AP 2
#define LOAD_MAX 1
#define LOAD_RATE 2
#define TCP 1
#define IPC 2
#define BILLION 1000000000UL
#define MILLION 1000000UL
#define STAT_ARR_SIZE 1024
#define PROG_TIMER 1 * BILLION
#define SEQ_BATCH_TIMER 5 * 1 * MILLION
#define SEED 0
#define SHMEM_ENV false
#define ENVIRONMENT_EC2 false
#define PARTITIONED 0
#define REPLICATED 1
// To select the amount of time to warmup and run.

// Select the consensus algorithm to run.
#define CONSENSUS PBFT
#define DBFT 1
#define PBFT 2
#define ZYZZYVA 3
#define HOTSTUFF 4
// Switching on RBFT consensus.
// Status: Partial implementation, only for PBFT.
#define RBFT_ON false
// Select the type of RBFT, (1) RBFT+PBFT, and  (2) RBFT+DBFT
#define RBFT_ALG RPBFT
#define RPBFT 1
#define RDBFT 2
// Enable or Disable pipeline at primary replica.
#define ENABLE_PIPELINE false
// Size of each batch.

#define BATCH_ENABLE BSET
#define BSET 1
#define BUNSET 0
// Number of transactions to wait for period checkpointing.
#define TXN_PER_CHKPT NODE_CNT * BATCH_SIZE
#define EXECUTION_THREAD true
#define EXECUTE_THD_CNT 1
#define SIGN_THREADS false
#define SIGN_THD_CNT 1
#define CLIENT_BATCH true
#define CLIENT_RESPONSE_BATCH true
// To Enable or disable the blockchain implementation.
#define ENABLE_CHAIN false
// To fail non-primary replicas.
#define LOCAL_FAULT false
#define NODE_FAIL_CNT 1
// To allow view changes.
#define VIEW_CHANGES false
// The amount of timeout value.
#define EXE_TIMEOUT 100000000000    //*10[Dakai]
#define CEXE_TIMEOUT 120000000000      //*10
// To turn the timer on.
#define TIMER_ON false
//Global variables to choose the encryptation algorithm
#define USE_CRYPTO true
#define CRYPTO_METHOD_RSA false     //Options RSA,
#define CRYPTO_METHOD_ED25519 true  // Option ED25519
#define CRYPTO_METHOD_CMAC_AES true // CMAC<AES>
// Test cases to check basic functioning.
// Status: Implementation only for PBFT.
#define TESTING_ON false
#define TEST_CASE ONLY_PRIMARY_BATCH_EXECUTE
#define ONLY_PRIMARY_NO_EXECUTE 1
#define ONLY_PRIMARY_EXECUTE 2
#define ONLY_PRIMARY_BATCH_EXECUTE 3
// Message Payload.
// We allow creation of two different message payloads,
// to see affects on latency and throughput.
// These payloads are added to each message.
#define PAYLOAD_ENABLE false
#define PAYLOAD M100
#define M100 1 // 100KB.
#define M200 2 // 200KB.
#define M400 3 // 400KB.

// To allow testing in-memory database or SQLite.
// Further, using SQLite a user can also choose to persist the data.
#define EXT_DB MEMORY
#define MEMORY 1
#define SQL 2
#define SQL_PERSISTENT 3

// To allow testing of a Banking Smart Contracts.

#define MULTI_THREADS (MULTI_INSTANCES>16 ? 16:MULTI_INSTANCES)

#define KDK false
#define KDK_DEBUG1 false
#define KDK_DEBUG2 false
#define KDK_DEBUG3 true
#define KDK_DEBUG5 true
#define SEMA_TEST false
#define FIX_INPUT_THREAD_BUG true
#define FIX_CL_INPUT_THREAD_BUG true
#define TRANSPORT_OPTIMIZATION true

#define IN_RECV false

#define LARGER_TXN false

#define PRICONSENSUS_SIZE 300000


#define CHECK_CONFILICT true

#define CALCULATE_LATENCY true
#define DISABLE_CHECKPOINT true

//discard
#define STRONG_SERIAL false
#define PRE_ORDER false
#define MINI_NET false

#define IS_TABLE_DEVIDE false
#define TABLE_NUM 4
#define TWO_KIND_SB false

#endif
