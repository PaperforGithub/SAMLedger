#include "global.h"
#include "message.h"
#include "ycsb.h"
#include "thread.h"
#include "worker_thread.h"
#include "io_thread.h"
#include "math.h"
#include "query.h"
#include "transport.h"
#include "msg_queue.h"
#include "ycsb_query.h"
#include "sim_manager.h"
#include "work_queue.h"
#include "client_query.h"
#include "crypto.h"
#include "timer.h"
#include "chain.h"
#include "txn_table.h"
#include "smart_contract_txn.h"

#include <array>

void network_test();
void network_test_recv();
void *run_thread(void *);

WorkerThread *worker_thds;
InputThread *input_thds;
OutputThread *output_thds;

// defined in parser.cpp
void parser(int argc, char *argv[]);
void clean();

int main(int argc, char *argv[])
{
    // 0. initialize global data structure
    parser(argc, argv);
#if SEED != 0
    uint64_t seed = SEED + g_node_id;
#else
    uint64_t seed = get_sys_clock();
#endif
    srand(seed);
    printf("Random seed: %ld\n", seed);

    int64_t starttime;
    int64_t endtime;
    starttime = get_server_clock();
    printf("Initializing stats... ");
    fflush(stdout);
    stats.init(g_total_thread_cnt);
    printf("Done\n");

    printf("Initializing DB %s... ", db->dbInstance().c_str());
    fflush(stdout);
    db->Open(string("db-") + to_string(g_node_id));
#if ISEOV
    db->Init(std::to_string(10000000));
    sleep(2);
#endif
    
#if !IS_TABLE_DEVIDE
    printf("DB testing\nInsert key K1 with value V1\n");
    db->Put("K1", "V1");
    printf("Reading value for key K1 = %s\n", db->Get("K1").c_str());

    printf("Done\n");
    fflush(stdout);
#endif

    printf("Initializing transport manager... ");
    fflush(stdout);
    tport_man.init();
    printf("Done\n");
    fflush(stdout);

    printf("Initializing simulation... ");
    fflush(stdout);
    simulation = new SimManager;
    simulation->init();
    printf("Done\n");
    fflush(stdout);
#if BANKING_SMART_CONTRACT
    // Workload *m_wl = new SCWorkload;
    SCWorkload wl;
#else
    // Workload *m_wl = new YCSBWorkload;
    YCSBWorkload wl;
#endif

    // m_wl->init();
    wl.init();
    printf("Workload initialized!\n");
    fflush(stdout);

#if NETWORK_TEST
    tport_man.init(g_node_id, m_wl);
    sleep(3);
    if (g_node_id == 0)
        network_test();
    else if (g_node_id == 1)
        network_test_recv();

    return 0;
#endif

    printf("Initializing work queue... ");
    fflush(stdout);
    work_queue.init();
    printf("Done\n");
    printf("Initializing message queue... ");
    fflush(stdout);
    msg_queue.init();
    printf("Done\n");
    printf("Initializing transaction manager pool... ");
    fflush(stdout);
    // txn_man_pool.init(m_wl, 0);
    printf("Done\n");
    printf("Initializing transaction pool... ");
    fflush(stdout);
    // txn_pool.init(m_wl, 0);
    txn_pool.init(&wl, 0);
    printf("Done\n");
    printf("Initializing txn node table pool... ");
    fflush(stdout);
    // txn_table_pool.init(m_wl, 0);
    printf("Done\n");
    printf("Initializing query pool... ");
    fflush(stdout);
    // qry_pool.init(m_wl, 0);
    qry_pool.init(&wl, 0);
    printf("Done\n");
    printf("Initializing transaction table... ");
    fflush(stdout);
    // txn_table.init();
#if NET_BROADCAST
    txn_tables.resize(g_net_cnt);
    for(uint64_t i = 0; i < g_net_cnt; i++)
    {
        //printf("test_v4:loop:txn_tables[%ld]", i);
        TxnTable *txn_table = new TxnTable;
        txn_tables[i] = txn_table;
        //txn_tables.push_back(txn_table);
        txn_tables[i]->init(&wl);
        //printf("Done txn_tables[%ld]\n", i);
    }
#else
    txn_tables.init(&wl);
#endif
    // txn_table.init(&wl);
    // printf("Done\n");

    printf("Initializing Chain... ");
    fflush(stdout);
    BlockChain = new BChain();
    printf("Done\n");

    //test_v3:add Array
#if NET_BROADCAST
    printf("Initializing priconsensus... ");
    fflush(stdout);
    priconsensus.fill(false);
#endif

#if ZS
printf("Initializing priconsensus... ");
g_dag.init();
printf("Done\n");
fflush(stdout);
#endif

#if TIMER_ON
    printf("Initializing timers... ");
    server_timer = new ServerTimer();
#endif

#if LOCAL_FAULT || VIEW_CHANGES
    // Adding a stop_nodes entry for each output thread.
    for (uint i = 0; i < g_send_thread_cnt; i++)
    {
        vector<uint64_t> temp;
        stop_nodes.push_back(temp);
    }
#endif

#if MULTI_ON
  set_next_idx(g_node_id / g_net_cnt);  
  for(uint64_t i=0; i<get_totInstances(); i++) {
    set_primary(i,i);
  }

#endif  
  initialize_primaries();

    for (uint64_t i = 0; i < g_node_cnt + g_client_node_cnt; i++)
    {
        receivedKeys[i] = 0;
    }

#if CRYPTO_METHOD_RSA
    cout << "___________________________________RSAGenerateKeys" << endl;
    auto key = RsaGenerateHexKeyPair(3072);
    g_priv_key = key.privateKey;
    g_pub_keys[g_node_id] = key.publicKey;
    g_public_key = key.publicKey;
    for (uint64_t i = 0; i < g_node_cnt + g_client_node_cnt; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }
        receivedKeys[i]++;
    }
    cout << "_____________RSA PUBLIC KEY: " << g_public_key << endl;
    fflush(stdout);

#elif CRYPTO_METHOD_ED25519
    cout << "___________________________________ED25519GenerateKeys" << endl;
    // Initlialize signer and keys for ED25519
    ED25519GenerateKeys(g_priv_key, g_public_key);
    g_pub_keys[g_node_id] = g_public_key;
    for (uint64_t i = 0; i < g_node_cnt + g_client_node_cnt; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }
        receivedKeys[i]++;
    }
    //cout << "_____________ED25519 PRIV KEY: " << g_priv_key << endl;
    // cout << "_____________ED25519 PUBLIC KEY: " << g_public_key << endl;
    fflush(stdout);

    // TEST:
    byte bKey[CryptoPP::ed25519PrivateKey::PUBLIC_KEYLENGTH];
    copyStringToByte(bKey, g_public_key);
    CryptoPP::ed25519::Verifier verif = CryptoPP::ed25519::Verifier(bKey);

    string message = "hello";
    string signtr;
    StringSource(message, true, new SignerFilter(NullRNG(), signer, new StringSink(signtr)));

    bool valid = true;
    CryptoPP::StringSource(signtr + message, true,
                           new CryptoPP::SignatureVerificationFilter(verif,
                                                                     new CryptoPP::ArraySink((byte *)&valid, sizeof(valid))));
    if (valid == false)
    {
        assert(0);
    }

#endif

#if CRYPTO_METHOD_CMAC_AES
    cout << "___________________________________CMACGenerateKeys" << endl;
    /* 
  When we are using CMAC we just use and exchange a private key between every 
  pairs of nodes. Therefore we need to generate g_node_cnt + g_client_node_cnt -1 keys.
  For the use of this keys we need to keep in mind that id=0 is always for the 
  primary node and the id=n is for the client and that we are generating a useless key 
  for the id g_node_id
  */

    for (unsigned int i = 0; i < g_node_cnt + g_client_node_cnt; i++)
    {
        cmacPrivateKeys[i] = CmacGenerateHexKey(16);
        if (i == g_node_id)
        {
            continue;
        }
        receivedKeys[i]++;

        cout << "_____________CMAC PRIV KEY: " << cmacPrivateKeys[i] << endl;
        fflush(stdout);
    }
    cmacOthersKeys[g_node_id] = cmacPrivateKeys[g_node_id];

#endif

    // 2. spawn multiple threads
    uint64_t thd_cnt = g_thread_cnt;
    uint64_t wthd_cnt = thd_cnt;
    uint64_t rthd_cnt = g_rem_thread_cnt;
    uint64_t sthd_cnt = g_send_thread_cnt;
    uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt;

    assert(all_thd_cnt == g_this_total_thread_cnt);

    pthread_t *p_thds =
        (pthread_t *)malloc(sizeof(pthread_t) * (all_thd_cnt));
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    worker_thds = new WorkerThread[wthd_cnt];
    input_thds = new InputThread[rthd_cnt];
    output_thds = new OutputThread[sthd_cnt];

    endtime = get_server_clock();
    printf("Initialization Time = %ld\n", endtime - starttime);
    fflush(stdout);
    warmup_done = true;
    pthread_barrier_init(&warmup_bar, NULL, all_thd_cnt);

    // spawn and run txns again.
    starttime = get_server_clock();
    simulation->run_starttime = starttime;

    uint64_t id = 0;
#if NET_BROADCAST
    for (uint64_t i = 0; i < wthd_cnt - 2; i++)
#else
    for (uint64_t i = 0; i < wthd_cnt - 1; i++)
#endif
    {
        assert(id >= 0 && id < wthd_cnt);
        //cout << "test_v4: worker_thread_create: id = "<< id <<"\n";
        // worker_thds[i].init(id, g_node_id, m_wl);
        worker_thds[i].init(id, g_node_id, &wl);
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
        pthread_setname_np(p_thds[id - 1], "s_worker");
    }

    uint64_t ii = id;
#if NET_BROADCAST
    assert(id >= 0 && id < wthd_cnt -1);
#else
    assert(id >= 0 && id < wthd_cnt);
#endif
    //cout << "test_v4: ckpt_worker_thread_create: id = "<< id <<"\n";
    // worker_thds[ii].init(id, g_node_id, m_wl);
    worker_thds[ii].init(id, g_node_id, &wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[ii]);
    pthread_setname_np(p_thds[id - 1], "s_worker");

    //test_v4:add an broadcastbatch_thread;
#if NET_BROADCAST
    ii = id;
    assert(id >= 0 && id < wthd_cnt);
    //cout << "test_v4: broadcastbatch_worker_thread_create: id = "<< id <<"\n";
    // worker_thds[ii].init(id, g_node_id, m_wl);
    worker_thds[ii].init(id, g_node_id, &wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[ii]);
    pthread_setname_np(p_thds[id - 1], "s_worker");
#endif

#if !NET_BROADCAST
    set_expectedExecuteNetid(g_net_id);
#endif

    for (uint64_t j = 0; j < rthd_cnt; j++)
    {
        //cout << "test_v4: input_thread_create: id = "<< id <<"\n";
        assert(id >= wthd_cnt && id < wthd_cnt + rthd_cnt);
        // input_thds[j].init(id, g_node_id, m_wl);
        input_thds[j].init(id, g_node_id, &wl);
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&input_thds[j]);
        pthread_setname_np(p_thds[id - 1], "s_receiver");
    }

    for (uint64_t j = 0; j < sthd_cnt; j++)
    {
        //cout << "test_v4: output_thread_create: id = "<< id <<"\n";
        assert(id >= wthd_cnt + rthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt);
        // output_thds[j].init(id, g_node_id, m_wl);
        output_thds[j].init(id, g_node_id, &wl);
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&output_thds[j]);
        pthread_setname_np(p_thds[id - 1], "s_sender");
    }
#if LOGGING
    // log_thds[0].init(id, g_node_id, m_wl);
    log_thds[0].init(id, g_node_id, &wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&log_thds[0]);
    pthread_setname_np(p_thds[id - 1], "s_logger");
#endif

    for (uint64_t i = 0; i < all_thd_cnt; i++)
        pthread_join(p_thds[i], NULL);

    endtime = get_server_clock();

    fflush(stdout);
    printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
    for (uint64_t i = 0; i < g_send_thread_cnt; i++)
        printf("Output %ld: %f\n", i + g_thread_cnt + g_rem_thread_cnt, output_thd_idle_time[i] / BILLION);
    for (uint64_t i = 0; i < g_rem_thread_cnt; i++)
        printf("Input %ld: %f\n", i + g_thread_cnt, input_thd_idle_time[i] / BILLION);

    if (STATS_ENABLE)
        stats.print(false);

    printf("\n");
    fflush(stdout);
    // Free things
    //tport_man.shutdown();
    //m_wl->index_delete_all();

    // Only for end cleanup.
#if NET_BROADCAST
    for(uint64_t i = 0; i < g_net_cnt; i++)
    {
        txn_tables[i]->free();
    }
#else
    txn_tables.free();
#endif
    //txn_table.free();
    //txn_pool.free_all();
    //txn_table_pool.free_all();
    //qry_pool.free_all();
    //stats.free();
    clean();
    return 0;
}

void *run_thread(void *id)
{
    Thread *thd = (Thread *)id;
    thd->run();
    return NULL;
}

void clean(){
     txn_pool.release();
     qry_pool.release();
     work_queue.release();
     msg_queue.release();
     stats.free();
     tport_man.release();

     delete worker_thds;

     delete input_thds;
     delete output_thds;
     delete simulation;
     delete BlockChain;
 #if TIMER_ON
     delete server_timer;
 #endif
 #if EXT_DB == SQL || EXT_DB == SQL_PERSISTENT
     db->Close("");
 #elif EXT_DB == MEMORY
     db->Close("");
 #endif
 }