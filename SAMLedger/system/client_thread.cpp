#include "global.h"
#include "thread.h"
#include "client_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "wl.h"
#include "message.h"
#include "timer.h"
#include "ring_all_comb.h"

#if GEN_ZIPF
double ClientThread::denom = 0;
#endif

void ClientThread::send_key()
{
	// Send everyone the public key.
	for (uint64_t i = 0; i < g_node_cnt + g_client_node_cnt; i++)
	{
		if (i == g_node_id)
		{
			continue;
		}

#if CRYPTO_METHOD_RSA || CRYPTO_METHOD_ED25519
		Message *msg = Message::create_message(KEYEX);
		KeyExchange *keyex = (KeyExchange *)msg;
		// The four first letters of the message set the type
#if CRYPTO_METHOD_RSA
		cout << "Sending the key RSA: " << g_public_key.size() << endl;
		keyex->pkey = "RSA-" + g_public_key;
#elif CRYPTO_METHOD_ED25519
		cout << "Sending the key ED25519: " << g_public_key.size() << endl;
		keyex->pkey = "ED2-" + g_public_key;
#endif

		keyex->pkeySz = keyex->pkey.size();
		keyex->return_node = g_node_id;

		vector<uint64_t> dest;
		dest.push_back(i);

		msg_queue.enqueue(get_thd_id(), keyex, dest);
#endif

#if CRYPTO_METHOD_CMAC_AES
		cout << "Sending the key CMAC: " << cmacPrivateKeys[i].size() << endl;
		Message *msgCMAC = Message::create_message(KEYEX);
		KeyExchange *keyexCMAC = (KeyExchange *)msgCMAC;
		keyexCMAC->pkey = "CMA-" + cmacPrivateKeys[i];

		keyexCMAC->pkeySz = keyexCMAC->pkey.size();
		keyexCMAC->return_node = g_node_id;
		//msg_queue.enqueue(get_thd_id(), keyexCMAC, i);
		msg_queue.enqueue(get_thd_id(), keyexCMAC, dest);
		dest.clear();
#endif
	}
}

void ClientThread::setup()
{

	// Increment commonVar.
	batchMTX.lock();
	commonVar++;
	batchMTX.unlock();

#if RING_BFT || SHARPER || NEW_SHARPER
	txn_batch_sent_cnt = 0;
	cross_txn_batch_sent_cnt = 0;
#endif

	if (_thd_id == 0)
	{
		while (commonVar < g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt)
			;

		send_init_done_to_all_nodes();
		send_key();
	}
#if GEN_ZIPF
	zeta_2_theta = zeta(2, g_zipf_theta);
	denom = zeta(g_account_num - 1, g_zipf_theta);
    sbmrand = (myrand *)mem_allocator.alloc(sizeof(myrand));
    sbmrand->init(get_sys_clock());
#endif
}

/*
Change for RCC
	In the while loop in this method, the client is constantly sending client_batches to server nodes,
	if there are 3 nodes, the order of nodes that the client_batches should be: 0,1,2,0,1,2,0,1,2.....
	The variable next_node_id denotes the id of the node that the next client_batch would be sent to.
	Thus, line 143 is deleted, which would reset the value of next_node_id to 0.
	And line 309 is added, which would increase the value of next_node_id by 1.
	But this only works for normal case, in order to implement the view-change sub-protocol, our system needs to adopt some other strategies
*/


RC ClientThread::run()
{
	tsetup();
	printf("Running ClientThread %ld\n", _thd_id);
	//cout << "test_v1:before while (true)\n";
	while (true)
	{
		keyMTX.lock();
		if (keyAvail)
		{
			keyMTX.unlock();
			break;
		}
		keyMTX.unlock();
	}
	//cout << "test_v1:outof while (true)\n";
#if !BANKING_SMART_CONTRACT
	BaseQuery *m_query;
#endif
	uint64_t iters = 0;
	uint32_t num_txns_sent = 0;
	int txns_sent[g_node_cnt];
	for (uint32_t i = 0; i < g_node_cnt; ++i)
		txns_sent[i] = 0;

	run_starttime = get_sys_clock();

#if USE_ETH_ACCOUNT
	read_transaction_file();
#endif

#if CLIENT_BATCH
	uint addMore = 0;

	// Initializing first batch
	Message *mssg = Message::create_message(CL_BATCH);
	ClientQueryBatch *bmsg = (ClientQueryBatch *)mssg;
	bmsg->init();
#endif

	uint32_t next_node_id = get_client_view(get_shard_number(g_node_id));
#if MULTI_ON
	uint64_t count = 0;
#endif

#if LIMIT_SEND_RATE
	uint64_t epoch_send_count = 0;
	uint64_t epoch_starttime = get_sys_clock();
	#if MULTI_CLIENT_SEND_RATE
		uint64_t send_rate = g_client_send_rate * g_client_send_rates_ctrl[g_node_id - g_node_cnt] / 100;
	#else
		uint64_t send_rate = g_client_send_rate;
	#endif
#endif

#if NEW_SHARPER
	#if !USE_ETH_ACCOUNT
	uint64_t random_shard;
	uint64_t source_shard;
	uint64_t local_shard = get_shard_number(g_node_id);
	source_shard = local_shard;
	#if RANDOM_CHOOSE_SHARD
		if (static_cast<uint64_t>(rand() % 100) < g_cross_shard_prob) {
			bmsg->is_cross_shard = true;
			// 随机选择源分片和目标分片
			uint64_t rand_num = rand() % 100;
			uint64_t prob_sum = 0;
			source_shard = 0;
			
			// 根据阶梯概率选择源分片
			for(uint64_t i = 0; i < g_shard_cnt; i++) {
				prob_sum += g_random_choose_shard_prob[i];
				if(rand_num < prob_sum) {
					source_shard = i;
					break;
				}
			}
			
			// 随机选择不同的目标分片
			do {
				rand_num = rand() % 100;
				prob_sum = 0;
				random_shard = 0;
				for(uint64_t i = 0; i < g_shard_cnt; i++) {
					prob_sum += g_random_choose_shard_prob[i];
					if(rand_num < prob_sum) {
						random_shard = i;
						break;
					}
				}
			} while(random_shard == source_shard);
			
			// 设置跨分片标记
			bmsg->involved_shards[source_shard] = true;
			bmsg->involved_shards[random_shard] = true;
			#if TX_CROSS_FOUR_SHARD
				for(uint64_t i = 0; i < 4; i++) {
					bmsg->involved_shards[i] = true;
				}
			#endif
		}
		#if RANDOM_CHOOSE_NO_CROSS
		else{
			uint64_t rand_num = rand() % 100;
			uint64_t prob_sum = 0;
			source_shard = 0;
			
			// 根据阶梯概率选择源分片
			for(uint64_t i = 0; i < g_shard_cnt; i++) {
				prob_sum += g_random_choose_no_cross_shard_prob[i];
				if(rand_num < prob_sum) {
					source_shard = i;
					break;
				}
			}
			random_shard = source_shard;
		}
		#endif
	#else
    	if (static_cast<uint64_t>(rand() % 100) < g_cross_shard_prob) {
    	    bmsg->is_cross_shard = true;
    	    // 设置本地分片为 true
    	    bmsg->involved_shards[get_shard_number(g_node_id)] = true;
    	    // 随机选择一个其他分片设置为 true
    	    do {
    	        random_shard = rand() % g_shard_cnt;
    	    } while (random_shard == get_shard_number(g_node_id));
    	    bmsg->involved_shards[random_shard] = true;
    	}
	#endif

	#endif

	#if REPEAT_TXN
	        uint64_t source;
        	uint64_t dest;
		#if USE_ETH_ACCOUNT
			static size_t txn_idx = 0;
			uint64_t source_shard_id;
        	uint64_t dest_shard_id;
			bool cross_shard_flag = false;
			if (txn_idx >= transaction_list.size()) {
				txn_idx = 0;
			}
			source = transaction_list[txn_idx].first;
			dest = transaction_list[txn_idx].second;
			txn_idx++;
			#if INIT_SHARD_BY_DIV
				source_shard_id = source % g_shard_cnt;
				dest_shard_id = dest % g_shard_cnt;
			#else
				source_shard_id = source / (g_account_num / g_shard_cnt);
				dest_shard_id = dest / (g_account_num / g_shard_cnt);
			#endif
			if(source_shard_id != dest_shard_id){
				cross_shard_flag = true;
				bmsg->is_cross_shard = cross_shard_flag;
				bmsg->involved_shards[source_shard_id] = true;
				bmsg->involved_shards[dest_shard_id] = true;
			#if TX_CROSS_FOUR_SHARD
				for(uint64_t i = 0; i < 4; i++) {
					bmsg->involved_shards[i] = true;
				}
			#endif
			}
			next_node_id = get_client_view(source_shard_id);
			cout << "cross_shard_tx, source_shard_id: " << source_shard_id << ", dest_shard_id: " << dest_shard_id << ", next_node_id = " << next_node_id << endl;
		#else
			#if RANDOM_CHOOSE_SHARD
				next_node_id = get_client_view(source_shard);
			#endif
			if(bmsg->is_cross_shard == true){
				source = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
				dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * random_shard;
				#if ISEOV
					while (dest == source){
						dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * random_shard;
						//cout << "regenerate dest" << endl;
					}
				#endif
			}
			else{
				source = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
				dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
				#if ISEOV
					while (dest == source){
						dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
						//cout << "regenerate dest" << endl;
					}
				#endif
			}
		#endif
	#endif
#endif
	//cout << "test_v1:before client while\n";
	while (!simulation->is_done())
	{
		heartbeat();
		progress_stats();
#if LIMIT_SEND_RATE
		uint64_t new_epoch_time = get_sys_clock();
		if(epoch_send_count > send_rate){
			if(new_epoch_time - epoch_starttime < BILLION) {continue;}
			else{
				epoch_starttime = new_epoch_time;
				epoch_send_count = 0;
			}
		}
#endif

		int32_t inf_cnt;

#if !USE_ETH_ACCOUNT
	#if RANDOM_CHOOSE_SHARD
		uint32_t next_node = get_client_view(source_shard);
		next_node_id = next_node;
	#else
		#if MULTI_ON
		uint32_t next_node = get_client_view() + count * CLIENT_NODE_CNT;
		#else
		uint32_t next_node = get_client_view(get_shard_number(g_node_id));
		// cout << "test_v5:next_node = " << next_node << "\n";
		#endif
		next_node_id = next_node;
		//cout << "test_v1:enter client while\n";
	#endif
#else
		uint32_t next_node = get_client_view(source_shard_id);
		next_node_id = next_node;
#endif

#if VIEW_CHANGES
		//if a request by this client hasnt been completed in time
		ClientQueryBatch *cbatch = NULL;
		if (client_timer->checkTimer(cbatch))
		{
			cout << "TIMEOUT!!!!!!\n";
#if RING_BFT
			//TODO for experimental purpose: force one view change
			if (view_to_primary(get_client_view(get_shard_number())) % g_shard_size == 0)
				resend_msg(cbatch);
#else
			//TODO for experimental purpose: force one view change
			if (get_client_view() == 0)
			 	resend_msg(cbatch);
#endif
		}
#endif

#if LOCAL_FAULT
		//if a request by this client hasnt been completed in time
		ClientQueryBatch *cbatch = NULL;
		if (client_timer->checkTimer(cbatch))
		{
			cout << "TIMEOUT!!!!!!\n";
		}
#endif	//LOCAL_FALUT

		// Just in case...
		if (iters == UINT64_MAX)
			iters = 0;

#if !CLIENT_BATCH // If client batching disable
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;

		m_query = client_query_queue.get_next_query(next_node, _thd_id);
		if (last_send_time > 0)
		{
			INC_STATS(get_thd_id(), cl_send_intv, get_sys_clock() - last_send_time);
		}
		last_send_time = get_sys_clock();
		assert(m_query);

		DEBUG("Client: thread %lu sending query to node: %u, %d, %f\n",
			  _thd_id, next_node_id, inf_cnt, simulation->seconds_from_start(get_sys_clock()));

		Message *msg = Message::create_message((BaseQuery *)m_query, CL_QRY);
		((ClientQueryMessage *)msg)->client_startts = get_sys_clock();

		YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)msg;
		clqry->return_node = g_node_id;

		msg_queue.enqueue(get_thd_id(), msg, next_node_id);
		num_txns_sent++;
		txns_sent[next_node]++;
		INC_STATS(get_thd_id(), txn_sent_cnt, 1);

#else // If client batching enable

		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
		{
			continue;
		}
#if BANKING_SMART_CONTRACT
	#if !REPEAT_TXN
        uint64_t source;
        uint64_t dest;
	#endif
#if GEN_ZIPF
#if NEW_SHARPER
	#if REPEAT_TXN
	#else
	if(bmsg->is_cross_shard == true){
		source = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * local_shard;
		dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * random_shard;
		#if ISEOV
			while (dest == source){
				dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * random_shard;
				//cout << "regenerate dest" << endl;
			}
		#endif
	}
	else{
		source = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * local_shard;
		dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * local_shard;
		#if ISEOV
			while (dest == source){
				dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * local_shard;
				//cout << "regenerate dest" << endl;
			}
		#endif
	}
	#endif
#else
	uint64_t source = zipf(g_account_num - 1, g_zipf_theta);
	uint64_t dest = zipf(g_account_num - 1, g_zipf_theta);
	#if ISEOV
		while (dest == source){
			dest = zipf(g_account_num - 1, g_zipf_theta);
			//cout << "regenerate dest" << endl;
		}
	#endif
#endif

#else
	#if GEN_HOT
			uint64_t is_hot = false;
        	if(((uint64_t)rand() % 100) < g_read_hot){
        	    is_hot = true;
        	}
			uint64_t source;
			uint64_t dest;
        	if(is_hot){
        	    source = (uint64_t)rand() % (uint64_t)(g_account_num * g_hotness);
				dest = (uint64_t)rand() % (uint64_t)(g_account_num * g_hotness);
        	}
        	else{
        	    source = g_account_num * g_hotness + (uint64_t)rand() % (uint64_t)(g_account_num * (1 - g_hotness));
				dest = g_account_num * g_hotness + (uint64_t)rand() % (uint64_t)(g_account_num * (1 - g_hotness));
        	}
			

		#if ISEOV
			while (dest == source){
        		if(is_hot){
					dest = (uint64_t)rand() % (uint64_t)(g_account_num * g_hotness);
        		}
        		else{
					dest = g_account_num * g_hotness + (uint64_t)rand() % (uint64_t)(g_account_num * (1 - g_hotness));
        		}
				//cout << "regenerate dest" << endl;
			}
		#endif	
	#else
			uint64_t source = (uint64_t)rand() % g_account_num;
			uint64_t dest = (uint64_t)rand() % g_account_num;
		#if ISEOV
			while (dest == source){
				dest = (uint64_t)rand() % g_account_num;
				//cout << "regenerate dest" << endl;
			}
		#endif
	#endif
#endif
#if USE_ETH_ACCOUNT || RANDOM_CHOOSE_SHARD
		uint64_t amount = 1;
#else
		uint64_t amount = (uint64_t)rand() % 100;
#endif
		BankingSmartContractMessage *clqry = new BankingSmartContractMessage();
		//cout << "test_v7:before clqry->rtype = BSC_MSG, txn_id = "<<clqry->txn_id<<"\n";
		clqry->rtype = BSC_MSG;
//		clqry->inputs.init(2);
//		clqry->type = (BSCType)(1);
#if TWO_KIND_SB
		clqry->inputs.init(!(addMore % 2) ? 3 : 2);
		if (addMore % 2 == 0){
			clqry->type = (BSCType)(0);
		}
		else clqry->type = (BSCType)(2);
		clqry->inputs.add(source);
		clqry->inputs.add(amount);
		((ClientQueryMessage *)clqry)->client_startts = get_sys_clock();
		if (addMore % 2 == 0)
			clqry->inputs.add(dest);
#else
	#if !USE_ETH_ACCOUNT && !RANDOM_CHOOSE_SHARD
		clqry->inputs.init(!(addMore % 3) ? 3 : 2);
		//cout << "test_v7:before clqry->type = (BSCType)(addMore % 3), txn_id = "<<clqry->txn_id<<"\n";
		clqry->type = (BSCType)(addMore % 3);
		clqry->inputs.add(source);
		clqry->inputs.add(amount);
		((ClientQueryMessage *)clqry)->client_startts = get_sys_clock();
		if (addMore % 3 == 0)
			clqry->inputs.add(dest);
	#else
		clqry->inputs.init(3);
		clqry->type = (BSCType)(0);
		clqry->inputs.add(source);
		//cout<<"test_v7:before clqry->inputs.add(amount), amount = "<<amount<<endl;
		clqry->inputs.add(amount);
		((ClientQueryMessage *)clqry)->client_startts = get_sys_clock();
		clqry->inputs.add(dest);
	#endif
#endif
		clqry->return_node_id = g_node_id;
#else
		//cout << "before:m_query = client_query_queue.get_next_query(_thd_id)\n";
		m_query = client_query_queue.get_next_query(_thd_id);
		if (last_send_time > 0)
		{
			INC_STATS(get_thd_id(), cl_send_intv, get_sys_clock() - last_send_time);
		}
		last_send_time = get_sys_clock();
		assert(m_query);

		Message *msg = Message::create_message((BaseQuery *)m_query, CL_QRY);
		//cout << "test_v1:Message *msg = Message::create_message((BaseQuery *)m_query, CL_QRY)\n";
		((ClientQueryMessage *)msg)->client_startts = get_sys_clock();

		YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)msg;
		clqry->return_node = g_node_id;

#endif

		bmsg->cqrySet.add(clqry);

		addMore++;

		// Resetting and sending the message
		if (addMore == g_batch_size)
		{
			//cout << "test_v7:before bmsg->batch_id = clqry->txn_id / g_batch_size, txn_id = "<<clqry->txn_id<<"\n";
			bmsg->batch_id = clqry->txn_id / g_batch_size;
			//cout << "test_v3:batch_id(wrong) = " << bmsg->batch_id << "\n";
#if PRE_ORDER
			bmsg->generate_input();
			bmsg->generate_output();
#endif
			bmsg->sign(next_node_id); // Sign the message.

#if TIMER_ON
			char *buf = create_msg_buffer(bmsg);
			Message *deepCMsg = deep_copy_msg(buf, bmsg);
			ClientQueryBatch *deepCqry = (ClientQueryBatch *)deepCMsg;
			uint64_t c_txn_id = get_sys_clock();
			deepCqry->txn_id = c_txn_id;
			client_timer->startTimer(deepCqry->cqrySet[get_batch_size() - 1]->client_startts, deepCqry);
			delete_msg_buffer(buf);
#endif // TIMER_ON

#if !USE_ETH_ACCOUNT
			vector<uint64_t> destination;
			destination.push_back(next_node_id);
			cout << "next_node_id: " << next_node_id;
			// cout << "  return_node_id: " << bmsg->return_node_id << "  rtype: " << bmsg->rtype << endl; 
			fflush(stdout);
			//DEBUG("send_client_batch! requests.size: %ld requests_writeset.size: %ld\n",bmsg->cqrySet[0]->requests.size(), bmsg->cqrySet[0]->requests_writeset.size());
			//DEBUG("send_client_batch! requests.size: %ld",bmsg->cqrySet[0]->requests.size());
			msg_queue.enqueue(get_thd_id(), bmsg, destination);
			destination.clear();
#else
			vector<uint64_t> destination;
			destination.push_back(next_node_id);
			msg_queue.enqueue(get_thd_id(), bmsg, destination);
			destination.clear();
#endif

			num_txns_sent += g_batch_size;
			txns_sent[next_node] += g_batch_size;
			INC_STATS(get_thd_id(), txn_sent_cnt, g_batch_size);

			mssg = Message::create_message(CL_BATCH);
			bmsg = (ClientQueryBatch *)mssg;
			
#if LIMIT_SEND_RATE
			epoch_send_count += g_batch_size;
#endif
			bmsg->init();
			addMore = 0;

#if NEW_SHARPER
    	#if !USE_ETH_ACCOUNT
		#if RANDOM_CHOOSE_SHARD
				if (static_cast<uint64_t>(rand() % 100) < g_cross_shard_prob) {
				bmsg->is_cross_shard = true;
				// 随机选择源分片和目标分片
				uint64_t rand_num = rand() % 100;
				uint64_t prob_sum = 0;
				source_shard = 0;

				// 根据阶梯概率选择源分片
				for(uint64_t i = 0; i < g_shard_cnt; i++) {
					prob_sum += g_random_choose_shard_prob[i];
					if(rand_num < prob_sum) {
						source_shard = i;
						break;
					}
				}

				// 随机选择不同的目标分片
				do {
					rand_num = rand() % 100;
					prob_sum = 0;
					random_shard = 0;
					for(uint64_t i = 0; i < g_shard_cnt; i++) {
						prob_sum += g_random_choose_shard_prob[i];
						if(rand_num < prob_sum) {
							random_shard = i;
							break;
						}
					}
				} while(random_shard == source_shard);

				// 设置跨分片标记
				bmsg->involved_shards[source_shard] = true;
				bmsg->involved_shards[random_shard] = true;
				#if TX_CROSS_FOUR_SHARD
					for(uint64_t i = 0; i < 4; i++) {
						bmsg->involved_shards[i] = true;
					}
				#endif
			}
			else{
			#if RANDOM_CHOOSE_NO_CROSS
				uint64_t rand_num = rand() % 100;
				uint64_t prob_sum = 0;
				source_shard = 0;
				
				// 根据阶梯概率选择源分片
				for(uint64_t i = 0; i < g_shard_cnt; i++) {
					prob_sum += g_random_choose_no_cross_shard_prob[i];
					if(rand_num < prob_sum) {
						source_shard = i;
						break;
					}
				}
				random_shard = source_shard;
			#else
				source_shard = local_shard;
			#endif
			}
		#else
			if (static_cast<uint64_t>(rand() % 100) < g_cross_shard_prob) {
    		    bmsg->is_cross_shard = true;
    		    // 设置本地分片为 true
    		    bmsg->involved_shards[get_shard_number(g_node_id)] = true;
    		    // 随机选择一个其他分片设置为 true
    		    do {
    		        random_shard = rand() % g_shard_cnt;
    		    } while (random_shard == get_shard_number(g_node_id));
    		    bmsg->involved_shards[random_shard] = true;
    		}
		#endif
		#endif


		#if REPEAT_TXN
			#if USE_ETH_ACCOUNT
				static size_t txn_idx = 0;
				if (txn_idx >= transaction_list.size()) {
					txn_idx = 0;
				}
				cross_shard_flag = false;
				source = transaction_list[txn_idx].first;
				dest = transaction_list[txn_idx].second;
				//cout << "repeat_txn, source: " << source << ", dest: " << dest << endl;
				txn_idx++;
			#if INIT_SHARD_BY_DIV
				source_shard_id = source % g_shard_cnt;
				dest_shard_id = dest % g_shard_cnt;
			#else
				source_shard_id = source / (g_account_num / g_shard_cnt);
				dest_shard_id = dest / (g_account_num / g_shard_cnt);
			#endif
				if(source_shard_id != dest_shard_id){
					cross_shard_flag = true;
					bmsg->is_cross_shard = cross_shard_flag;
					bmsg->involved_shards[source_shard_id] = true;
					bmsg->involved_shards[dest_shard_id] = true;
				#if TX_CROSS_FOUR_SHARD
				for(uint64_t i = 0; i < 4; i++) {
					bmsg->involved_shards[i] = true;
					}
				#endif
					//cout << "cross_shard_tx, source_shard_id: " << source_shard_id << ", dest_shard_id: " << dest_shard_id << ", next_node_id = " << next_node_id  << endl;
				}
			#else
			if(bmsg->is_cross_shard == true){
				source = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
				dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * random_shard;
				#if ISEOV
					while (dest == source){
						dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * random_shard;
						//cout << "regenerate dest" << endl;
					}
				#endif
			}
			else{
				source = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
				dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
				#if ISEOV
					while (dest == source){
						dest = zipf(g_account_num / g_shard_cnt - 1, g_zipf_theta) + (g_account_num / g_shard_cnt) * source_shard;
						//cout << "regenerate dest" << endl;
					}
				#endif
			}
			#endif
		#endif
#endif

#if MULTI_ON
			count += 1;
			if(get_client_view() + count * CLIENT_NODE_CNT >= get_totInstances()){
				count = 0;
			}
#endif
		}

#endif // Batch Enable
	}

	printf("FINISH %ld:%ld\n", _node_id, _thd_id);
	fflush(stdout);
	return FINISH;
}

#if VIEW_CHANGES
#if RING_BFT
// Resend message to all the servers.
void ClientThread::resend_msg(ClientQueryBatch *symsg)
{
	//cout << "Resend: " << symsg->cqrySet[get_batch_size()-1]->client_startts << "\n";
	//fflush(stdout);

	char *buf = create_msg_buffer(symsg);
	uint64_t first_node_in_shard = get_shard_number(g_node_id) * g_shard_size;
	for (uint64_t j = first_node_in_shard; j < first_node_in_shard + g_shard_size; j++)
	{
		vector<uint64_t> dest;
		dest.push_back(j);

		Message *deepCMsg = deep_copy_msg(buf, symsg);
		msg_queue.enqueue(get_thd_id(), deepCMsg, dest);
		dest.clear();
	}
	delete_msg_buffer(buf);
	Message::release_message(symsg);
}
#else
// Resend message to all the servers.
void ClientThread::resend_msg(ClientQueryBatch *symsg)
{
	//cout << "Resend: " << symsg->cqrySet[get_batch_size()-1]->client_startts << "\n";
	//fflush(stdout);

	char *buf = create_msg_buffer(symsg);
	for (uint64_t j = 0; j < g_node_cnt; j++)
	{
		vector<uint64_t> dest;
		dest.push_back(j);

		Message *deepCMsg = deep_copy_msg(buf, symsg);
		msg_queue.enqueue(get_thd_id(), deepCMsg, dest);
		dest.clear();
	}
	delete_msg_buffer(buf);
	Message::release_message(symsg);
}
#endif
#endif // VIEW_CHANGES

#if GEN_ZIPF
uint64_t ClientThread::zipf(uint64_t n, double theta)
{
    //assert(this->the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
                 (1 - zeta_2_theta / zetan);
    //	double eta = (1 - pow(2.0 / n, 1 - theta)) /
    //		(1 - zeta_2_theta / zetan);
    double u = (double)(sbmrand->next() % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1)
        return 1;
    if (uz < 1 + pow(0.5, theta))
        return 2;
    return 1 + (uint64_t)(n * pow(eta * u - eta + 1, alpha));
}

double ClientThread::zeta(uint64_t n, double theta)
{
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}
#endif

#if USE_ETH_ACCOUNT
void ClientThread::read_transaction_file() {
    transaction_file.open(transaction_file_path);
    if (!transaction_file.is_open()) {
        cout << "ERROR: Cannot open file: " << transaction_file_path << endl;
        assert(false);
    }
	transaction_list.clear();
	string line;
	while (getline(transaction_file, line)) {
		size_t comma_pos = line.find(',');
		transaction_list.push_back(make_pair(stoull(line.substr(0, comma_pos)), stoull(line.substr(comma_pos + 1))));
	}
	transaction_file.close();
	return;
}
#endif