#ifndef _RDMAHELPERS
#define _RDMAHELPERS
#include <iostream>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

using namespace std;

typedef struct MemoryInfo
{
	char Symbol[5];
	double 	Value;
} MemoryInfo_t;

#define NUM_OPERATIONS 10000

extern struct rdma_event_channel	*g_CMEventChannel;
extern struct rdma_cm_id			*g_CMId;

extern char 						*s_dstAddr;
extern int 							n_dstPort;
extern char 						*s_srcAddr;
extern struct sockaddr_in			g_srcAddr;
extern struct sockaddr_in			g_dstAddr;

extern struct ibv_pd                   *g_pd;                            /* Protection Domain Handle */
extern struct ibv_cq                   *g_cq;                            /* Completion Queue Handle */
extern struct ibv_mr                   *g_mr;                            /* Memory Region Handle */

int PollCQ();
ibv_send_wr* create_SEND_WQE(void*, size_t, ibv_mr*);
ibv_recv_wr* create_RECEIVE_WQE(void*, size_t, ibv_mr*);
ibv_send_wr* create_WRITE_WQE(void* , size_t, ibv_mr*, void*, uint32_t);

int post_SEND_WQE(ibv_send_wr*);
int post_RECEIVE_WQE(ibv_recv_wr*);

ibv_mr *create_MEMORY_REGION(void* , size_t);

int RDMACreateQP();
int RDMACreateChannel();

int RDMAClientInit();
int RDMAServerInit();

int RDMAClientConnect();
int RDMAServerConnect();

void CleanUpCMContext();
void CleanUpQPContext();

#endif /*_RDMAHELPERS */
