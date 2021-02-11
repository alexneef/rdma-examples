#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <cstring>

#include "instrument.h"
#include "RDMAHelpers.h"

using namespace std;

extern instrument_t 			instrument;

struct rdma_event_channel		*g_CMEventChannel;
struct rdma_cm_id				*g_CMId;

char 							*s_dstAddr;
char 							*s_srcAddr;
int 							n_dstPort;
struct sockaddr_in				g_srcAddr;
struct sockaddr_in				g_dstAddr;

struct ibv_pd                   *g_pd;                            /* Protection Domain Handle */
struct ibv_cq                   *g_cq;                            /* Completion Queue Handle */
struct ibv_mr                   *g_mr;                            /* Memory Region Handle */

/*
 * Returns Number of Completions Received
 */
int PollCQ()
{
	struct ibv_wc wc;
	int ret = 0;

	fprintf(stderr, "Waiting for CQE\n");
	do {
		ret = ibv_poll_cq(g_cq, 10, &wc);
	} while(ret == 0);
	fprintf(stderr, "Received %u CQE Elements\n", ret);
	fprintf(stderr, "WRID(%llu)\tStatus(%u)\n", wc.wr_id, wc.status);
	return ret;
}

ibv_send_wr* create_SEND_WQE(void* buffer, size_t bufferlen, ibv_mr* bufferMemoryRegion)
{
	struct ibv_send_wr *wqe;
	struct ibv_sge *sge;

	wqe = (ibv_send_wr *)malloc(sizeof(ibv_send_wr));
	sge = (ibv_sge *)malloc(sizeof(ibv_sge));

	memset(wqe, 0, sizeof(ibv_send_wr));
	memset(sge, 0, sizeof(ibv_sge));

	wqe->wr_id = 1;
	wqe->next = NULL;
	wqe->opcode = IBV_WR_SEND;
	wqe->sg_list = sge;
	wqe->num_sge = 1;
	wqe->send_flags = IBV_SEND_SIGNALED;

	sge->addr = (uintptr_t)buffer;
	sge->length = bufferlen;
	sge->lkey = bufferMemoryRegion->lkey;

	return wqe;
}

ibv_send_wr* create_WRITE_WQE(void* buffer, size_t bufferlen, ibv_mr* bufferMemoryRegion, void* addr, uint32_t rkey)
{
	struct ibv_send_wr *wqe;
	struct ibv_sge *sge;

	wqe = (ibv_send_wr *)malloc(sizeof(ibv_send_wr));
	sge = (ibv_sge *)malloc(sizeof(ibv_sge));

	memset(wqe, 0, sizeof(ibv_send_wr));
	memset(sge, 0, sizeof(ibv_sge));

	wqe->wr_id = 1;
	wqe->next = NULL;
	wqe->opcode = IBV_WR_RDMA_WRITE;
	wqe->wr.rdma.remote_addr = (uint64_t)addr;
	wqe->wr.rdma.rkey = rkey;
	wqe->sg_list = sge;
	wqe->num_sge = 1;
	wqe->send_flags = IBV_SEND_SIGNALED;

	sge->addr = (uintptr_t)buffer;
	sge->length = bufferlen;
	sge->lkey = bufferMemoryRegion->lkey;

	return wqe;
}

ibv_recv_wr* create_RECEIVE_WQE(void* buffer, size_t bufferlen, ibv_mr* bufferMemoryRegion)
{
	struct ibv_recv_wr *wqe;
	struct ibv_sge *sge;

	wqe = (ibv_recv_wr *)malloc(sizeof(ibv_recv_wr));
	sge = (ibv_sge *)malloc(sizeof(ibv_sge));

	memset(wqe, 0, sizeof(ibv_recv_wr));
	memset(sge, 0, sizeof(ibv_sge));

	wqe->wr_id = 2;
	wqe->next = NULL;
	wqe->sg_list = sge;
	wqe->num_sge = 1;

	sge->addr = (uintptr_t)buffer;
	sge->length = bufferlen;
	sge->lkey = bufferMemoryRegion->lkey;

	return wqe;
}

/*
 * Post a Receive WQE
 */
int post_RECEIVE_WQE(ibv_recv_wr* ll_wqe)
{
	int ret;
	struct ibv_recv_wr *bad_wqe = NULL;

	ret = ibv_post_recv(g_CMId->qp, ll_wqe, &bad_wqe);
	if(ret != 0)
	{
		fprintf(stderr, "ERROR post_RECEIVE_WQE - Couldn't Post Receive WQE\n");
		return -1;
	}

	return 0;
}

ibv_mr *create_MEMORY_REGION(void* buffer, size_t bufferlen)
{
	ibv_mr* tmpmr = (ibv_mr*)malloc(sizeof(ibv_mr));
	int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	tmpmr = ibv_reg_mr(g_pd, buffer, bufferlen, mr_flags);
	if(!tmpmr)
	{
		fprintf(stderr, "ERROR - create_MEMORY_REGION: Couldn't Register memory region\n");
		return NULL;
	}

	fprintf(stdout, "Memory Region was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
			buffer, tmpmr->lkey, tmpmr->rkey, mr_flags);

	return tmpmr;
}

/*
 * Post a Send WQE
 */
int post_SEND_WQE(ibv_send_wr* ll_wqe)
{
	int ret;
	struct ibv_send_wr *bad_wqe = NULL;

	ret = ibv_post_send(g_CMId->qp, ll_wqe, &bad_wqe);
	if(ret != 0)
	{
		fprintf(stderr, "ERROR post_SEND_WQE - Couldn't Post Send WQE\n");
		return -1;
	}
	return 0;
}

void PrintCMEvent(struct rdma_cm_event *event)
{
	if(event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_ADDR_RESOLVED)\n");
	else if(event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_ADDR_RESOLVED)\n");
	else if(event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_ROUTE_RESOLVED)\n");
	else if(event->event == RDMA_CM_EVENT_ROUTE_ERROR)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_ROUTE_ERROR)\n");
	else if(event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_CONNECT_REQUEST)\n");
	else if(event->event == RDMA_CM_EVENT_CONNECT_RESPONSE)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_CONNECT_RESPONSE)\n");
	else if(event->event == RDMA_CM_EVENT_CONNECT_ERROR)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_CONNECT_ERROR)\n");
	else if(event->event == RDMA_CM_EVENT_UNREACHABLE)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_UNREACHABLE)\n");
	else if(event->event == RDMA_CM_EVENT_REJECTED)
	{
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_REJECTED)\n");
		fprintf(stderr, "Status(%u)", event->status);
	}
	else if(event->event == RDMA_CM_EVENT_ESTABLISHED)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_ESTABLISHED)\n");
	else if(event->event == RDMA_CM_EVENT_DISCONNECTED)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_DISCONNECTED)\n");
	else if(event->event == RDMA_CM_EVENT_DEVICE_REMOVAL)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_DEVICE_REMOVAL)\n");
	else if(event->event == RDMA_CM_EVENT_MULTICAST_JOIN)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_MULTICAST_JOIN)\n");
	else if(event->event == RDMA_CM_EVENT_MULTICAST_ERROR)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_MULTICAST_ERROR)\n");
	else if(event->event == RDMA_CM_EVENT_ADDR_CHANGE)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_ADDR_CHANGE)\n");
	else if(event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT)
		fprintf(stderr, "Received CM Event(RDMA_CM_EVENT_TIMEWAIT_EXIT)\n");
	return;
}

int GetCMEvent(rdma_cm_event_type* EventType)
{
	int ret;
	struct rdma_cm_event *CMEvent;

	ret = rdma_get_cm_event(g_CMEventChannel, & CMEvent);
	if(ret != 0)
	{
		fprintf(stderr, "No Event Received Time Out\n");
		return -1;
	}
	*EventType = CMEvent->event;
	PrintCMEvent(CMEvent);

	/*
	 * Release the Event now that we are done with it
	 */
	ret=rdma_ack_cm_event(CMEvent);
	if(ret != 0)
	{
		fprintf(stderr, "CM couldn't release CM Event\n");
		return -1;
	}

	return 0;

}

int RDMACreateChannel()
{
	int ret = 0;
	g_CMEventChannel = NULL;

	// Open a Channel to the Communication Manager used to receive async events from the CM.
	g_CMEventChannel = rdma_create_event_channel();
	if(!g_CMEventChannel)
	{
		fprintf(stderr, "Failed to Open CM Event Channel");
		CleanUpCMContext();
		return -1;
	}

	ret = rdma_create_id(g_CMEventChannel,&g_CMId, NULL, RDMA_PS_TCP);
	if(ret != 0)
	{
		fprintf(stderr, "Failed to Create CM ID");
		CleanUpCMContext();
		return -1;
	}

	return 0;
}

static int get_addr(char *dst, struct sockaddr *addr)
{
	struct addrinfo *res;
	int ret;
	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret)
	{
		fprintf(stderr, "getaddrinfo failed - invalid hostname or IP address\n");
		return -1;
	}
	memcpy(addr, res->ai_addr, res->ai_addrlen);
	freeaddrinfo(res);
	return ret;
}



int RDMAClientInit()
{
	int ret;
	rdma_cm_event_type et;

	if(get_addr(s_srcAddr,(struct sockaddr*)&g_srcAddr) != 0)
	{
		fprintf(stderr, "Failed to REsolve Local Address\n");
		CleanUpCMContext();
		return -1;
	}


	if(get_addr(s_dstAddr,(struct sockaddr*)&g_dstAddr) != 0)
	{
		fprintf(stderr, "Failed to Resolve Destination Address\n");
		CleanUpCMContext();
		return -1;
	}
	g_dstAddr.sin_port = htons(n_dstPort);

	fprintf(stderr, "port: %u\n", g_dstAddr.sin_port);
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(g_dstAddr.sin_addr), str, INET_ADDRSTRLEN);
	fprintf(stderr, "address: %s\n", str);

	/*
	 * Resolve the IP Addresses to GIDs.
	 */
	fprintf(stdout, "Resolving IP addresses to GIDS ...\n");
	ret = rdma_resolve_addr(g_CMId, (struct sockaddr*)&g_srcAddr, (struct sockaddr*)&g_dstAddr,2000);
	if(ret != 0)
	{
		fprintf(stderr, "CM couldn't resolve IP addresses to GIDS\n");
		return -1;
	}

	fprintf(stdout, "Waiting for CM to resolve IP Addresses ...\n");
	do
	{
		ret = GetCMEvent(&et);
		if(ret != 0)
		{
			fprintf(stderr, "ERROR Processing CM Events\n");
		}
	} while(et != RDMA_CM_EVENT_ADDR_RESOLVED);

	return 0;
}

int RDMACreateQP()
{
	int ret;
	struct ibv_qp_init_attr qp_init_attr;

	//Create a Protection Domain
	g_pd = ibv_alloc_pd(g_CMId->verbs);
	if(!g_pd)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Couldn't allocate protection domain\n");
		return -1;
	}

	/*Create a completion Queue */
	g_cq = ibv_create_cq(g_CMId->verbs, NUM_OPERATIONS, NULL, NULL, 0);
	if(!g_cq)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Couldn't create completion queue\n");
		return -1;
	}

	/* create the Queue Pair */
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));

	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.sq_sig_all = 0;
	qp_init_attr.send_cq = g_cq;
	qp_init_attr.recv_cq = g_cq;
	qp_init_attr.cap.max_send_wr = NUM_OPERATIONS;
	qp_init_attr.cap.max_recv_wr = NUM_OPERATIONS;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;


	ret = rdma_create_qp(g_CMId, g_pd, &qp_init_attr);
	if(ret != 0)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Couldn't Create Queue Pair Error(%s)\n", strerror(errno));
		return -1;
	}
	return 0;
}

void PrintConnectionInfo(rdma_conn_param cParam)
{
	fprintf(stderr, "QPN: %d\n", cParam.qp_num);
}

int RDMAClientConnect()
{
	int ret;
	rdma_cm_event_type et;

	//rdma resolve route
	ret = rdma_resolve_route(g_CMId, 2000);
	if(ret != 0)
	{
		fprintf(stderr, "ERROR - RDMAClientConnect: Couldn't resolve the Route\n");
		return -1;
	}

	fprintf(stdout, "Waiting for Resolve Route CM Event ...\n");
	do
	{
		ret = GetCMEvent(&et);
		if(ret != 0)
		{
			fprintf(stderr, "ERROR Processing CM Events\n");
		}
	} while(et != RDMA_CM_EVENT_ROUTE_RESOLVED);

	fprintf(stdout, "Waiting for Connection Established Event ...\n");

		struct rdma_conn_param ConnectionParams;

		memset(&ConnectionParams, 0, sizeof(ConnectionParams));
		ret = rdma_connect(g_CMId, &ConnectionParams);
		if(ret != 0)
		{
			fprintf(stderr, "Client Couldn't Establish Connection\n");
			return -1;
		}

		PrintConnectionInfo(ConnectionParams);

		do
			{
		ret = GetCMEvent(&et);
		if(ret != 0)
		{
			fprintf(stderr, "ERROR Processing CM Events\n");
		}
	} while(et != RDMA_CM_EVENT_ESTABLISHED);



	return 0;
}

int RDMAServerInit()
{
	int ret;

	if(get_addr(s_srcAddr,(struct sockaddr*)&g_srcAddr) != 0)
	{
		fprintf(stderr, "Failed to REsolve Local Address\n");
		CleanUpCMContext();
		return -1;
	}
	g_srcAddr.sin_port = htons(20079);

	ret = rdma_bind_addr(g_CMId, (struct sockaddr*)&g_srcAddr);
	if(ret != 0 )
	{
		fprintf(stderr, "ERROR RDMAServerInit: Couldn't bind to local address\n");
	}

	ret = rdma_listen(g_CMId, 10);

	uint16_t port = 0;
	port = ntohs(rdma_get_src_port(g_CMId));
	fprintf(stderr, "listening on port %d.\n", port);
	return 0;
}

int RDMAServerConnect()
{
	int ret;
	struct rdma_cm_event *CMEvent;
	rdma_cm_event_type et;

	/*
	 * Wait for the Connect REquest to Come From the Client
	 */
	do
	{
		ret = rdma_get_cm_event(g_CMEventChannel, & CMEvent);
		if(ret != 0)
		{
			fprintf(stderr, "No Event Received Time Out\n");
			return -1;
		}

		PrintCMEvent(CMEvent);
	} while(CMEvent->event != RDMA_CM_EVENT_CONNECT_REQUEST);

	/*
	 * Get the CM Id from the Event
	 */

	g_CMId = CMEvent->id;
	/*
	 * Now we can create the QP
	 */
	ret = RDMACreateQP();
	if(ret != 0)
	{
		fprintf(stderr, "ERROR RDMAServerConnect - Couldn't Create QP\n");
		return -1;
	}

	struct rdma_conn_param ConnectionParams;
	memset(&ConnectionParams, 0, sizeof(ConnectionParams));
	ret = rdma_accept(g_CMId, &ConnectionParams);
		if(ret != 0)
		{
			fprintf(stderr, "Client Couldn't Establish Connection\n");
			return -1;
		}

		PrintConnectionInfo(ConnectionParams);

	/*
	 * Release the Event now that we are done with it
	 */
	ret=rdma_ack_cm_event(CMEvent);
	if(ret != 0)
	{
		fprintf(stderr, "CM couldn't release CM Event\n");
		return -1;
	}

	fprintf(stdout, "Waiting for Connection Established Event ...\n");
		do
		{
			ret = GetCMEvent(&et);
			if(ret != 0)
			{
				fprintf(stderr, "ERROR Processing CM Events\n");
			}
		} while(et != RDMA_CM_EVENT_ESTABLISHED);

	return 0;
}

void CleanUpCMContext()
{
	if(g_CMEventChannel != NULL)
	{
		rdma_destroy_event_channel(g_CMEventChannel);
	}

	if(g_CMId != NULL)
	{
		if(rdma_destroy_id(g_CMId) != 0)
		{
			fprintf(stderr, "CleanUpCMContext: Failed to destroy Connection Manager Id\n");
		}
	}
}

void CleanUpQPContext()
{
	if(g_pd != NULL)
	{
		if(ibv_dealloc_pd(g_pd) != 0)
		{
			fprintf(stderr, "CleanUpQPContext: Failed to destroy Protection Domain\n");
		}
	}

	if(g_cq != NULL)
	{
		ibv_destroy_cq(g_cq);
		{
			fprintf(stderr, "CleanUpQPContext: Failed to destroy Completion Queue\n");
		}
	}

	rdma_destroy_qp(g_CMId);

}

