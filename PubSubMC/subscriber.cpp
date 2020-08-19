#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>

#include </usr/include/infiniband/verbs.h>
#include </usr/include/rdma/rdma_cma.h>

#include "instrument.h"

using namespace std;

void PrintUsage();
static int get_addr(char *dst, struct sockaddr *addr);

//Construction and Destruction
int RDMASubscriberInit();
int RDMACreateQP();
void CleanUpSubContext();

//CM Event Channel Handlers
int OnAddressResolved(struct rdma_cm_event *event);
int OnMulticastJoin(struct rdma_cm_event *event);

//QP Operations
ibv_recv_wr* create_RECEIVE_WQE(void* buffer, size_t bufferlen, ibv_mr* bufferMemoryRegion);
ibv_mr *create_MEMORY_REGION(void* buffer, size_t bufferlen);
int post_RECEIVE_WQE(ibv_recv_wr* ll_wqe);


// Completion Queue Event Handlers
void OnReceiveUpdate();

void *MonitorCMEventChannel(void* data);
void *SubscriptionMonitor(void* data);

typedef struct CQContext
{
	int id;
} CQContext_t;

typedef struct QPContext
{
	int id;
} QPContext_t;

typedef struct CMContext
{
	int id;
} CMContext_t;

struct SubContext {

	/*
	 * Variable Used for Address Resolution.
	 */
	char 						*mcastAddr_string;
	char 						*localAddr_string;
	struct sockaddr_in6			localAddr_in;
	struct sockaddr_in6			mcastAddr_in;
	struct sockaddr				*localAddr_resolved;
	struct sockaddr				*mcastAddr_resolved;

	//Contexts - Defined by the Local Applicaiton for tracking state (Can be NULL)
	CMContext_t 					*CMContext;
	QPContext_t						*QPContext;

	//Connection Manager Constructs
	struct 	rdma_event_channel		*CMEventChannel;
	struct 	rdma_cm_id				*CMId;

	//QP Context Structures
	struct 	ibv_cq                	*CompletionQueue;                            /* Completion Queue Handle */
	struct 	ibv_comp_channel		*CompletionChannel;							 /* Completion Channel */
	struct 	ibv_ah 					*AddressHandle;								 /* Address Handle */
	uint32_t 						RemoteQpn;
	uint32_t 						RemoteQkey;

	//Protection Domain
	struct 	ibv_pd              	*ProtectionDomain;                       	/* Protection Domain Handle */

	//Memory Regions
	struct 	ibv_mr                  *MemoryRegion;                            	/* Memory Region Handle */
	char				*mem;
	instrument_t			*ins_mlnx;
};
static struct SubContext g_SubContext;

/*
 * Signals
 */
static bool SIG_SUBSCRIPTIONESTABLISHED = false;
static bool SIG_KILLCMMONITOR = false;

int main(int argc,char *argv[], char *envp[])
{

	fprintf(stderr, "a");
	g_SubContext.mem = (char *)malloc(sizeof(struct ibv_grh) + sizeof(instrument_t));
	fprintf(stderr, "b");
	g_SubContext.ins_mlnx = (instrument_t *)&g_SubContext.mem[40];
	fprintf(stderr, "c");

	//Create the Instrument
	g_SubContext.ins_mlnx->Symbol[0] = 'M';
	g_SubContext.ins_mlnx->Symbol[1] = 'L';
	g_SubContext.ins_mlnx->Symbol[2] = 'N';
	g_SubContext.ins_mlnx->Symbol[3] = 'X';
	g_SubContext.ins_mlnx->Symbol[4] = '\0';
	g_SubContext.ins_mlnx->Value = 1.0;

	int op;
	while ((op = getopt(argc, argv, "l:m:")) != -1)
	{
		switch (op)
		{
		case 'l':
			g_SubContext.localAddr_string = optarg;
			break;
		case 'm':
			g_SubContext.mcastAddr_string = optarg;
			break;
		default:
			PrintUsage();
			return -1;
		}
	}

	g_SubContext.localAddr_resolved = (struct sockaddr *)&g_SubContext.localAddr_in;
	g_SubContext.mcastAddr_resolved = (struct sockaddr *)&g_SubContext.mcastAddr_in;


	fprintf(stdout, "********  ********  ********  ********\n");
	fprintf(stdout,"MARKET DATA SUBSCRIBER\n");
	fprintf(stderr, "Local IPoIB Address:      %s\n", g_SubContext.localAddr_string);
	fprintf(stderr, "Subscribing to Multicast Group:      %s\n", g_SubContext.mcastAddr_string);
	fprintf(stdout, "********  ********  ********  ********\n\n");

	if(RDMASubscriberInit() != 0)
	{
		fprintf(stderr, "Exiting - Failed Create the RDMA Channel.\n");
		return 0;
	}

	/*
	 * Start Monitoring the CM Event Channel
	 */
	int data = 5;
	pthread_t 				CMEventChannelMonitorThread;
	fprintf(stderr, "Starting CM Event Monitor ...\n");
	pthread_create(&CMEventChannelMonitorThread, NULL, MonitorCMEventChannel, (void*) &data);

	//Wait for the Signal that the Channel is Established
	while(!SIG_SUBSCRIPTIONESTABLISHED) { sleep(5); }

	//Register the Memory Region
	 g_SubContext.MemoryRegion = create_MEMORY_REGION(g_SubContext.mem, 
							  sizeof(struct ibv_grh) + sizeof(instrument_t));

	/*
	 * Start Monitoring the Completion Queue (CQ) for CQEs
	 */
	pthread_t 				CQMonitorThread;
	pthread_create(&CQMonitorThread, NULL, SubscriptionMonitor, (void *) &data);



	/*
	 * Monitor my memory ever second and print the value.
	 * The Publisher has a channel and the key to write to my memory. It will change without me doing anything.
	 */
	fprintf(stdout, "\n********  ********  ********  ********\n");
	fprintf(stdout,"Displaying MLNX Ticker Value Every Second\n");
	while(true)
	{
		fprintf(stdout, "MKT UPDATE (%s,%f)\n", g_SubContext.ins_mlnx->Symbol, g_SubContext.ins_mlnx->Value);
		sleep(1);
	}

	CleanUpSubContext();

	return 0;
}

void PrintUsage()
{
	printf("usage: sub [ -l ip ] [-m mcast_ip] \n");
	printf("\t[-l ip] - bind to the local interface associated with this IPoIB Address.\n");
	printf("\t[-m ip] - bind to the local interface associated with this IPoIB Address.\n");
}

void OnReceiveUpdate()
{
	fprintf(stdout, "Received an Update (%s,%f)\n", g_SubContext.ins_mlnx->Symbol, g_SubContext.ins_mlnx->Value);
	return;
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

/*
 * Monitors the Communication Managers Event Channel.
 *
 * The CM event channel will notify the program when joins or multicast error happen.
 */
void *MonitorCMEventChannel(void* data)
{
	struct rdma_cm_event *event;
	int ret = 0;

	do {
		ret = rdma_get_cm_event(g_SubContext.CMEventChannel, &event);
		if(ret != 0)
		{
			fprintf(stderr, "ERROR - MonitorCMEventChannel: Non-Zero Return Code from rdma_get_cm_event.\n");
		}

		switch(event->event)
		{
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			fprintf(stderr, "Received RDMA_CM_EVENT_ADDR_RESOLVED Event\n");
			OnAddressResolved(event);
			break;
		case RDMA_CM_EVENT_MULTICAST_JOIN:
			fprintf(stderr, "Received RDMA_CM_EVENT_MULTICAST_JOIN Event\n");
			OnMulticastJoin(event);
			break;

		//TODO: Error Cases
		case RDMA_CM_EVENT_ADDR_ERROR:
			fprintf(stdout, "Address Resolution Error: event: %s, error: %d\n", rdma_event_str(event->event), event->status);
			break;
		case RDMA_CM_EVENT_ROUTE_ERROR:
			fprintf(stdout, "Route Error: event: %s, error: %d\n", rdma_event_str(event->event), event->status);
			break;
		case RDMA_CM_EVENT_MULTICAST_ERROR:
			fprintf(stdout, "Multicast Error: event: %s, error: %d\n", rdma_event_str(event->event), event->status);
			break;
		default:
			break;

		}

		rdma_ack_cm_event(event);
	} while(!SIG_KILLCMMONITOR);

	return 0;
}

/*
 * Subscriber Initilization
 *
 * Create the CM Event Channel, the Connection Identifier, Bind the application to a local address
 */
int RDMASubscriberInit()
{
	int ret = 0;
	g_SubContext.CMEventChannel = NULL;
	g_SubContext.CMContext = (CMContext_t*) malloc(sizeof(CMContext_t));
	g_SubContext.CMContext->id = 1;

	// Open a Channel to the Communication Manager used to receive async events from the CM.
	g_SubContext.CMEventChannel = rdma_create_event_channel();
	if(!g_SubContext.CMEventChannel)
	{
		fprintf(stderr, "Failed to Open CM Event Channel");
		CleanUpSubContext();
		return -1;
	}

	ret = rdma_create_id(g_SubContext.CMEventChannel, &g_SubContext.CMId, g_SubContext.CMContext, RDMA_PS_UDP);
	if(ret != 0)
	{
		fprintf(stderr, "Failed to Create CM ID");
		CleanUpSubContext();
		return -1;
	}

	if(get_addr(g_SubContext.localAddr_string,(struct sockaddr*)&g_SubContext.localAddr_in) != 0)
	{
		fprintf(stderr, "Failed to Resolve Local Address\n");
		CleanUpSubContext();
		return -1;
	}

	if(get_addr(g_SubContext.mcastAddr_string,(struct sockaddr*)&g_SubContext.mcastAddr_in) != 0)
	{
		fprintf(stderr, "Failed to Resolve Multicast Address Address\n");
		CleanUpSubContext();
		return -1;
	}

	ret = rdma_bind_addr(g_SubContext.CMId, g_SubContext.localAddr_resolved);
	if(ret != 0 )
	{
		fprintf(stderr, "ERROR RDMAServerInit: Couldn't bind to local address.\n");
	}

	ret = rdma_resolve_addr(g_SubContext.CMId, (struct sockaddr*)&g_SubContext.localAddr_in, (struct sockaddr*)&g_SubContext.mcastAddr_in, 2000);
	if(ret != 0 )
	{
		fprintf(stderr, "ERROR RDMAServerInit: Couldn't resolve local address and or mcast address.\n");
	}

	return 0;
}

int OnAddressResolved(struct rdma_cm_event *event)
{
	int ret;

	/*
	 * Get the CM Id from the Event
	 */
	g_SubContext.CMId = event->id;

	/*
	 * Create the QP
	 */
	ret = RDMACreateQP();
	if(ret != 0)
	{
		fprintf(stderr, "ERROR OnAddressResolved - Couldn't Create QP\n");
		return -1;
	}

	ret = rdma_join_multicast(g_SubContext.CMId, g_SubContext.mcastAddr_resolved, NULL);
	if(ret != 0)
	{
		fprintf(stderr, "ERROR OnAddressResolved - RDMA MC Join Failed %d\n", ret);
		return -1;
	}

	return 0;
}

int OnMulticastJoin(struct rdma_cm_event *event)
{
	rdma_ud_param *param;
	param = &event->param.ud;

	char buf[40];

	inet_ntop(AF_INET6, param->ah_attr.grh.dgid.raw, buf, 40);

	g_SubContext.RemoteQpn = param->qp_num;
	g_SubContext.RemoteQkey = param->qkey;
	g_SubContext.AddressHandle = ibv_create_ah(g_SubContext.ProtectionDomain, &param->ah_attr);
	if (!g_SubContext.AddressHandle)
	{
		fprintf(stderr, "ERROR OnMulticastJoin - Failed to create the Address Handle\n");
		return -1;
	}

	fprintf(stderr, "Joined Multicast Group QPN(%d) QKey(%d)\n", g_SubContext.RemoteQpn, g_SubContext.RemoteQkey);
	SIG_SUBSCRIPTIONESTABLISHED = true;

	return 0;
}

int RDMACreateQP()
{
	int ret;
	struct ibv_qp_init_attr qp_init_attr;
	CQContext_t *cqcontext = (CQContext_t*)malloc(sizeof(CQContext_t));
	QPContext_t	*qpcontext = (QPContext_t*)malloc(sizeof(QPContext_t));

	cqcontext->id = 1;
	qpcontext->id = 1;

	//Create a Protection Domain
	g_SubContext.ProtectionDomain = ibv_alloc_pd(g_SubContext.CMId->verbs);
	if(!g_SubContext.ProtectionDomain)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Couldn't allocate protection domain\n");
		return -1;
	}

	/*
	 * Create a Completion Channel - Used to Handle CQE in a callback model.
	 */
	g_SubContext.CompletionChannel = ibv_create_comp_channel(g_SubContext.CMId->verbs);
	if(!g_SubContext.CompletionChannel)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Coun'dn't Create a Completion CHannel\n");
		return -1;
	}

	/*Create a completion Queue */
	g_SubContext.CompletionQueue = ibv_create_cq(g_SubContext.CMId->verbs, 10, &cqcontext, g_SubContext.CompletionChannel, 1);
	if(!g_SubContext.CompletionQueue)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Couldn't create completion queue\n");
		return -1;
	}

	/* create the Queue Pair */
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));

	qp_init_attr.qp_type = IBV_QPT_UD;
	qp_init_attr.sq_sig_all = 1;
	qp_init_attr.send_cq = g_SubContext.CompletionQueue;
	qp_init_attr.recv_cq = g_SubContext.CompletionQueue;
	qp_init_attr.cap.max_send_wr = 1;
	qp_init_attr.cap.max_recv_wr = 5;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.qp_context = &qpcontext;


	ret = rdma_create_qp(g_SubContext.CMId, g_SubContext.ProtectionDomain, &qp_init_attr);
	if(ret != 0)
	{
		fprintf(stderr, "ERROR - RDMACreateQP: Couldn't Create Queue Pair Error(%d)\n", ret);
		return -1;
	}
	return 0;
}

void *SubscriptionMonitor(void* data)
{
	ibv_recv_wr* receiveWQE;
	struct ibv_wc wc;
	struct ibv_cq *temp_cq;
	CQContext_t *temp_cqContext;
	int ret = 0;

	//Create a Receive Queue Element
	receiveWQE = create_RECEIVE_WQE(g_SubContext.mem,
					(sizeof(struct ibv_grh) + sizeof(instrument_t)),
					g_SubContext.MemoryRegion);
	do {
	/*
	 * Post a WQ Element to the Receive Queue for the next message that will come in
	 */

		 //Put it on the Receive Queue
		 ret = post_RECEIVE_WQE(receiveWQE);
		ret = post_RECEIVE_WQE(receiveWQE);
		 if(ret != 0)
		 {
			 fprintf(stderr, "ERROR Subscription Monitor - Failed to Post new WQE on Receive Queue.\n");
		 }

		 /*
		  * Wait for a new message from the publisher. We know we got a message becasue a completion will be generated.
		  */
		 //ARM the Completion Channel
		ibv_req_notify_cq(g_SubContext.CompletionQueue, 1);

		fprintf(stderr, "Waiting for a Completion to Be Generated... \n");

		// Get CQ Events Blocks Until we receive CQE's
		ret = ibv_get_cq_event(g_SubContext.CompletionChannel, &temp_cq, ((void **)&temp_cqContext));
		if(ret != 0)
		{
			fprintf(stderr, "ERROR - MonitorCQ: Blocking call to get CQ events failed\n");
			sleep(1); // kill time so it doesn't busy loop
		}
		fprintf(stderr, "done ibv_get_cq_event %d\n",ret);

		// We CAn now Process the events on the Queue
		ret = ibv_poll_cq(g_SubContext.CompletionQueue, 1, &wc);
		if(ret <= 0)
		{
			fprintf(stderr, "ERROR - MonitorCQ: Couldn't remove events from CQ\n");
		}
		fprintf(stderr, "Received %u Completions!\n", ret);
		fprintf(stderr, "Status (%s)\n", ibv_wc_status_str(wc.status));
		sleep(1);

		// Polled for the Events we can now Ack them.
		ibv_ack_cq_events(g_SubContext.CompletionQueue, ret);

		/*
		 * We can now process the message
		 */
		OnReceiveUpdate();

	} while(true);
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

	fprintf(stderr, "Posting a Receive WQE ...\n");
	ret = ibv_post_recv(g_SubContext.CMId->qp, ll_wqe, &bad_wqe);
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
	tmpmr = ibv_reg_mr(g_SubContext.ProtectionDomain, buffer, bufferlen, mr_flags);
	if(!tmpmr)
	{
		fprintf(stderr, "ERROR - create_MEMORY_REGION: Couldn't Register memory region\n");
		return NULL;
	}

	fprintf(stdout, "Memory Region was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
			buffer, tmpmr->lkey, tmpmr->rkey, mr_flags);
	return tmpmr;
}


void CleanUpSubContext()
{
	if(g_SubContext.CMEventChannel != NULL)
	{
		rdma_destroy_event_channel(g_SubContext.CMEventChannel);
	}

	if(g_SubContext.CMId != NULL)
	{
		if(rdma_destroy_id(g_SubContext.CMId) != 0)
		{
			fprintf(stderr, "CleanUpCMContext: Failed to destroy Connection Manager Id\n");
		}
	}
}
