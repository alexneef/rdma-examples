#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <netinet/in.h>
#include <netdb.h>
#include <vector>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <algorithm>

#include "rdmautil.h"
#include "message.h"

extern int errno ;

#define ERROR_MESG_SIZE 100

#define LOCAL_IP_ADD    "10.10.10.122"
#define LISTEN_PORT     20080

#define QUOTE_COUNT     10              //Number of Quotes in teh library below
#define NUM_OPERATIONS  100000          //Number of Messages to Send

const char *quotes[150] = {
"Spread love everywhere you go. Let no one ever come to you without leaving happier. -Mother Teresa",
"When you reach the end of your rope, tie a knot in it and hang on. -Franklin D. Roosevelt",
"Always remember that you are absolutely unique. Just like everyone else. -Margaret Mead",
"Don't judge each day by the harvest you reap but by the seeds that you plant. -Robert Louis Stevenson",
"The future belongs to those who believe in the beauty of their dreams. -Eleanor Roosevelt",
"Tell me and I forget. Teach me and I remember. Involve me and I learn. -Benjamin Franklin",
"The best and most beautiful things in the world cannot be seen or even touched - they must be felt with the heart. -Helen Keller",
"It is during our darkest moments that we must focus to see the light. -Aristotle",
"Whoever is happy will make others happy too. -Anne Frank",
"Do not go where the path may lead, go instead where there is no path and leave a trail. -Ralph Waldo Emerson"
};

struct PubContext
{
   struct sockaddr_in6              local_in;
   struct sockaddr *                local_addr;
   struct rdma_event_channel *      CMEventChannel;
};
static struct PubContext g_PubContext;

typedef struct ConnectionContext
{
    struct sockaddr_in              pubAddr;
    struct sockaddr_in			    subAddr;

    rdma_event_channel *            CMEventChannel;

    struct rdma_cm_id *             pcmid;
    struct ibv_pd *                 pd;
    struct ibv_cq *                 cq;
    struct ibv_mr*                  mr;

    struct ibv_send_wr*             sendWQE;
} ConnectionContext_t ;

static std::vector<ConnectionContext_t*> vConnections;

static message_t m;

static bool appExit = false;


ibv_send_wr* create_Send_WQE(void* buffer, size_t bufferlen, ibv_mr* bufferMemoryRegion)
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


int processSub(ConnectionContext_t* c, const char* local_ipoib_address, int port)
{
    rdma_cm_event_type et;
    struct rdma_cm_event *CMEvent;

    do
    {
        if(0 != rdma_get_cm_event(c->CMEventChannel, & CMEvent))
        {
            fprintf(stderr, "ERROR(%s): No Event Received Time Out\n", strerror(errno));
            return -1;
        }

        PrintCMEvent(CMEvent);
    } while(CMEvent->event != RDMA_CM_EVENT_CONNECT_REQUEST);

    //Get the Connection id from the CMEvent
    c->pcmid = CMEvent->id;

    //Now that we know we can connect to the other node start creating the RDMA Queues we need
    struct ibv_qp_init_attr qp_init_attr;
    //Create a Protection Domain
    c->pd = ibv_alloc_pd(c->pcmid->verbs);
    if(!c->pd)
    {
        fprintf(stderr, "ERROR: Couldn't create protection domain\n");
        return -1;
    }

    // Create a completion Queue
    c->cq = ibv_create_cq(c->pcmid->verbs, NUM_OPERATIONS, NULL, NULL, 0);
    if(!c->cq)
    {
        fprintf(stderr, "ERROR: Couldn't create completion queue\n");
        return -1;
    }

    // Initialize the QP Attributes
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));

    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = c->cq;
    qp_init_attr.recv_cq = c->cq;
    qp_init_attr.cap.max_send_wr = 2;
    qp_init_attr.cap.max_recv_wr = 2;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    //Create the QP on the clientside
    if(0 != rdma_create_qp(c->pcmid, c->pd, &qp_init_attr))
    {
        fprintf(stderr, "ERROR(%s): Couldn't Create Queue Pair Error\n", strerror(errno));
        return -1;
    }

    //And finaly Accept the connection request

    struct rdma_conn_param ConnectionParams;
    memset(&ConnectionParams, 0, sizeof(ConnectionParams));
    if(0 != rdma_accept(c->pcmid, &ConnectionParams))
    {
        fprintf(stderr, "Client Couldn't Establish Connection\n");
        return -1;
    }

    fprintf(stdout, "New Subscriber Connected %u\n", c->pcmid->qp->qp_num);

    //Release the Event now that we are done with it
    if(0 != rdma_ack_cm_event(CMEvent))
    {
        fprintf(stderr, "CM couldn't release CM Event\n");
        return -1;
    }

    fprintf(stdout, "Waiting for Connection Established Event ...\n");
    do
    {
        if(0 != GetCMEvent(c->CMEventChannel, &et))
        {
            fprintf(stderr, "ERROR Processing CM Events\n");
        }
    } while(et != RDMA_CM_EVENT_ESTABLISHED);


    c->mr = createMemoryRegion(c->pd, &m, sizeof(message_t));


    c->sendWQE = create_Send_WQE(&m, sizeof(message_t),  c->mr);

    return 0;
}

void subscriptionHandler()
{
    ConnectionContext_t* c = new ConnectionContext();

    //Create the Communication Manager id, conceptualy similar to a socket
    if(0 != rdma_create_id(c->CMEventChannel, &c->pcmid, NULL, RDMA_PS_TCP))
    {
        fprintf(stderr, "Failed to Create CM ID");
        return;
    }

    //Resolve our Local IPoIB Address
    if(get_addr(LOCAL_IP_ADD, LISTEN_PORT, &c->pubAddr) != 0)
    {
        fprintf(stderr, "Failed to Resolve Local Address\n");
        return;
    }

    c->pubAddr.sin_port = htons(LISTEN_PORT);

    print_ipv4(&c->pubAddr);

    fprintf(stdout, "Binding and Listening on local address %s %u\n", LOCAL_IP_ADD, LISTEN_PORT);
    if(0 != rdma_bind_addr(c->pcmid, (struct sockaddr*)&c->pubAddr))
    {
        fprintf(stderr, "ERROR(%s): Couldn't bind to local address\n", strerror(errno));
        return;
    }

    if(0 != rdma_listen(c->pcmid, 10))
    {
        fprintf(stderr, "ERROR(%s): Failed to Start Listener\n", strerror(errno));
        return;
    }

    do {
        c = new ConnectionContext();
        c->CMEventChannel = g_PubContext.CMEventChannel;

        if (0 != processSub(c, LOCAL_IP_ADD, LISTEN_PORT)) {
            fprintf(stderr, "ERROR processing new subscriber");
        }
        vConnections.push_back(c);
    }while(appExit);
}

//-------------------------------------------------------------------
int Post_Send_WQE(ConnectionContext_t* c, ibv_send_wr* ll_wqe)
{
	struct ibv_send_wr *bad_wqe = NULL;

	if(0 != ibv_post_send(c->pcmid->qp, ll_wqe, &bad_wqe))
	{
        fprintf(stderr, "ERROR posting work request");
		return -1;
	}
	return 0;
}

int main(int argc,char *argv[], char *envp[])
{
    //Create on global event channel that we will assign and use for all our subscribers.
    g_PubContext.CMEventChannel = rdma_create_event_channel();
    if(!g_PubContext.CMEventChannel)
    {
        fprintf(stderr, "Failed to Open CM Event Channel");
        return -1;
    }

    //Start our Subscriber handling thread
    std::thread thread(subscriptionHandler);

    fprintf(stdout, "********  ********  ********  ********\n");
    fprintf(stdout,"Publisher \n");
    fprintf(stdout,"Uses an RC Channel to do Sends\n");
    fprintf(stdout,"Posts an update to all subscribers every 10 seconds\n");
    fprintf(stdout, "********  ********  ********  ********\n\n");

    /*
     * The Publisher will send an updated message every 10 seconds
     * Demonstrates Message Passing (SEND) Operation
     */
    struct ibv_wc wc;
    for(int i=0; i < NUM_OPERATIONS; i++) {
        //update the message
        m.number = i;
        strcpy(m.message, quotes[i % QUOTE_COUNT]);
        int numSubsInDispatch = vConnections.size();

        sleep(10);
        fprintf(stderr, "Sending Message Number (%i) to %i subscribers\n", i, numSubsInDispatch);

        for (int j = 0; j < numSubsInDispatch; j++) {
            //Post the WQEs
            Post_Send_WQE(vConnections[j], vConnections[j]->sendWQE);
        }

        for (int j = 0; j < numSubsInDispatch; j++) {
            fprintf(stderr, "Starting Completion Queue Monitor\n");
            int ret = 0;

            ret = ibv_poll_cq(vConnections[j]->cq, 1, &wc);
            if (ret > 0 && wc.status == IBV_WC_SUCCESS) {
                fprintf(stderr, "Received Completions for subscriber %u\n", vConnections[j]->pcmid->qp->qp_num);
                fprintf(stderr, "Received WC status: %i %i\n", wc.status, wc.vendor_err);
            } else {
                fprintf(stderr, "Completion Queue Error %i %i removing %u\n",
                        wc.status, wc.vendor_err, vConnections[j]->pcmid->qp->qp_num);

                //TODO: Clean up and destroy context.
                vConnections.erase(vConnections.begin() + j);
            }

        }

    }

    appExit = true;
    thread.join();
    return 0;
}
