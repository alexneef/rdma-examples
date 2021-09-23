#include <cstring>
#include <cstdlib>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>

#define ERROR_MESG_SIZE 100

extern int errno ;

/*
 * Returns Number of Completions Received
 */
bool PollCQ(struct ibv_cq *cq, char *errorMsg)
{
	struct ibv_wc wc;
	int ret = 0;

	do {
		ret = ibv_poll_cq(cq, 10, &wc);
	} while(ret == 0);

	if (ret < 0)
	{
	   snprintf(errorMsg, ERROR_MESG_SIZE, "ERROR %s, status %s\n", __func__, ibv_wc_status_str(wc.status));
	   return false; 
	}

	fprintf(stderr, "Received %u CQE Elements\n", ret);
	fprintf(stderr, "WRID(%llu)\tStatus(%u)\n", wc.wr_id, wc.status);
	return true;
}

ibv_mr *createMemoryRegion(struct ibv_pd* pd, void* buffer, size_t bufferlen)
{
	ibv_mr* tmpmr = (ibv_mr*)malloc(sizeof(ibv_mr));
	int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	tmpmr = ibv_reg_mr(pd, buffer, bufferlen, mr_flags);
	if(!tmpmr)
	{
		fprintf(stderr, "ERROR - createMemoryRegion: Couldn't Register memory region\n");
		return NULL;
	}

	fprintf(stdout, "Memory Region was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
			buffer, tmpmr->lkey, tmpmr->rkey, mr_flags);

	return tmpmr;
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

int GetCMEvent(rdma_event_channel *EC, rdma_cm_event_type* EventType)
{
	int ret;
	struct rdma_cm_event *CMEvent;

	ret = rdma_get_cm_event(EC, & CMEvent);
	if(ret != 0)
	{
        fprintf(stderr, "number of errno: %d\n", errno);
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

void print_ipv4(sockaddr_in *s)
{
    struct sockaddr_in *sin = (struct sockaddr_in *)s;
    char ip[INET_ADDRSTRLEN];
    uint16_t port;

    strcpy(ip, (char*)inet_ntoa((struct in_addr)sin->sin_addr));
    port = htons (sin->sin_port);

    printf ("host %s:%d\n", ip, port);
}

int get_addr(const char *dst, int port, struct sockaddr_in *addr)
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
	addr->sin_port = htons(port);
	freeaddrinfo(res);
	return ret;
}
