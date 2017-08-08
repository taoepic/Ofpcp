/*
 * ofpcp
 *
 * copy files base on OFP/ODP/DPDK
 *
*/

#ifndef _OFPCP_H_
#define _OFPCP_H_

#include "ofp.h"
#include "ofpi_socketvar.h"
#include "ofpi_sockstate.h"
#include <pthread.h>
#include <semaphore.h>
#include <sched.h>

#define OFPCP_MUTEX

#ifdef OFPCP_MUTEX
typedef pthread_mutex_t	ofpcp_lock_t;
#define ofpcp_lock_init(a)	do { pthread_mutex_init(&(a),NULL); } while(0)
#define ofpcp_lock(a)		do { pthread_mutex_lock(&(a)); } while(0)
#define ofpcp_unlock(a)		do { pthread_mutex_unlock(&(a)); } while(0)
#else
typedef volatile char ofpcp_lock_t;
#define ofpcp_lock_init(a)	do { a = 0; } while(0)
#define ofpcp_lock(a)	\
		do { \
			while(!__sync_bool_compare_and_swap(&(a),0,1)) \
				sched_yield(); \
		} while(0)
#define ofpcp_unlock(a)		do { a = 0; ofpcp_mfence(); } while(0)
#endif

#define __OFPCP_LOCK_DO(l,s)	do { ofpcp_lock(l);(s);ofpcp_unlock(l); } while(0)

#ifdef OFPCP_NOCLR
#define _OFPCP_G(s)	(s)
#define _OFPCP_R(s)	(s)
#else
#define __CLR_(s)	"\033["#s"m"
#define __G_		__CLR_(32)
#define __R_		__CLR_(31)
#define __0_		__CLR_(0)
#define _OFPCP_G(s)	(__G_ s __0_)
#define _OFPCP_R(s)	(__R_ s __0_)
#endif

#define ofpcp_barrier()	__asm__ __volatile__("":::"memory")
#define ofpcp_mfence()	__asm__ __volatile__("mfence":::"memory")

#ifndef __PR
#define __PR(fmt,...) \
		do { \
			if (fast_frame_args.verbose) \
				printf("<%s:%d> "fmt"\n", \
					__FILE__, __LINE__, ##__VA_ARGS__); \
		} while(0)
#endif

struct ofpcp_request_slab {
	struct ofpcp_request *pool;
	int capacity;
	int valid_count;
	struct ofpcp_request *no_used;
	ofpcp_lock_t lock;
};

struct ofpcp_agent {
	ofpcp_lock_t req_lock;
	struct ofpcp_request *requests;
	struct ofpcp_request_slab request_slab;
};

#define __NEW_REQ(_handle,_req,_type) \
	struct ofpcp_request *_req = ofpcp_request_slab_alloc(&_handle->agent->request_slab); \
	assert(_req); req->type = _type

#define __LOCK_APPEND_REQ(_handle,_req) \
	_req->handle = _handle; \
	ofpcp_lock(_handle->agent->req_lock); \
	append_ofpcp_request(&_handle->agent->requests,_req); \
	ofpcp_unlock(_handle->agent->req_lock)

struct ofpcp_request {
	char *data;
	int size;
	struct ofpcp_handle *handle;
	int type;
#define OFPCP_REQUEST_TYPE_CONNECT		0x1
#define OFPCP_REQUEST_TYPE_CLOSE		0x2
#define OFPCP_REQUEST_TYPE_SEND			0x3
#define OFPCP_REQUEST_TYPE_SEND_SEMA		0x4
#define OFPCP_REQUEST_TYPE_SEMA			0x5
#define OFPCP_REQUEST_TYPE_RECV_ACK_SEMA	0x6
#define OFPCP_REQUEST_TYPE_QUIT			0x10
	unsigned long request_tick;
	struct ofpcp_request *next;
};

void append_ofpcp_request(struct ofpcp_request **head, struct ofpcp_request *req);
int remove_ofpcp_request(struct ofpcp_request **head, struct ofpcp_request *req);
int ofpcp_request_list_empty(struct ofpcp_request **head);
int ofpcp_request_list_count(struct ofpcp_request **head);
struct ofpcp_request* ofpcp_request_pop_head(struct ofpcp_request **head);

enum ofpcp_handle_prep_status {
	FILENAME_LEN_PREPARE = 0,
	FILENAME_PREPARE,
	FILESIZE_PREPARE,
	SPLITSIZE_PREPARE,
	FILEBODY_PREPARE
};

struct ofpcp_handle {
	int sockfd;
	struct ofpcp_agent *agent;
	char *filename;
	char *body;
	int filename_len;
	unsigned long file_size;
	int split_size;
	int localfd;
	enum ofpcp_handle_prep_status prep_status;
	int prep_recv_size;
	unsigned long file_received;
	unsigned long last_tick;
	struct timeval start_time;
	int total_file_count;
	sem_t sema;
	struct ofpcp_handle *next;
};

void destroy_ofpcp_handle(struct ofpcp_handle *);
struct ofpcp_handle *new_ofpcp_handle();

struct ofpcp_hash {
#define OFPCP_HASH_SIZE	200
	int capacity;
	struct ofpcp_handle **handles;
	int (*get_hash)(int fd);
};

struct ofpcp_handle *ofpcp_hash_find_sockfd(struct ofpcp_hash *hash, int sockfd);
void clear_ofpcp_handle(struct ofpcp_handle *);
int ofpcp_hash_remove_entry(struct ofpcp_hash *hash, struct ofpcp_handle *);
int ofpcp_hash_add_entry(struct ofpcp_hash *hash, struct ofpcp_handle *);
int ofpcp_hash_get_hash(int sockfd);
struct ofpcp_hash *init_ofpcp_hash();
void destroy_ofpcp_hash(struct ofpcp_hash *hash);

struct ofpcp_pool {
	ofpcp_lock_t locker;
	struct file_list *file_list;
};

struct file_list {
	int capacity;
	int count;
	char **filename;
};

typedef struct fast_frame_args {
	int verbose;
	int server_mode;
	int command_mode;
	int pool_count;
	int agent_count;
	int pseudo_mode;
	int notify_mode;
	int pingpong_count;
	int auto_create_path;
	int core_count;
	int if_count;		/* number of interfaces to be used */
	char **if_names;
	char *config_filename;
	int burst_mode;
#define IP_ADDRESS_SIZE	64
	char ip_address[IP_ADDRESS_SIZE];
	uint16_t ip_port;
	int stat_interval;
	unsigned int split_size;
	struct file_list file_list;
	struct ofpcp_pool pool;
	char *work_dir;
} fast_frame_args_t;


struct ofpcp_stat {
	struct timeval epoch;
	struct timeval tv0;
	unsigned long bs0;
	struct timeval tv1;
	unsigned long bs1;
};
	
extern ofp_init_global_t app_init_params;
extern struct fast_frame_args fast_frame_args;
extern odp_instance_t instance;
extern int worker_numbers;

int create_file_list(struct file_list*);
void destroy_file_list(struct file_list*);
int add_new_file(struct file_list*, char *filename);

int create_all_path(char *);
void print_usage(char *);
void parse_args(int, char*[], struct fast_frame_args*);
void start_process(int, void*(*thread)(void*), void* arg, int detached);
void* server_thread(void *);
void* client_thread(void *);
void* server_session_start(void *);
void* request_single_session_start(void *);
void* request_queue_session_start(void *);
void* pingpong_start(void *);

#endif

