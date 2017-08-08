/*
 * ofpcp
 *
 * copy files base on OFP/ODP/DPDK
 *
*/

#define _GNU_SOURCE
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <inttypes.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <sys/time.h>
#include <errno.h>
#include <signal.h>
#include <sched.h>
#include "ofpcp.h"

#define DEFAULT_RECEIVE_BUFFER_SIZE (unsigned long)(1 * 1024 * 1024)
#define DEFAULT_TRY_COUNT 20
#define DEFAULT_CONNECTING_WAIT_COUNT		30	
#define DEFAULT_CONNECTING_TRY_COUNT		8
#define DEFAULT_BUFFER_SIZE (4 * 1024)
#define DEFAULT_CHECK_CONNECT_INTERVAL		200000
#define ACK_CHAR 'y'
#define MAX_WORKERS 32
#define OFPCP_DEFAULT_IP_PORT 17322
#define DEFAULT_SPLIT_K	16
#define SEND_TIMEOUT_SEC	1
#define SEND_TIMEOUT_USEC	0
#define RECV_TIMEOUT_SEC	1
#define RECV_TIMEOUT_USEC	0

#define REQUEST_TIMEOUT		8

#define REQUEST_TYPE_NONE	0
#define REQUEST_TYPE_FILE	1
#define REQUEST_TYPE_FINISH	2
#define REQUEST_TYPE_PINGPONG	3

struct fast_frame_args fast_frame_args;
ofp_init_global_t app_init_params;
odp_instance_t instance;
int worker_numbers;
ofpcp_lock_t hashs_lock;
struct ofpcp_hash *hashs;
struct ofpcp_agent** agents;
struct ofpcp_stat stats;

void append_ofpcp_request(struct ofpcp_request **head, struct ofpcp_request *req) {
	struct ofpcp_request *iter;
	struct ofpcp_request **link;
	assert(head && req);
	assert(req->next == NULL);
	iter = *head;
	link = head;
	while (iter) {
		link = &(iter->next);
		iter = iter->next;
	}
	*link = req;
}

/* return 1: found and removed, return 0: not found */
int remove_ofpcp_request(struct ofpcp_request **head, struct ofpcp_request *req) {
	struct ofpcp_request *iter;
	struct ofpcp_request **link;
	if (!head || !req)
		return 0;
	iter = *head;
	link = head;
	while (iter) {
		if (iter == req) {
			*link = req->next;
			return 1;
		}
		link = &iter->next;
		iter = iter->next;
	}
	return 0;
}

int ofpcp_request_list_empty(struct ofpcp_request **head) {
	return (*head == NULL);
}

int ofpcp_request_list_count(struct ofpcp_request **head) {
	struct ofpcp_request *iter;
	int count = 0;
	iter = *head;
	while (iter) {
		count++;
		iter = iter->next;
	}
	return count;
}

struct ofpcp_request *ofpcp_request_pop_head(struct ofpcp_request **head) {
	struct ofpcp_request *first;

	assert(head);
	first = *head;
	if (first)
		*head = first->next;
	return first;
}

void ofpcp_request_slab_init(struct ofpcp_request_slab *slab, int size) {
	assert(slab && size > 0);
	slab->capacity = size;
	slab->pool = calloc(sizeof(struct ofpcp_request), slab->capacity);
	assert(slab->pool);
	slab->valid_count = slab->capacity;
	slab->no_used = NULL;
	ofpcp_lock_init(slab->lock);
	for (int i = 0; i < slab->capacity; i++)
		append_ofpcp_request(&slab->no_used, &slab->pool[i]);
}

void ofpcp_request_slab_destroy(struct ofpcp_request_slab *slab) {
	if (slab->pool)
		free(slab->pool);
}

struct ofpcp_request *ofpcp_request_slab_alloc(struct ofpcp_request_slab *slab) {
	struct ofpcp_request *req;
	if (!slab->valid_count)
		return NULL;
	ofpcp_lock(slab->lock);
	req = ofpcp_request_pop_head(&slab->no_used);
	assert(req);
	req->next = NULL;
	slab->valid_count--;
	ofpcp_unlock(slab->lock);
	return req;
}

void ofpcp_request_slab_free(struct ofpcp_request_slab *slab, struct ofpcp_request *req) {
	assert(req);
	ofpcp_lock(slab->lock);
	req->next = NULL;
	append_ofpcp_request(&slab->no_used, req);
	slab->valid_count++;
	ofpcp_unlock(slab->lock);
}

void destroy_ofpcp_handle(struct ofpcp_handle *handle) {
	if (handle == NULL)
		return;
	sem_destroy(&handle->sema);
	if (handle->filename)
		free(handle->filename);
	if (handle->body)
		free(handle->body);
	free(handle);
}

struct ofpcp_handle *new_ofpcp_handle() {
	struct ofpcp_handle *handle;

	handle = calloc(sizeof(struct ofpcp_handle), 1);
	assert(handle);
	handle->last_tick = time(NULL);
	sem_init(&handle->sema, 0, 0);
	return handle;
}

struct ofpcp_handle *ofpcp_hash_find_sockfd(struct ofpcp_hash *hash, int sockfd) {
	int index = (*hash->get_hash)(sockfd);
	struct ofpcp_handle *handle = hash->handles[index];
	while (handle) {
		if (handle->sockfd == sockfd)
			return handle;
		handle = handle->next;
	}
	return NULL;
}

/* return 1: found and removed, return 0: not found */
int ofpcp_hash_remove_entry(struct ofpcp_hash *hash, struct ofpcp_handle *h) {
	int index = (*hash->get_hash)(h->sockfd);
	struct ofpcp_handle *handle = hash->handles[index];
	struct ofpcp_handle **link = &hash->handles[index];
	assert(hash && h);
	while (handle) {
		if (handle == h) {
			*link = handle->next;
			return 1;
		}
		link = &handle->next;
		handle = handle->next;
	}
	return 0;
}

/* return 0: sucess, -1: indicates same sockfd has been in, -2: no memory */
int ofpcp_hash_add_entry(struct ofpcp_hash *hash, struct ofpcp_handle *h) {
	int index = (*hash->get_hash)(h->sockfd);
	struct ofpcp_handle *handle = hash->handles[index];
	struct ofpcp_handle **link = &hash->handles[index];
	while (handle) {
		if (handle == h) {
			return -1;
		}
		link = &handle->next;
		handle = handle->next;
	}
	*link = h;
	return 0;
}

int ofpcp_hash_get_hash(int sockfd) {
	sockfd = (sockfd < 0) ? -sockfd : sockfd;
	return sockfd % OFPCP_HASH_SIZE;
}

struct ofpcp_hash *init_ofpcp_hash() {
	struct ofpcp_hash *hash;

	hash = malloc(sizeof(struct ofpcp_hash));
	if (hash == NULL)
		return NULL;
	hash->capacity = OFPCP_HASH_SIZE;
	hash->handles = (struct ofpcp_handle **)calloc(
				sizeof(struct ofpcp_handle *), 
				hash->capacity);
	if (hash->handles == NULL) {
		free(hash);
		return NULL;
	}
	hash->get_hash = ofpcp_hash_get_hash;
	return hash;
}

void destroy_ofpcp_hash(struct ofpcp_hash *hash) {
	if (hash == NULL)
		return;
	free(hash->handles);
	free(hash);
}

void ofpcp_post(sem_t* wait) {
	sem_post(wait);
}

void ofpcp_wait(sem_t* wait) {
	while (1) {
		if (!sem_wait(wait))
			break;
		if (errno == EINTR)
			continue;
		else {
			printf("ofpcp_wait ERROR\n");
		}
	}
}

int create_all_path(char *filename) {
	int len = strlen(filename);
	char *dir = malloc(len + 1);
	if (!dir)
		return -1;
	strcpy(dir, filename);
	for (int i = 0; i < len; i++) {
		if (dir[i] != '/')
			continue;
		dir[i] = '\0';
		if (dir[0] && access(dir, F_OK)) {
			if (mkdir(dir, 0755) < 0 && errno != EEXIST) {
				perror("mkdir");
				OFP_ERR("MKDIR error: create path '%s' fail", dir);
				return -1;
			}
		}
		dir[i] = '/';
	}
	free(dir);
	return 0;
}

int splice_file_list(struct file_list *dest, struct file_list *src) {
	if (!src || !dest)
		return 0;
	for (int i = 0; i < src->count; i++) 
		if (add_new_file(dest, src->filename[i]) < 0)
			return -1;
	return 0;
}

int create_file_list(struct file_list* file_list) {
	file_list->capacity = 0;
	file_list->count = 0;
	file_list->filename = (char**) malloc(128 * sizeof(char*));
	if (file_list->filename == NULL)
		return -1;
	file_list->capacity = 128;
	return 0;
}

void destroy_file_list(struct file_list* file_list) {
	for (int i = 0; i < file_list->count; i++)
		free(file_list->filename[i]);
	free (file_list->filename);
	file_list->capacity = 0;
	file_list->count = 0;
	file_list->filename = NULL;
}

int expand_file_list(struct file_list *file_list) {
	char **ptr;
	int new_capacity = file_list->capacity * 2;

	ptr = (char**) malloc(new_capacity * sizeof(char*));
	if (ptr == NULL)
		return -1;
	memcpy(ptr, file_list->filename, file_list->count * sizeof(char*));
	free(file_list->filename);
	file_list->filename = ptr;
	file_list->capacity = new_capacity;
	return 0;
}

int add_new_file(struct file_list* file_list, char *filename) {
	char *ptr;

	if (!filename)
		return -1;
	if (file_list->count >= file_list->capacity) {
		if (expand_file_list(file_list) < 0)
			return -1;
	}
	ptr = malloc(strlen(filename) + 1);
	if (ptr == NULL)
		return -1;
	memcpy(ptr, filename, strlen(filename) + 1);
	file_list->filename[file_list->count++] = ptr;
	return 0;
}

int remove_filename(struct file_list *file_list, char **filename) {
	if (!file_list->count)
		return -1;

	*filename = malloc(strlen(file_list->filename[file_list->count - 1]) + 1);
	if (*filename == NULL)
		return -1;
	strcpy(*filename, file_list->filename[file_list->count - 1]);
	free(file_list->filename[file_list->count - 1]);
	file_list->count--;
	return 0;
}

int is_blank_string(const char *string) {
	if (!string)
		return 1;
	for (int i = 0; i < strlen(string); i++) 
		if (string[i] != ' ' && string[i] != '\t' && string[i] != '\n')
			return 0;
	return 1;
}

char *skip_space(char *string) {
	if (string == NULL)
		return string;

	while (*string == ' ' || *string == '\t' || *string == '\n')
		string++;
	return string;
}

int write_local_file(int fd, char *buffer, int size) {
	return (fast_frame_args.pseudo_mode) ?  size : write(fd, buffer, size);
}

int read_local_file(int fd, char *buffer, int size) {
	return (fast_frame_args.pseudo_mode) ?  size : read(fd, buffer, size);
}

int create_local_file(char *filename) {
	int file;

	if (fast_frame_args.auto_create_path)
		create_all_path(filename);

	if (fast_frame_args.pseudo_mode)
		return 0;

	file = open(filename, O_CREAT | O_RDWR | O_TRUNC, 0644);
	if (file < 0) {
		OFP_ERR("Error: create '%s' fail", filename);
		return -1;
	}
	OFP_INFO("create file '%s' success", filename);
	return file;
}

int close_local_file(int fd) {
	return (fast_frame_args.pseudo_mode) ? 0 : close(fd);
}


static enum ofp_return_code fastpath_local_hook(odp_packet_t pkt, void *arg) {
	int protocol = *(int*)arg;
	(void)protocol;
	(void)pkt;
	(void)arg;
	return OFP_PKT_CONTINUE;
}

void print_usage(char *appname) {
	printf("Usage: %s [OPTIONS]\n", appname);
	printf("\t-i, --interface eth interfaces(comma separated, no spaces)\n");
	printf("\t-S, --pseudo mode, skip read/write the files\n");
	printf("\t-k, --pingpong <count>, client send pingpong for test latency\n");
	printf("\t-c, --count <number> working core count.\n");
	printf("\t-l, --listen <ipaddress> ip address to listen, server mode\n");
	printf("\t-s, --server <ipaddress> ip address to connect, client mode\n");
	printf("\t-p, --port <port> port for server bind or client to connect. default %d\n", 
					OFPCP_DEFAULT_IP_PORT);
	printf("\t-C, --command make client side into command mode\n");
	printf("\t-B, --burst make sending file in concurrent multi thread mode\n");
	printf("\t-P, --pool <workcount> pre-connecting pool mode\n");
	printf("\t-f, --config <filename> config filename, default ofp.conf\n");
	printf("\t-w, --workdir <path> change to workdir, only for server\n");
	printf("\t-b, --splitsize <size_K> unit: KB, 0 = no_split, default %d\n", 
					DEFAULT_SPLIT_K);
	printf("\t-d, --data <filename>, the filename for sending\n");
	printf("\t-a, --auto_create_path, auto create the destination directories\n");
	printf("\t-N, --notify mode\n");
	printf("\t-A, --agent <agent count>, agent threads help to provide network service\n");
	printf("\t-I, --interval <stat_interval_secs>, start server statistics interval\n");
	printf("\t-h, --help display this help\n"); 
	printf("\t-v, --verbose verbose information\n");
	return;
}

void parse_args(int argc, char *argv[], struct fast_frame_args* ffargs) {
	int opt;
	int long_index;
	char *names, *str, *token, *save;
	size_t len;
	int i;

	static struct option longopts[] = {
		{"interface", required_argument, NULL, 'i'},
		{"count", required_argument, NULL, 'c'},
		{"listen", required_argument, NULL, 'l'},
		{"server", required_argument, NULL, 's'},
		{"port", required_argument, NULL, 'p'},
		{"config", required_argument, NULL, 'f'},
		{"data", required_argument, NULL, 'd'},
		{"workdir", required_argument, NULL, 'w'},
		{"splitsize", required_argument, NULL, 'b'},
		{"auto_create_path", no_argument, NULL, 'a'},
		{"command", no_argument, NULL, 'C'},
		{"burst", no_argument, NULL, 'B'},
		{"pool", required_argument, NULL, 'P'},
		{"pseudo", no_argument, NULL, 'S'},
		{"pingpong", required_argument, NULL, 'k'},
		{"notify", no_argument, NULL, 'N'},
		{"agent", required_argument, NULL, 'A'},
		{"interval", required_argument, NULL, 'I'},
		{"help", no_argument, NULL, 'h'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	memset(ffargs, 0, sizeof(struct fast_frame_args));
	ffargs->split_size = DEFAULT_SPLIT_K * 1024;
	ffargs->auto_create_path = 1;
	create_file_list(&ffargs->file_list);

	while (1) {
		opt = getopt_long(argc, argv, "+i:c:ls:p:f:d:w:b:P:k:A:I:CBSNahv",
				longopts, &long_index);
		if (opt == -1)
			break;	
		switch (opt) {
		case 'c':
			ffargs->core_count = atoi(optarg);
			printf("set core_count: %d\n", ffargs->core_count);
			break;

		case 'i':
			len = strlen(optarg) + 1;
			if (len == 1) {
				print_usage(argv[0]);
				exit(EXIT_FAILURE);
			}
			names = malloc(len);
			assert(names != NULL);
			strcpy(names, optarg);
			for (str = names, i = 0;; str = NULL, i++) {
				token = strtok_r(str, ",", &save);
				if (token == NULL)
					break;
			}
			ffargs->if_count = i;
			if (!ffargs->if_count) {
				printf("arg error: no interface\n");
				exit(EXIT_FAILURE);
			}
			printf("interface count: %d\n", ffargs->if_count);

			ffargs->if_names = calloc(ffargs->if_count, sizeof(char*));
			assert(ffargs->if_names != NULL);

			strcpy(names, optarg);
			for (str = names, i = 0;; str = NULL, i++) {
				token = strtok_r(str, ",", &save);
				if (token == NULL)
					break;
				ffargs->if_names[i] = token;
				printf("interface: %s\n", ffargs->if_names[i]);
			}
			break;

		case 'f':
			len = strlen(optarg) + 1;
			if (len == 1) {
				printf("arg error: no config file\n");
				exit(EXIT_FAILURE);
			}
			ffargs->config_filename = malloc(len);
			assert(ffargs->config_filename != NULL);
			strcpy(ffargs->config_filename, optarg);
			break;

		case 'l':
			ffargs->server_mode = 1;
			if (optind >= argc || argv[optind][0] == '-') {
				/* no listen address */
				strcpy(ffargs->ip_address, "");
				printf("listen address: ANY\n");
			} else {
				strcpy(ffargs->ip_address, argv[optind]);
				printf("listen address: %s\n", ffargs->ip_address);
				optind++;
			}
			break;

		case 'b':
			ffargs->split_size = atoi(optarg) * 1024;
			if (ffargs->split_size > 32 * 1024 * 1024) {
				printf("arg error: split size should small than 32M");
				exit(EXIT_FAILURE);
			}
			break;

		case 'C':
			ffargs->command_mode = 1;
			break;

		case 'B':
			ffargs->burst_mode = 1;
			break;

		case 'S':
			ffargs->pseudo_mode = 1;
			printf("PSEUDO mode\n");
			break;

		case 'N':
			ffargs->notify_mode = 1;
			printf("NOTIFY mode\n");
			break;

		case 'P':
			if (!strlen(optarg)) {
				printf("arg error: no pool count\n");
				exit(EXIT_FAILURE);
			}
			ffargs->pool_count = atoi(optarg);
			break;

		case 'I':
			if (!strlen(optarg)) {
				printf("arg error: no statistic interval\n");
				exit(EXIT_FAILURE);
			}
			ffargs->stat_interval = atoi(optarg);
			break;

		case 'k':
			if (!strlen(optarg)) {
				printf("arg error: no pingpong count\n");
				exit(EXIT_FAILURE);
			}
			ffargs->pingpong_count = atoi(optarg);
			break;

		case 'A':
			if (!strlen(optarg)) {
				printf("arg error: no agent count\n");
				exit(EXIT_FAILURE);
			}
			ffargs->agent_count = atoi(optarg);
			break;

		case 's':
			if (!strlen(optarg)) {
				printf("arg error: no connect address\n");
				exit(EXIT_FAILURE);
			}
			ffargs->server_mode = 0;
			strcpy(ffargs->ip_address, optarg);
			printf("server address: %s\n", ffargs->ip_address);
			break;

		case 'p':
			if (!strlen(optarg)) {
				printf("arg error: no port number\n");
				exit(EXIT_FAILURE);
			}
			ffargs->ip_port = atoi(optarg);
			break;

		case 'd':
			if (!strlen(optarg)) {
				printf("arg error: no data filename\n");
				exit(EXIT_FAILURE);
			}
			add_new_file(&ffargs->file_list, optarg);
			printf("file: %s\n", optarg);
			while (optind < argc && argv[optind][0] != '-') {
				add_new_file(&ffargs->file_list, argv[optind]);
				printf("file: %s\n", argv[optind]);
				optind++;
			}
			break;

		case 'w':
			ffargs->work_dir = malloc(strlen(optarg) + 1);
			strcpy(ffargs->work_dir, optarg);
			printf("set working dir '%s'\n", optarg);
			break;

		case 'a':
			ffargs->auto_create_path = 1;
			break;

		case 'v':
			ffargs->verbose = 1;
			break;

		case 'h':
			print_usage(argv[0]);
			break;

		default:
			printf("arg error: unknown argument '%c'\n", opt);
			break;
		}
	}

	if (ffargs->if_count == 0) {
		printf("error: interface count is 0\n");
		exit(EXIT_FAILURE);
	}
}

#if 0
static void signal_process(int signum) {
	printf("Receive signal %d, start exiting.\n", signum);
	ofp_stop_processing();
}

static int linux_sigaction(int signum, void (*sig_func)(int)) {
	sigset_t sigmask_all;
	struct sigaction sigact = {
		.sa_handler = sig_func,
		.sa_mask = {{0}},
		.sa_flags = 0,
	};

	if (sigfillset(&sigmask_all)) {
		printf("Error: sigfillset fail.\n");
		return -1;
	}
	sigact.sa_mask = sigmask_all;
	if (sigaction(signum, &sigact, NULL)) {
		printf("Error: sigaction fail.\n");
		return -1;
	}
	return 0;
}

static int set_interrupt_signal(void (*sig_func)(int)) {
	if (linux_sigaction(SIGINT, sig_func)) {
		printf("Error: set SIGINT fail\n");
		return -1;
	}
	if (linux_sigaction(SIGQUIT, sig_func)) {
		printf("Error: set SIGQUIT fail\n");
		return -1;
	}
	if (linux_sigaction(SIGTERM, sig_func)) {
		printf("Error: set SIGTERM fail\n");
		return -1;
	}
	return 0;
}
#endif

int main(int argc, char *argv[]) {
	odph_linux_pthread_t thread_tbl[MAX_WORKERS];
	int core_count;
	odp_cpumask_t cpumask;
	char cpumaskstr[128];
	odph_linux_thr_params_t thr_params;
	int ret;
	int err = EXIT_SUCCESS;

	if (argc == 1) {
		print_usage(argv[0]);
		err = EXIT_FAILURE;
		goto leave_main;
	}

	srandom(1);
	parse_args(argc, argv, &fast_frame_args);

	if (fast_frame_args.ip_port == 0)
		fast_frame_args.ip_port = OFPCP_DEFAULT_IP_PORT;

	if (!fast_frame_args.server_mode && !fast_frame_args.ip_address[0]) {
		printf("Error: no specific server address in client mode\n");
		err = EXIT_FAILURE;
		goto leave_main;
	}

	if (odp_init_global(&instance, NULL, NULL)) {
		printf("Error: odp_init_global fail\n");
		err = EXIT_FAILURE;
		goto leave_main;
	}
	if (odp_init_local(instance, ODP_THREAD_CONTROL)) {
		printf("Error: odp_init_local fail\n");
		err = EXIT_FAILURE;
		goto leave_odp_global;
	}

	ofpcp_lock_init(hashs_lock);
	hashs = init_ofpcp_hash();

	if (fast_frame_args.agent_count) {
		agents = malloc(sizeof(struct ofpcp_agent *) * fast_frame_args.agent_count);
		assert(agents);
	}

	core_count = odp_cpu_count();
	worker_numbers = core_count;

	if (fast_frame_args.core_count)
		worker_numbers = fast_frame_args.core_count;
	if (worker_numbers > MAX_WORKERS)
		worker_numbers = MAX_WORKERS;

	memset(&app_init_params, 0, sizeof(app_init_params));
	app_init_params.linux_core_id = 0;

	if (core_count > 1)
		worker_numbers--;

	worker_numbers = odp_cpumask_default_worker(&cpumask, worker_numbers);
	if (odp_cpumask_to_str(&cpumask, cpumaskstr, sizeof(cpumaskstr)) < 0) {
		printf("Error: too small buffer for odp_cpumask_to_str\n");
		err = EXIT_FAILURE;
		goto leave_odp_local;
	}

	printf("num worker threads: %i\n", worker_numbers);
	printf("first CPU: %i\n", odp_cpumask_first(&cpumask));
	printf("cpu mask: %s\n", cpumaskstr);

	app_init_params.if_count = fast_frame_args.if_count;
	app_init_params.if_names = fast_frame_args.if_names;
	app_init_params.pkt_hook[OFP_HOOK_LOCAL] = fastpath_local_hook;

	if (ofp_init_global(instance, &app_init_params)) {
		printf("Error: ofp_init_global fail\n");
		err = EXIT_FAILURE;
		goto leave_odp_local;
	}

	if (ofp_init_local()) {
		printf("Error: OFP local init failed.\n");
		err = EXIT_FAILURE;
		goto leave_ofp_global;
	}

	/* set signal for interrupt */
	//set_interrupt_signal(signal_process);

	/* start dataplane dispatcher worker threads */
	memset(thread_tbl, 0, sizeof(thread_tbl));
	thr_params.start = default_event_dispatcher;
	thr_params.arg = ofp_eth_vlan_processing;
	thr_params.thr_type = ODP_THREAD_WORKER;
	thr_params.instance = instance;
	ret = odph_linux_pthread_create(thread_tbl, &cpumask, &thr_params);

	if (ret != worker_numbers) {
		OFP_ERR("Error: fail create work threads, expect %d, got %d",
				worker_numbers, ret);
		ofp_stop_processing();
		odph_linux_pthread_join(thread_tbl, worker_numbers);
		err = EXIT_FAILURE;
		goto leave_ofp_local;
	}

	/* start CLI */
	if (ofp_start_cli_thread(instance, app_init_params.linux_core_id, 
			fast_frame_args.config_filename ? 
			fast_frame_args.config_filename : "ofp.conf") < 0) {
		OFP_ERR("Error: fail to init CLI thread");
		ofp_stop_processing();
		odph_linux_pthread_join(thread_tbl, worker_numbers);
		err = EXIT_FAILURE;
		goto leave_ofp_local;
	}

	sleep(2);
	ofp_loglevel = OFP_LOG_INFO;

	if (fast_frame_args.server_mode)
		start_process(app_init_params.linux_core_id + 1, server_thread, NULL, 0);
	else
		start_process(app_init_params.linux_core_id + 1, client_thread, NULL, 0);

	ofp_stop_processing();
	odph_linux_pthread_join(thread_tbl, worker_numbers);
	goto leave_main;

leave_ofp_local:
	if (ofp_term_local() < 0) 
		printf("Error: ofp_term_local fail\n");
leave_ofp_global:
	if (ofp_term_global() < 0) 
		printf("Error: ofp_term_global fail\n");
leave_odp_local:
	if (odp_term_local() < 0)
		printf("Error: odp_term_local fail\n");
leave_odp_global:
	if (odp_term_global(instance) < 0) 
		printf("Error: odp_term_global fail\n");
leave_main:
	printf("main exit\n");
	return err;
}

static int get_one_unworks_cpu() {
	static int current_cpu_itr = 1;

	++current_cpu_itr;
	if (current_cpu_itr >= odp_cpu_count() - worker_numbers)
		current_cpu_itr = 1;
	return current_cpu_itr;
}

void start_process(int core_id, void*(*process)(void*), void*arg, int detached) {
	odph_linux_pthread_t ofpcp_pthread;
	odp_cpumask_t cpumask;
	odph_linux_thr_params_t thr_params;

	odp_cpumask_zero(&cpumask);
	odp_cpumask_set(&cpumask, core_id);

	thr_params.arg = arg;
	thr_params.thr_type = ODP_THREAD_CONTROL;
	thr_params.instance = instance;
	thr_params.start = process;
	odph_linux_pthread_create(&ofpcp_pthread, &cpumask, &thr_params);
		
	if (detached) {
		pthread_attr_destroy(&ofpcp_pthread.attr);
		if (pthread_detach(ofpcp_pthread.thread)) 
			OFP_ERR("failed to detach thread");
	} else
		odph_linux_pthread_join(&ofpcp_pthread, 1);

}

int set_send_timeout(int sock, int sec, int usec) {
	int ret;
	struct ofp_timeval tv;
	tv.tv_sec = sec;
	tv.tv_usec = 0;
	ret = ofp_setsockopt(sock, OFP_SOL_SOCKET, OFP_SO_SNDTIMEO,
			&tv, sizeof(tv));
	if (ret) {
		OFP_ERR("Err: ofp_setsockopt(OFP_SO_SNDTIMEO)");
		return -1;
	}
	return 0;
}

int set_recv_timeout(int sock, int sec, int usec) {
	int ret;
	struct ofp_timeval tv;
	tv.tv_sec = sec;
	tv.tv_usec = 0;
	ret = ofp_setsockopt(sock, OFP_SOL_SOCKET, OFP_SO_RCVTIMEO,
			&tv, sizeof(tv));
	if (ret) {
		OFP_ERR("Err: ofp_setsockopt(OFP_SO_RCVRIMEO)");
		return -1;
	}
	return 0;
}

int send_pack_size(int sock, char *buf, int size) {
	int ret;
	int try_count = DEFAULT_TRY_COUNT;

	while (size > 0) {
		__PR(">> send_pack_size: sock %d, buf %p, size %d",
				sock, buf, size);
		ret = ofp_send(sock, buf, size, 0);
		__PR(">> send_pack_size: sock %d, buf %p, size %d, ret --- %d",
				sock, buf, size, ret);
		if (ret == 0) {
			OFP_ERR("ofp_send = 0");
			return -2;
		}
		if (ret < 0) {
			OFP_ERR("Error: send pack size = %d, errno = %s",
				size,
				ofp_strerror(ofp_errno));
			if (ofp_errno == OFP_EWOULDBLOCK || ofp_errno == OFP_EINTR) {
				usleep(100);
				continue;
			}
			try_count--;
			if (!try_count)
				return -1;
			OFP_INFO("retry count %d send(size %d)", 
				DEFAULT_TRY_COUNT - try_count, size);
			usleep(800000);
			continue;
		}
		if (ret > size) {
			printf("Err: ret(=%d) > size(=%d)\n", ret, size);
			assert(0);
		}
		buf += ret;
		size -= ret;
	}
	return size;
}

int recv_pack_size(int sock, char *buf, int size) {
	int ret;
	int try_count = DEFAULT_TRY_COUNT;

	while (size > 0) {
		ret = ofp_recv(sock, buf, size, 0);
		if (ret == 0) {
			OFP_INFO("ofp_recv = 0");
			return -2;
		}
		if (ret < 0) {
			if (ofp_errno == OFP_EWOULDBLOCK) {
				usleep(100);
				continue;
			}
			try_count--;
			if (!try_count)
				return -1;
			OFP_INFO("retry count %d recv(size %d)", 
				DEFAULT_TRY_COUNT - try_count, size);
			usleep(800000);
			continue;
		}
		buf += ret;
		size -= ret;
		__sync_add_and_fetch(&stats.bs1, ret);
	}
	return size;
}

int single_pingpong_request(int sock, int number) {
	int ret;
	char ack;
	int pingpong_mark = -2;
	ret = send_pack_size(sock, (char*)&pingpong_mark, sizeof(int));
	if (ret < 0) {
		OFP_INFO("Error: send pingpong REQUEST %d fail", number);
		return -1;
	} 

	ret = recv_pack_size(sock, &ack, 1);
	if (ret < 0) {
		OFP_INFO("Error: recv pingpong ACK %d fail", number);
		return -2;
	}

	return 0;
}

void pingpong_response(int sock) {
	char ack = 'S';
	if (send_pack_size(sock, &ack, 1) < 0) 
		OFP_INFO("Error: send pingpong ACK fail");
}
 
static int send_finish_mark(int sock) {
	int ret;
	int finish_mark = -1;
	ret = send_pack_size(sock, (char*)&finish_mark, sizeof(int));
	if (ret < 0)
		return -1;
	return 0;
}

static int send_filename_and_size(int sock, char *filename, unsigned long size,
			unsigned int split_size) {
	int ret;
	int filename_len = strlen(filename);
	unsigned long filesize = size;

	ret = send_pack_size(sock, (char*)&filename_len, sizeof(int));
	if (ret < 0) 
		return -1;

	ret = send_pack_size(sock, filename, filename_len);
	if (ret < 0) 
		return -1;

	ret = send_pack_size(sock, (char*)&filesize, sizeof(unsigned long));
	if (ret < 0)
		return -1;

	ret = send_pack_size(sock, (char*)&split_size, sizeof(unsigned int));
	if (ret < 0)
		return -1;

	return 0;
}

static char* receive_filename_and_size(int sock, unsigned long *size, 
			unsigned int *split_size, int* request_type) {
	char *filename;
	int filename_len;

	if (recv_pack_size(sock, (char*)&filename_len, sizeof(int)) < 0) {
		*request_type = REQUEST_TYPE_NONE;
		return NULL;
	}
	*request_type = (filename_len >= 0) ? REQUEST_TYPE_FILE :
			(filename_len == -1) ? REQUEST_TYPE_FINISH :
			(filename_len == -2) ? REQUEST_TYPE_PINGPONG : REQUEST_TYPE_NONE;
	if (*request_type == REQUEST_TYPE_FILE) {
		filename = calloc(filename_len + 1, 1);
		if (filename == NULL)
			return NULL;
		if (recv_pack_size(sock, filename, filename_len) < 0) {
			free(filename);
			return NULL;
		}

		if (recv_pack_size(sock, (char*)size, sizeof(unsigned long)) < 0) {
			free(filename);
			return NULL;
		}

		if (recv_pack_size(sock, (char*)split_size, sizeof(unsigned int)) < 0) {
			free(filename);
			return NULL;
		}
		return filename;
	} else
		return NULL;
}

static int send_filebody(int sock, char *filename, int fd, unsigned long size, unsigned int splitsize) {
	int ret;
	char ack;
	char *send_buffer;
	unsigned int buffer_size = (splitsize == 0) ? DEFAULT_BUFFER_SIZE : splitsize;
	int err = 0;
	int want;

	send_buffer = malloc(buffer_size);
	if (send_buffer == NULL) {
		OFP_INFO("Error: send buffer malloc fail");
		return -1;
	}

	OFP_INFO("start sending %s, %ld size, split %d", filename, size, splitsize);
	while (size > 0) {
		want = (size > buffer_size) ? buffer_size : size;
		ret = read_local_file(fd, send_buffer, 
				(size > buffer_size) ? buffer_size : size);
		if (ret < 0) {
			OFP_INFO("Error: read file fail");
			err = -1;
			goto leave;
		} else if (ret == 0) {
			OFP_INFO("Error: read file end");
			err = -1;
			goto leave;
		}
		if (send_pack_size(sock, send_buffer, want) < 0) {
			OFP_INFO("Error: send file body fail");
			err = -1;
			goto leave;
		}
		size -= want;

		if (splitsize) {
			if (recv_pack_size(sock, &ack, 1) < 0) {
				OFP_INFO("Error: recv ACK fail");
				err = -1;
				goto leave;
			}
			if (ack != ACK_CHAR) {
				OFP_ERR("Error: ack");
				err = -1;
				goto leave;
			}
		}
	}

leave:
	free(send_buffer);
	return err;
}

static int receive_filebody(int sock, int fd, unsigned long size, unsigned int splitsize) {
	char *recv_buffer;
	char ack = ACK_CHAR;
	int want;
	unsigned long total_recv = 0;
	unsigned int buffer_size = (splitsize == 0) ? DEFAULT_BUFFER_SIZE : splitsize;
	int err = 0;

	recv_buffer = malloc(buffer_size);
	if (recv_buffer == NULL) {
		OFP_INFO("Error: recv buffer malloc fail");
		return -1;
	}

	OFP_INFO("start receiving %ld bytes", size);
	while (size > 0) {
		want = (size > buffer_size) ? buffer_size : size;
		if (recv_pack_size(sock, recv_buffer, want) < 0) {
			OFP_INFO("Error: receive body fail");
			err = -1;
			goto leave;
		}
		total_recv += want;
		if (write_local_file(fd, recv_buffer, want) <= 0) {
			OFP_INFO("Error: write file error");
		} 
		size -= want;
		if (splitsize) {
			if (send_pack_size(sock, &ack, 1) < 0) {
				OFP_INFO("Error: send ACK fail");
				err = -1;
				goto leave;
			}
		} 
	}

leave:
	free(recv_buffer);
	return err;
}

int connect_server(const char* server_address, int server_port) {
	struct ofp_sockaddr_in server_addrin;
	int try_connect_count = DEFAULT_CONNECTING_TRY_COUNT;
	int try_wait_count;
	int sock;
	struct socket *so;

	for (int i = 0; i < try_connect_count; i++) {
		sock = ofp_socket(OFP_AF_INET, OFP_SOCK_STREAM, OFP_IPPROTO_TCP);
		if (sock < 0) {
			OFP_ERR("ofp_socket failed");
			return -1;
		}
		memset(&server_addrin, 0, sizeof(server_addrin));
		server_addrin.sin_family = OFP_AF_INET;
		server_addrin.sin_port = odp_cpu_to_be_16(server_port);
		server_addrin.sin_addr.s_addr = inet_addr(server_address);
		server_addrin.sin_len = sizeof(server_addrin);

		if(ofp_connect(sock, (struct ofp_sockaddr*)&server_addrin, 
						sizeof(server_addrin)) < 0) {
			OFP_ERR("ofp_connect failed\n");
			ofp_close(sock);
			return -2;
		}

		usleep(DEFAULT_CHECK_CONNECT_INTERVAL);
		try_wait_count = DEFAULT_CONNECTING_WAIT_COUNT;
		so = ofp_get_sock_by_fd(sock);
		while (try_wait_count > 0 && !(so->so_state & SS_ISCONNECTED)) {
			try_wait_count--;
			usleep(DEFAULT_CHECK_CONNECT_INTERVAL);
		}
		if (so->so_state & SS_ISCONNECTED)
			break;
		else {
			ofp_close(sock);
			printf("retry %d connecting\n", i + 1);
			usleep(DEFAULT_CHECK_CONNECT_INTERVAL);
		}
	}
	if (!(so->so_state & SS_ISCONNECTED)) {
		OFP_ERR(""__R_"<FAIL> connect server fail");
		ofp_close(sock);
		return -2;
	}
		
	OFP_INFO("connect %s:%d success, sockfd %d", 
			fast_frame_args.ip_address, fast_frame_args.ip_port, sock);
	usleep(DEFAULT_CHECK_CONNECT_INTERVAL);
	return sock;
}

void *start_routine(void *arg)
{
        odph_linux_thr_params_t *thr_params = arg;

        if (odp_init_local(thr_params->instance, thr_params->thr_type)) {
                OFP_ERR("Local init failed\n");
                return NULL;
        }

        void *ret_ptr = thr_params->start(thr_params->arg);
        int ret = odp_term_local();

        if (ret < 0)
                OFP_ERR("Local term failed\n");
        else if (ret == 0 && odp_term_global(thr_params->instance))
                OFP_ERR("Global term failed\n");

        return ret_ptr;
}

void command_help() {
	printf("command:\n\n");
	printf("\thelp | ?\n");
	printf("\tcp filepath1 filepath2 ...\n");
	printf("\tset burst on|off\n");
	printf("\n");
}

#define COMMAND_OFPCP_ERROR		-1
#define COMMAND_OFPCP_QUIT		1
#define COMMAND_OFPCP_HELP		2
#define COMMAND_OFPCP_CP		3
#define COMMAND_OFPCP_SET_BURST		4

int command_type(char *command) {
	if (!strncmp(command, "help", strlen("help")) ||
			!strncmp(command, "?", strlen("?")))
		return COMMAND_OFPCP_HELP;
	if (!strncmp(command, "cp", strlen("cp")))
		return COMMAND_OFPCP_CP;
	if (!strncmp(command, "quit", strlen("quit")) || 
			!strncmp(command, "exit", strlen("exit")))
		return COMMAND_OFPCP_QUIT;
	if (!strncmp(command, "set burst", strlen("set burst")))
		return COMMAND_OFPCP_SET_BURST;
	return COMMAND_OFPCP_ERROR;
}

void command_set_burst(char *command) {
	char *mark = command + strlen("set burst");
	mark = skip_space(mark);
	if (!strncmp(mark, "on", strlen("on"))) {
		printf("BURST on\n");
		fast_frame_args.burst_mode = 1;
	} else if (!strncmp(mark, "off", strlen("off"))) {
		printf("BURST off\n");
		fast_frame_args.burst_mode = 0;
	} else {
		printf("BURST command unknown\n");
	}
}

void command_cp_process(char *command) {
	struct file_list *file_list;
	char *saveptr, *token;

	file_list = malloc(sizeof(struct file_list));
	assert(file_list);
	create_file_list(file_list);

	token = strtok_r(command + strlen("cp") + 1, " \t", &saveptr);
	while (token) {
		if (!is_blank_string(token)) {
			add_new_file(file_list, token);
			printf("+'%s'\n", token);
		}
		token = strtok_r(NULL, " \t", &saveptr);
	}
	if (fast_frame_args.pool_count) {
		__OFPCP_LOCK_DO(fast_frame_args.pool.locker,
			splice_file_list(fast_frame_args.pool.file_list, file_list));
	} else
		start_process(get_one_unworks_cpu(), request_queue_session_start, 
			file_list, 0);

	destroy_file_list(file_list);
	free(file_list);
}


#define COMMAND_MAX_SIZE	2047
void command_interact() {
	char *command;

	command = malloc(COMMAND_MAX_SIZE + 1);
	if (command == NULL) {
		OFP_ERR("Err: memory out\n");
		return;
	}
	while (1) {
		printf("ofpcp>");
		if (!fgets(command, COMMAND_MAX_SIZE, stdin))
			goto quit;
		if (is_blank_string(command))
			continue;
		if (command[strlen(command) - 1] == '\n')
			command[strlen(command) - 1] = 0;
		switch(command_type(command)) {
		case COMMAND_OFPCP_ERROR:
			printf("unknow command\n");
			continue;
		case COMMAND_OFPCP_QUIT:
			goto quit;
		case COMMAND_OFPCP_HELP:
			command_help();
			break;
		case COMMAND_OFPCP_CP:
			command_cp_process(command);
			break;
		case COMMAND_OFPCP_SET_BURST:
			command_set_burst(command);
			break;
		}
	}
quit:
	free(command);
	return;
}

void send_req_connect(struct ofpcp_handle *handle) {
	__NEW_REQ(handle, req, OFPCP_REQUEST_TYPE_CONNECT);
	__LOCK_APPEND_REQ(handle, req);
}

void send_req_close(struct ofpcp_handle *handle) {
	__NEW_REQ(handle, req, OFPCP_REQUEST_TYPE_CLOSE);
	__LOCK_APPEND_REQ(handle, req);
}

void send_req_data(struct ofpcp_handle *handle, char *data, int size) {
	__NEW_REQ(handle, req, OFPCP_REQUEST_TYPE_SEND);
	req->data = data;
	req->size = size;
	__LOCK_APPEND_REQ(handle, req);
}

void send_req_data_sema(struct ofpcp_handle *handle, char *data, int size) {
	__NEW_REQ(handle, req, OFPCP_REQUEST_TYPE_SEND_SEMA);
	req->data = data;
	req->size = size;
	__LOCK_APPEND_REQ(handle, req);
}

void send_req_sema(struct ofpcp_handle *handle) {
	__NEW_REQ(handle, req, OFPCP_REQUEST_TYPE_SEMA);
	__LOCK_APPEND_REQ(handle, req);
}

void send_req_recv_ack_sema(struct ofpcp_handle *handle) {
	__NEW_REQ(handle, req, OFPCP_REQUEST_TYPE_RECV_ACK_SEMA);
	__LOCK_APPEND_REQ(handle, req);
}

void send_req_quit(struct ofpcp_agent* agent) {
	struct ofpcp_request *req = ofpcp_request_slab_alloc(&agent->request_slab);
	assert(req);
	req->type = OFPCP_REQUEST_TYPE_QUIT;
	ofpcp_lock(agent->req_lock);
	append_ofpcp_request(&agent->requests, req);
	ofpcp_unlock(agent->req_lock);
}

void *worker_processor(void *arg) {
	struct ofpcp_handle *handle;
	struct ofpcp_agent *agent;
	unsigned long filesize, size;
	unsigned int split_size = fast_frame_args.split_size;
	struct stat *statbuf;
	char *send_buffer;
	unsigned int buffer_size = (split_size == 0) ? DEFAULT_BUFFER_SIZE : split_size;
	char *filename = (char*)arg;
	int filename_len = strlen(filename);
	int finish_mark = -1;
	int want;
	int ret;

	printf("start worker processor, process %s\n", filename);
	agent = agents[random() % fast_frame_args.agent_count];

	handle = new_ofpcp_handle();
	assert(handle);

	handle->agent = agent;

	__PR("REQUEST CONNECT");
	send_req_connect(handle);
	ofpcp_wait(&handle->sema);

	if (handle->sockfd <= 0) {
		printf("connect server fail\n");
		goto leave_destroy;
	}

	__OFPCP_LOCK_DO(hashs_lock, ofpcp_hash_add_entry(hashs, handle));

	handle->localfd = open(filename, O_RDONLY);
	if (handle->localfd < 0) {
		printf("Error: data file %s cannot open", filename);
		goto leave_remove_entry;
	}
	statbuf = malloc(sizeof(struct stat));
	if (stat(filename, statbuf) < 0) {
		printf("Error: data file %s get size error", filename);
		free(statbuf);
		goto leave_remove_entry;
	}
	filesize = statbuf->st_size;
	free(statbuf);

	printf("start send '%s' size %ld %s\n", filename, filesize,
			fast_frame_args.pseudo_mode ? "(PSEUDO)" : "");

	send_req_data(handle, (char*)&filename_len, sizeof(int));
	send_req_data(handle, filename, filename_len);
	send_req_data(handle, (char*)&filesize, sizeof(unsigned long));
	send_req_data(handle, (char*)&split_size, sizeof(unsigned int));

	send_buffer = malloc(buffer_size);
	__PR("alloc send_buffer %d", buffer_size);
	if (send_buffer == NULL) {
		printf("Error: send buffer malloc fail");
		goto leave_remove_entry;
	}

	printf("start sending %s body, %ld size, split %d\n", filename, filesize, split_size);
	size = filesize;
	while (size > 0) {
		want = (size > buffer_size) ? buffer_size : size;
		ret = read_local_file(handle->localfd, send_buffer, want);
		if (ret < 0) {
			printf("Error: read file fail");
		} else if (ret == 0) {
			printf("Error: read file end");
		}
		send_req_data(handle, send_buffer, want);
		size -= want;

		if (split_size) {
			if (!fast_frame_args.notify_mode)
				send_req_recv_ack_sema(handle);
			ofpcp_wait(&handle->sema);
		}
	}

	free(send_buffer);
	printf("send file " __G_ "%s OK" __0_ ", size %ld\n", filename, filesize);

	close_local_file(handle->localfd);

	send_req_data(handle, (char*)&finish_mark, sizeof(int));
	sleep(1);

	send_req_close(handle);
	ofpcp_wait(&handle->sema);

leave_remove_entry:
	__OFPCP_LOCK_DO(hashs_lock,
		ofpcp_hash_remove_entry(hashs, handle));
leave_destroy:
	destroy_ofpcp_handle(handle);
	printf("session close\n");
	return NULL;
}

void client_pkt_notify(union ofp_sigval sv) {
	struct ofp_sock_sigval *ss = sv.sival_ptr;
	int sockfd = ss->sockfd;
	int event = ss->event;
	struct ofpcp_handle *handle;
	odp_packet_t pkt = ss->pkt;
	char *data;
	int pkt_size;

	if (event != OFP_EVENT_RECV) {
		OFP_INFO("NOT OFP_EVENT_RECV");
		return;
	}

	pkt_size = odp_packet_len(pkt);
	data = odp_packet_data(pkt);
	__PR("recv: sock %d size %d", sockfd, pkt_size);

	__OFPCP_LOCK_DO(hashs_lock,
		handle = ofpcp_hash_find_sockfd(hashs, sockfd));

	assert(handle);
	if (handle) {
		if (pkt_size > 0) {
			if (pkt_size == 1 && data[0] == ACK_CHAR) {
				__PR("sock %d receive ACKCHAR", sockfd);
				ofpcp_post(&handle->sema);
			}
		} else if (pkt_size == 0) {
			printf("server disconnect\n");
			handle->sockfd = 0;
		}
	}

	odp_packet_free(pkt);
	ss->pkt = ODP_PACKET_INVALID;
}

void client_set_notify(int fd) {
	struct ofp_sigevent ev;
	struct ofp_sock_sigval ss;

	memset(&ss, 0, sizeof(struct ofp_sock_sigval));
	ss.sockfd = fd;
	ss.event = 0;
	ss.pkt = ODP_PACKET_INVALID;

	memset(&ev, 0, sizeof(struct ofp_sigevent));
	ev.ofp_sigev_notify = 1;
	ev.ofp_sigev_notify_function = client_pkt_notify;
	ev.ofp_sigev_value.sival_ptr = &ss;
	if (ofp_socket_sigevent(&ev) == -1) {
		OFP_ERR("Error: failed configure sock callback: errno = %s",
				ofp_strerror(ofp_errno));
		return;
	}
	sleep(1);
}


void *agent_processor(void *arg) {
	int id = *(int*)arg;
	struct ofpcp_agent *this_agent = agents[id];
	struct ofpcp_request *req;
	int quit = 0;

	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}
	OFP_INFO("Agent #%d (%ld) start.", id, pthread_self());

	while (1) {
		__OFPCP_LOCK_DO(this_agent->req_lock,
			req = ofpcp_request_pop_head(&this_agent->requests));
		if (req == NULL) {
			usleep(5000);
			continue;
		}

		assert(req->handle);
		switch(req->type) {
		case OFPCP_REQUEST_TYPE_CONNECT:
			req->handle->sockfd = connect_server(
				fast_frame_args.ip_address, 
				fast_frame_args.ip_port);
			if (fast_frame_args.notify_mode)
				client_set_notify(req->handle->sockfd);
			ofpcp_post(&req->handle->sema);
			break;

		case OFPCP_REQUEST_TYPE_CLOSE:
			if (req->handle->sockfd) {
				ofp_close(req->handle->sockfd);
				req->handle->sockfd = 0;
				sleep(1);
			}
			ofpcp_post(&req->handle->sema);
			break;

		case OFPCP_REQUEST_TYPE_SEND:
			if (req->handle->sockfd) {
				send_pack_size(req->handle->sockfd, req->data, req->size);
			} else
				assert(0);
			break;

		case OFPCP_REQUEST_TYPE_SEND_SEMA:
			if (req->handle->sockfd) {
				send_pack_size(req->handle->sockfd, req->data, req->size);
			}
			ofpcp_post(&req->handle->sema);
			break;

		case OFPCP_REQUEST_TYPE_SEMA:
			ofpcp_post(&req->handle->sema);
			break;

		case OFPCP_REQUEST_TYPE_RECV_ACK_SEMA:
			if (req->handle->sockfd) {
				char buf;
				int ret;
				ret = recv_pack_size(req->handle->sockfd, &buf, 1);
				if (!ret) 
					ofpcp_post(&req->handle->sema);
			}
			break;

		case OFPCP_REQUEST_TYPE_QUIT:
			OFP_INFO("Req agent quit");
			quit = 1;
			break;

		default:
			OFP_ERR("Error: unknown req %d", req->type);
			assert(0);
		}

		ofpcp_request_slab_free(&this_agent->request_slab, req);
		if (quit)
			break;
		sched_yield();
	}

	ofp_term_local();
	printf("Agent(#%d) exit.\n", id);
	return NULL;
}

void *client_connect_pool_start(void *arg) {
	int pool_id = *(int *)arg;
	int ret;
	int sock;
	char *filename = NULL;
	int datafile;
	unsigned long filesize = 0;
	unsigned int splitsize = fast_frame_args.split_size;
	struct stat statbuf;


	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}
	OFP_INFO("Pool(#%d) start.", pool_id);

	sock = connect_server(fast_frame_args.ip_address, fast_frame_args.ip_port);
	if (sock < 0)
		goto leave_with_term;
	sleep(1);

	while (1) {
		if (filename) {
			free(filename);
			filename = NULL;
		}
		ofpcp_lock(fast_frame_args.pool.locker);
		if (fast_frame_args.pool.file_list == NULL) {
			ofpcp_unlock(fast_frame_args.pool.locker);
			usleep(500000);
			continue;
		}
		ret = remove_filename(fast_frame_args.pool.file_list, &filename);
		ofpcp_unlock(fast_frame_args.pool.locker);
		if (ret < 0) {
			if (!fast_frame_args.command_mode) 
				break;
			usleep(20000);
			continue;
		}

		if (is_blank_string(filename)) {
			continue;
		}
		OFP_INFO("Pool(#%d) start send '%s'%s", pool_id, filename,
				(fast_frame_args.pseudo_mode ? "(PSEUDO)" : ""));

		datafile = open(filename, O_RDONLY);
		if (datafile < 0) {
			OFP_INFO("Error: data file %s cannot open", filename);
			continue;
		}
		if (stat(filename, &statbuf) < 0) {
			OFP_INFO("Error: data file %s get size error", filename);
			close(datafile);
			continue;
		}
		filesize = statbuf.st_size;

		if (send_filename_and_size(sock, filename, filesize, splitsize) < 0) {
			OFP_INFO("Error: send '%s' HEAD fail", filename);
			close(datafile);
			continue;
		}

		if (send_filebody(sock, filename, datafile, filesize, splitsize) < 0) {
			OFP_INFO("Error: send '%s' BODY fail", filename);
			close(datafile);
			continue;
		}
		OFP_INFO("Pool(#%d) send file " __G_ "%s OK" __0_ ", size %ld", 
				pool_id, filename, filesize);

		close(datafile);
		usleep(20000);
	}
	
	OFP_INFO("send finish mark");
	send_finish_mark(sock);
	sleep(2);

	//ofp_shutdown(sock, OFP_SHUT_RDWR);
	ofp_close(sock);
	sleep(2);

leave_with_term:
	ofp_term_local();
	printf("Pool(#%d) exit.\n", pool_id);
	return NULL;
}

#define AGENT_REQUEST_SLAB_SIZE	1024

int client_start_agents_and_workers() {
	int agent_count = fast_frame_args.agent_count;
	int file_count = fast_frame_args.file_list.count;
	odph_linux_pthread_t *agent_threads;
	pthread_t *worker_threads;
	int cpu_id;
	int ret;
	
	if (agent_count == 0)
		return 0;

	for (int i = 0; i < agent_count; i++) {
		agents[i] = calloc(sizeof(struct ofpcp_agent), 1);
		assert(agents[i]);
		ofpcp_lock_init(agents[i]->req_lock);
		ofpcp_request_slab_init(&agents[i]->request_slab, AGENT_REQUEST_SLAB_SIZE);
	}

	agent_threads = malloc(sizeof(odph_linux_pthread_t) * agent_count);
	if (!agent_threads)
		return 0;
	for (int i = 0; i < agent_count; i++) {
		cpu_set_t cpu_set;

		cpu_id = get_one_unworks_cpu();
		CPU_ZERO(&cpu_set);
		CPU_SET(cpu_id, &cpu_set);

		agent_threads[i].thr_params.arg = (void*)&i;
		agent_threads[i].thr_params.thr_type = ODP_THREAD_CONTROL;
		agent_threads[i].thr_params.instance = instance;
		agent_threads[i].thr_params.start = agent_processor;

		pthread_attr_init(&agent_threads[i].attr);
		agent_threads[i].cpu = cpu_id;
		pthread_attr_setaffinity_np(&agent_threads[i].attr,
					sizeof(cpu_set_t), &cpu_set);

		ret = pthread_create(&agent_threads[i].thread,
				&agent_threads[i].attr,
				start_routine,
				&agent_threads[i].thr_params);

		if (ret != 0) {
			OFP_ERR("Err: POOL pthread create cpu #%d\n", cpu_id);
			return 0;
		}
		usleep(200000);
	}

	worker_threads = malloc(sizeof(pthread_t) * file_count);
	if (!worker_threads)
		return 0;
	for (int i = 0; i < file_count; i++) {
		cpu_set_t cpu_set;
		pthread_attr_t attr;

		cpu_id = get_one_unworks_cpu();
		CPU_ZERO(&cpu_set);
		CPU_SET(cpu_id, &cpu_set);

		OFP_INFO("create worker pthread %d/%d", i, file_count);
		pthread_attr_init(&attr);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpu_set);
		if (pthread_create(&worker_threads[i], &attr,
				worker_processor, 
				(void*)fast_frame_args.file_list.filename[i])) {
			perror("create thread");
		}
		pthread_attr_destroy(&attr);
	}

	for (int i = 0; i < file_count; i++) {
		pthread_join(worker_threads[i], NULL);
	}
	free(worker_threads);

	for (int i = 0; i < agent_count; i++) 
		send_req_quit(agents[i]);

	odph_linux_pthread_join(agent_threads, agent_count);
	free(agent_threads);
	return 1;
}

int client_start_pools() {
	int pool_count = fast_frame_args.pool_count;
	odph_linux_pthread_t *ofpcp_pthread;
	int cpu_id;
	int ret;

	if (pool_count == 0)
		return 0;

	OFP_INFO("start POOL mode, total %d", pool_count);

	fast_frame_args.pool.file_list = NULL;
	ofpcp_lock_init(fast_frame_args.pool.locker);

	ofpcp_pthread = malloc(sizeof(odph_linux_pthread_t) * pool_count);
	if (!ofpcp_pthread)
		return 0;
	for (int i = 0; i < pool_count; i++) {
		cpu_set_t cpu_set;

		cpu_id = get_one_unworks_cpu();
		CPU_ZERO(&cpu_set);
		CPU_SET(cpu_id, &cpu_set);

		ofpcp_pthread[i].thr_params.arg = (void*)&i;
		ofpcp_pthread[i].thr_params.thr_type = ODP_THREAD_CONTROL;
		ofpcp_pthread[i].thr_params.instance = instance;
		ofpcp_pthread[i].thr_params.start = client_connect_pool_start;

		pthread_attr_init(&ofpcp_pthread[i].attr);
		ofpcp_pthread[i].cpu = cpu_id;
		pthread_attr_setaffinity_np(&ofpcp_pthread[i].attr,
					sizeof(cpu_set_t), &cpu_set);

		ret = pthread_create(&ofpcp_pthread[i].thread,
				&ofpcp_pthread[i].attr,
				start_routine,
				&ofpcp_pthread[i].thr_params);

		if (ret != 0) {
			OFP_ERR("Err: POOL pthread create cpu #%d\n", cpu_id);
			return 0;
		}
		sleep(1);
	}

	/* set file list, let pool thread start process */
	__OFPCP_LOCK_DO(fast_frame_args.pool.locker,
		fast_frame_args.pool.file_list = &fast_frame_args.file_list);

	odph_linux_pthread_join(ofpcp_pthread, pool_count);
	free(ofpcp_pthread);
	return 1;
}

void client_pingpong() {
	start_process(get_one_unworks_cpu(), pingpong_start, &fast_frame_args.pingpong_count, 0);
}

void client_send_files(struct file_list* file_list) {
	odph_linux_pthread_t *ofpcp_pthread;
	int cpu_id;
	int request_count = file_list->count;
	int ret;

	if (fast_frame_args.burst_mode) {
		ofpcp_pthread = malloc(sizeof(odph_linux_pthread_t) * request_count);
		if (!ofpcp_pthread)
			return;
		memset(ofpcp_pthread, 0, sizeof(odph_linux_pthread_t) * request_count);
		for (int i = 0; i < request_count; i++) {
			cpu_set_t cpu_set;

			cpu_id = get_one_unworks_cpu();
			CPU_ZERO(&cpu_set);
			CPU_SET(cpu_id, &cpu_set);

			ofpcp_pthread[i].thr_params.arg = (void*)file_list->filename[i];
			ofpcp_pthread[i].thr_params.thr_type = ODP_THREAD_CONTROL;
			ofpcp_pthread[i].thr_params.instance = instance;
			ofpcp_pthread[i].thr_params.start = request_single_session_start;

			pthread_attr_init(&ofpcp_pthread[i].attr);
			ofpcp_pthread[i].cpu = cpu_id;
			pthread_attr_setaffinity_np(&ofpcp_pthread[i].attr,
							sizeof(cpu_set_t), &cpu_set);

			ret = pthread_create(&ofpcp_pthread[i].thread, 
					&ofpcp_pthread[i].attr,
					start_routine,
					&ofpcp_pthread[i].thr_params);

			if (ret != 0) {
				OFP_ERR("Err: pthread create cpu #%d\n", cpu_id);
				return;
			}
			usleep(300000);

		}
		odph_linux_pthread_join(ofpcp_pthread, request_count);
		free(ofpcp_pthread);
	} else {
		start_process(get_one_unworks_cpu(), request_queue_session_start, 
				file_list, 0);
	}
}

void* client_thread(void *arg) {
	printf("client starting\n");
	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}

	if (fast_frame_args.pingpong_count) {
		client_pingpong();
	} else if (fast_frame_args.pool_count) {
		client_start_pools();
	}
	else if (fast_frame_args.command_mode) {
		command_interact();
	} else if (fast_frame_args.agent_count) {
		client_start_agents_and_workers();
	} else
		client_send_files(&fast_frame_args.file_list);

	ofp_term_local();
	printf("client exit.\n");

	return NULL;
}

void* pingpong_start(void *arg) {
	int sock;
	struct timeval start_time, end_time;
	unsigned long elapse_us;
	int count = *(int *)arg;

	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}

	sock = connect_server(fast_frame_args.ip_address, fast_frame_args.ip_port);
	if (sock < 0)
		goto leave_with_term;

	OFP_INFO("start pingpong REQUEST, count %d", count);
	gettimeofday(&start_time, NULL);
	for (int i = 0; i < count; i++) {
		if (single_pingpong_request(sock, i) < 0) 
			goto leave_with_shutdown;
	}
	gettimeofday(&end_time, NULL);
	elapse_us = (end_time.tv_sec - start_time.tv_sec) * 1000000 + 
					(end_time.tv_usec - start_time.tv_usec);

	printf(_OFPCP_G("ping round %d, total %ldus, average %ldus\n"),
		count, elapse_us, elapse_us / count);

	send_finish_mark(sock);
	sleep(1);

leave_with_shutdown:
	//ofp_shutdown(sock, OFP_SHUT_RDWR);
	ofp_close(sock);
	sleep(2);
leave_with_term:
	ofp_term_local();
	printf("client session completed.\n");
	return NULL;
}

void* request_queue_session_start(void *arg) {
	struct file_list *file_list;
	char* filename;
	int sock;
	int datafile;
	unsigned long filesize = 0;
	unsigned int splitsize = fast_frame_args.split_size;
	struct stat statbuf;

	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}
	file_list = (struct file_list*)arg;
	
	sock = connect_server(fast_frame_args.ip_address, fast_frame_args.ip_port);
	if (sock < 0)
		goto leave_with_term;

	for (int i = 0; i < file_list->count; i++) {
		filename = file_list->filename[i];
		assert(filename != NULL);
		if (is_blank_string(filename))
			continue;

		datafile = open(filename, O_RDONLY);
		if (datafile < 0) {
			OFP_INFO("Error: data file %s cannot open", filename);
			continue;
		}
		if (stat(filename, &statbuf) < 0) {
			OFP_INFO("Error: data file %s get size error", filename);
			close(datafile);
			continue;
		}
		filesize = statbuf.st_size;

		OFP_INFO("start send '%s' size %ld %s", filename, filesize,
				fast_frame_args.pseudo_mode ? "(PSEUDO)" : "");

		/* send filename */
		if (send_filename_and_size(sock, filename, filesize, splitsize) < 0) {
			OFP_INFO("Error: send '%s' HEAD fail", filename);
			close(datafile);
			goto leave_with_shutdown;
		}

		/* send filebody */
		if (send_filebody(sock, filename, datafile, filesize, splitsize) < 0) {
			OFP_INFO("Error: send '%s' BODY fail", filename);
			close(datafile);
			goto leave_with_shutdown;
		}
		OFP_INFO("send file " __G_ "%s OK" __0_ ", size %ld", filename, filesize);
		close(datafile);
	}

	OFP_INFO("send finish mark");
	send_finish_mark(sock);
	sleep(2);

leave_with_shutdown:
	//ofp_shutdown(sock, OFP_SHUT_RDWR);
	ofp_close(sock);
	sleep(2);
leave_with_term:
	ofp_term_local();
	printf("client session completed.\n");
	return NULL;

}

void* request_single_session_start(void *arg) {
	int sock;
	int datafile;
	unsigned long filesize = 0;
	unsigned int splitsize = fast_frame_args.split_size;
	struct stat statbuf;
	char *filename = (char*)arg;


	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}
	sock = connect_server(fast_frame_args.ip_address, fast_frame_args.ip_port);
	if (sock < 0)
		goto leave_with_term;

	datafile = open(filename, O_RDONLY);
	if (datafile < 0) {
		OFP_INFO("Error: data file %s cannot open", filename);
		goto leave_with_shutdown;
	}
	if (stat(filename, &statbuf) < 0) {
		OFP_INFO("Error: data file %s get size error", filename);
		goto leave_with_closedata;
	}
	filesize = statbuf.st_size;

	OFP_INFO("start send '%s' size %ld %s", filename, filesize,
			fast_frame_args.pseudo_mode ? "(PSEUDO)" : "");

	/* send filename */
	if (send_filename_and_size(sock, filename, filesize, splitsize) < 0) {
		OFP_INFO("Error: send '%s' HEAD fail", filename);
		goto leave_with_closedata;
	}

	/* send filebody */
	if (send_filebody(sock, filename, datafile, filesize, splitsize) < 0) {
		OFP_INFO("Error: send '%s' BODY fail", filename);
		goto leave_with_closedata;
	}

	OFP_INFO("send file " __G_ "%s OK" __0_ ", size %ld", filename, filesize);

leave_with_closedata:
	close(datafile);
	send_finish_mark(sock);
	sleep(2);
leave_with_shutdown:
	//ofp_shutdown(sock, OFP_SHUT_RDWR);
	ofp_close(sock);
	sleep(2);
leave_with_term:
	ofp_term_local();
	printf("client session completed.\n");
	return NULL;
}

int set_recv_buffer_size(int fd, unsigned long size) {
	unsigned long recv_buf_size = (size > 0) ? size : DEFAULT_RECEIVE_BUFFER_SIZE;
	if (ofp_setsockopt(fd, OFP_SOL_SOCKET, OFP_SO_RCVBUF,
				&recv_buf_size, sizeof(recv_buf_size)) < 0) {
		OFP_ERR("ofp_setsockopt (OFP_SO_RCVBUF) fail");
		return -1;
	}

	return 0;
}

void server_send_ack_back(int sock) {
	char ack = ACK_CHAR;
	if (send_pack_size(sock, &ack, 1) < 0) {
		OFP_INFO("Error: send ACK fail");
	}
}

int server_pkt_process(struct ofpcp_handle *handle, odp_packet_t pkt, int start) {
	int pkt_size = odp_packet_len(pkt);
	char *data = odp_packet_data(pkt);
	int to, len;
	assert(pkt_size > start);

	if (handle->prep_status == FILENAME_LEN_PREPARE && start < pkt_size) {
		to = handle->prep_recv_size;
		len = min(sizeof(int) - to, pkt_size - start);
		memcpy((char*)&handle->filename_len + to, data + start, len);
		handle->prep_recv_size += len;
		start += len;
		if (handle->prep_recv_size == sizeof(int)) {
			handle->prep_status = FILENAME_PREPARE;
			handle->prep_recv_size = 0;
			if (handle->filename_len == -1) {
				OFP_INFO("receive finish mark");
				return 0;
			} else if (handle->filename_len > 0) {
				handle->filename = calloc(handle->filename_len + 1, 1);
				assert(handle->filename);
				OFP_INFO("filename_len: %d", handle->filename_len);
			}
		}
	}

	if (handle->prep_status == FILENAME_PREPARE && start < pkt_size) {
		to = handle->prep_recv_size;
		len = min(handle->filename_len - to, pkt_size - start);
		memcpy(handle->filename + to, data + start, len);
		handle->prep_recv_size += len;
		start += len;
		if (handle->prep_recv_size == handle->filename_len) {
			handle->prep_status = FILESIZE_PREPARE;
			handle->prep_recv_size = 0;
			OFP_INFO("create filename: %s", handle->filename);
			handle->localfd = create_local_file(handle->filename);
		}
	}

	if (handle->prep_status == FILESIZE_PREPARE && start < pkt_size) {
		to = handle->prep_recv_size;
		len = min(sizeof(unsigned long) - to, pkt_size - start);
		memcpy((char*)&handle->file_size + to, data + start, len);
		handle->prep_recv_size += len;
		start += len;
		if (handle->prep_recv_size == sizeof(unsigned long)) {
			handle->prep_status = SPLITSIZE_PREPARE;
			handle->prep_recv_size = 0;
			OFP_INFO("filesize: %ld", handle->file_size);
		}
	}

	if (handle->prep_status == SPLITSIZE_PREPARE && start < pkt_size) {
		to = handle->prep_recv_size;
		len = min(sizeof(int) - to, pkt_size - start);
		memcpy((char*)&handle->split_size + to, data + start, len);
		handle->prep_recv_size += len;
		start += len;
		if (handle->prep_recv_size == sizeof(int)) {
			handle->prep_status = FILEBODY_PREPARE;
			handle->prep_recv_size = 0;
			gettimeofday(&handle->start_time, NULL);
			handle->body = malloc(handle->split_size);
			assert(handle->body);
			OFP_INFO("split: %d", handle->split_size);
		}
	}
	
	if (handle->prep_status == FILEBODY_PREPARE && start < pkt_size) {
		to = handle->prep_recv_size;
		len = min(handle->split_size - to, pkt_size - start);
		memcpy(handle->body + to, data + start, len);
		handle->prep_recv_size += len;
		handle->file_received += len;
		if (handle->prep_recv_size == handle->split_size 
				|| handle->file_received >= handle->file_size) {
			server_send_ack_back(handle->sockfd);
			write_local_file(handle->localfd, handle->body, handle->prep_recv_size);
			handle->prep_recv_size = 0;
			if (handle->file_received >= handle->file_size) {	
				struct timeval end_time;
				unsigned long elapse_us;
				gettimeofday(&end_time, NULL);
				elapse_us = (end_time.tv_sec - handle->start_time.tv_sec) * 1000000 + 
					(end_time.tv_usec - handle->start_time.tv_usec);

				OFP_INFO(__G_"<OK> '%s'"__0_" %ld bytes, %ldms, %.02fMB/s%s",
					handle->filename, handle->file_size, elapse_us / 1000, 
					(float)(handle->file_size * 1000000 / (1024 * 1024 * elapse_us)),
					(fast_frame_args.pseudo_mode) ? "(PSEUDO)" : "");
				close_local_file(handle->localfd);
				handle->total_file_count++;
				if (handle->filename) {
					free(handle->filename);
					handle->filename = NULL;
				}
				if (handle->body) {
					free(handle->body);
					handle->body = NULL;
				}
				handle->prep_status = FILENAME_LEN_PREPARE;
				handle->file_received = 0;
				handle->filename_len = 0;
				handle->file_size = 0;
			} else {
				/*
				int percent = (int)(handle->file_received * 100 / handle->file_size);
				if (!(percent % 20))
					OFP_INFO("file '%s', got %d%%", handle->filename, percent);
				*/
			}
		}
	}
	return 1;
}

void server_pkt_notify(union ofp_sigval sv) {
	struct ofp_sock_sigval *ss = sv.sival_ptr;
	int sockfd = ss->sockfd;
	int event = ss->event;
	odp_packet_t pkt = ss->pkt;
	int pkt_size;
	struct ofpcp_handle *handle;
	static int counter = 0;

	if (event == OFP_EVENT_ACCEPT) {
		struct ofp_sockaddr_in caller;
		ofp_socklen_t alen = sizeof(caller);

		ofp_accept(ss->sockfd, (struct ofp_sockaddr *)&caller, &alen);
		OFP_INFO("notify accept connection %d (sock %d)", ++counter, ss->sockfd2);
		handle = new_ofpcp_handle();
		assert(handle);
		handle->sockfd = ss->sockfd2;
		__OFPCP_LOCK_DO(hashs_lock,
			ofpcp_hash_add_entry(hashs, handle));
		return;
	}

	if (event != OFP_EVENT_RECV)
		return;

	__OFPCP_LOCK_DO(hashs_lock,
		handle = ofpcp_hash_find_sockfd(hashs, sockfd));
	if (handle == NULL) {
		goto leave;			
	}
	assert(handle->sockfd == sockfd);

	pkt_size = odp_packet_len(pkt);
	if (pkt_size > 0) {
		__PR("sock %d receive size %d", sockfd, pkt_size);
		__sync_add_and_fetch(&stats.bs1, pkt_size);
		server_pkt_process(handle, pkt, 0);
	} else if (pkt_size == 0) {
		if (handle->prep_status == FILEBODY_PREPARE) {
			OFP_ERR(__R_"receive file '%s' interrupt, got %ld/%ld"__0_, 
				handle->filename, handle->file_received, handle->file_size);
			close_local_file(handle->localfd);
		}
		OFP_INFO("connection close (received complete %d files)", handle->total_file_count);
		__OFPCP_LOCK_DO(hashs_lock, 
			ofpcp_hash_remove_entry(hashs, handle));
		ofp_close(sockfd);
		destroy_ofpcp_handle(handle);
	}

leave:
	odp_packet_free(pkt);
	ss->pkt = ODP_PACKET_INVALID;
}

void notify_accepting(int server_fd) {
	struct ofp_sigevent ev;
	struct ofp_sock_sigval ss;

	ss.sockfd = server_fd;
	ss.event = 0;
	ss.pkt = ODP_PACKET_INVALID;
	ev.ofp_sigev_notify = 1;
	ev.ofp_sigev_notify_function = server_pkt_notify;
	ev.ofp_sigev_value.sival_ptr = &ss;
	if (ofp_socket_sigevent(&ev) == -1) {
		OFP_ERR("Error: failed configure sock callback: errno = %s",
				ofp_strerror(ofp_errno));
		return;
	}

	OFP_INFO("waiting connect request (NOTIFY mode)");
	while (1) {
		sleep(5);
	}
}

void start_accepting(int server_fd) {
	int *accept_fd_p;
	int counter = 0;
	struct ofp_sockaddr_in caller_addrin;
	unsigned int alen = sizeof(caller_addrin);

	while (1) {
		accept_fd_p = malloc(sizeof(int));
		if (accept_fd_p == NULL) {
			sleep(10);
			continue;
		}
		OFP_INFO("waiting connect request %d", ++counter);
		*accept_fd_p = ofp_accept(server_fd, 
				(struct ofp_sockaddr*)&caller_addrin, &alen);
		if (*accept_fd_p < 0) {
			OFP_ERR("ofp_accept fail, err=%s", ofp_strerror(ofp_errno));
			free(accept_fd_p);
			sleep(10);
			continue;
		}
		start_process(get_one_unworks_cpu(), 
				server_session_start, (void*)accept_fd_p, 1);
		usleep(200000);
	}
}

#define STAT_BUF_SIZE	256
void *stat_thread_start(void *arg) {
	char buf[STAT_BUF_SIZE];
	int fd;
	unsigned long elapse_us;
	unsigned long offset_us;
	unsigned long elapse_bytes;
	fd = open("ofpcp.stat", O_CREAT | O_RDWR | O_TRUNC, 0644);
	if (fd < 0) {
		printf("Err: fail to create ofpcp.stat\n");
		return NULL;
	}
	__PR("start statistics thread, interval %d", fast_frame_args.stat_interval);
	while (1) {
		if (!stats.tv0.tv_sec && !stats.tv0.tv_usec) {
			gettimeofday(&stats.epoch, NULL);
			stats.tv0 = stats.epoch;
			sleep(fast_frame_args.stat_interval);
			continue;
		}
		gettimeofday(&stats.tv1, NULL);
		elapse_us = (stats.tv1.tv_sec - stats.tv0.tv_sec) * 1000000 +
				(stats.tv1.tv_usec - stats.tv0.tv_usec);
		offset_us = (stats.tv1.tv_sec - stats.epoch.tv_sec) * 1000000 +
				(stats.tv1.tv_usec - stats.epoch.tv_usec);
		elapse_bytes = stats.bs1 - stats.bs0;
		snprintf(buf, STAT_BUF_SIZE,
			"%12.6lf %12.6lf \n",
			(double)offset_us / 1000000.0,
			(double)elapse_bytes / (elapse_us * 1.024 * 1.024));
		stats.tv0 = stats.tv1;
		stats.bs0 = stats.bs1;
		write(fd, buf, strlen(buf));
		sleep(fast_frame_args.stat_interval);
	}
}

void* server_thread(void *arg) {
	int server_fd;
	int optval;
	struct ofp_sockaddr_in server_addrin;

	/* set working dir */
	if (fast_frame_args.work_dir) {
		if (chdir(fast_frame_args.work_dir) < 0) {
			OFP_ERR("Error: set working dir '%s' fail.", 
					fast_frame_args.work_dir);
			return NULL;
		}
		printf("SET work dir to: '%s'", fast_frame_args.work_dir);
	}

	if (fast_frame_args.stat_interval) {
		pthread_t stat_handle;
		pthread_create(&stat_handle, NULL, stat_thread_start, NULL);
	}

	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.\n");
		return NULL;
	}
	OFP_INFO("server starting");
	
	if ((server_fd = ofp_socket(OFP_AF_INET, OFP_SOCK_STREAM, 
				OFP_IPPROTO_TCP)) < 0) {
		OFP_ERR("ofp_socket failed");
		return NULL;
	}
	memset(&server_addrin, 0, sizeof(struct ofp_sockaddr_in));
	server_addrin.sin_family = OFP_AF_INET;
	server_addrin.sin_port = odp_cpu_to_be_16(fast_frame_args.ip_port);
	server_addrin.sin_addr.s_addr = 
		(fast_frame_args.ip_address == NULL || !fast_frame_args.ip_address[0]) ?
			OFP_INADDR_ANY : inet_addr(fast_frame_args.ip_address);
	server_addrin.sin_len = sizeof(struct ofp_sockaddr_in);

	optval = 1;
	if (ofp_setsockopt(server_fd, OFP_SOL_SOCKET, OFP_SO_REUSEADDR, 
				&optval, sizeof(optval)) < 0) {
		OFP_ERR("ofp_setsockopt (OFP_SO_REUSEADDR) fail");
		return NULL;
	}
	/*
	if (ofp_setsockopt(server_fd, OFP_SOL_SOCKET, OFP_SO_REUSEPORT,
				&optval, sizeof(optval)) < 0) {
		OFP_ERR("ofp_setsockopt (OFP_SO_REUSEPORT) fail");
		return NULL;
	}
	*/

	if (ofp_bind(server_fd, (const struct ofp_sockaddr *)&server_addrin,
				sizeof(struct ofp_sockaddr_in)) < 0) {
		OFP_ERR("ofp_bind failed, addr=%s, port=%d, err=%s",
			(fast_frame_args.ip_address == NULL || !fast_frame_args.ip_address[0]) ?
				"any" : fast_frame_args.ip_address, 
			fast_frame_args.ip_port,
			ofp_strerror(ofp_errno));
		return NULL;
	}
	OFP_INFO("listening @%s:%d", 
			(fast_frame_args.ip_address == NULL || !fast_frame_args.ip_address[0]) ? 
				"any" : fast_frame_args.ip_address, 
			fast_frame_args.ip_port);
	ofp_listen(server_fd, 20);

	if (fast_frame_args.notify_mode) 
		notify_accepting(server_fd);
	else 
		start_accepting(server_fd);
	return NULL;
}

void* server_session_start(void *arg) {
	int service_sock;
	char *filename;
	unsigned long filesize;
	unsigned int splitsize;
	int file;
	int request_type = REQUEST_TYPE_NONE;
	struct timeval start_time, end_time;
	unsigned long elapse_us;
	int ret;

	if (ofp_init_local()) {
		OFP_ERR("Error: OFP local init failed.");
		return NULL;
	}
	OFP_INFO("new connection accept.");
	service_sock = *(int*)arg;
	free(arg);

	while (1) {
		filename = receive_filename_and_size(service_sock, &filesize, &splitsize, &request_type);
		if (request_type == REQUEST_TYPE_FINISH)
			break;
		if (request_type == REQUEST_TYPE_PINGPONG) {
			pingpong_response(service_sock);
			continue;
		}
		if (filename == NULL) {
			OFP_INFO("Error: receive file HEAD fail");
			goto leave_with_shutdown;
		}

		OFP_INFO("filename:'%s' size:%ld split:%d %s", 
			filename, filesize, splitsize,
			fast_frame_args.pseudo_mode ? "(PSEUDO)" : "");

		gettimeofday(&start_time, NULL);
		file = create_local_file(filename);

		if (file < 0) {
			free(filename);
			goto leave_with_shutdown;
		}
		ret = receive_filebody(service_sock, file, filesize, splitsize);
		close_local_file(file);
		if (!ret) {
			gettimeofday(&end_time, NULL);
			elapse_us = (end_time.tv_sec - start_time.tv_sec) * 1000000 + 
					(end_time.tv_usec - start_time.tv_usec);

			OFP_INFO(__G_"<OK> '%s'"__0_" %ld bytes, %ldms, %.02fMB/s%s",
				filename, filesize, elapse_us / 1000, 
				(float)(filesize * 1000000 / (1024 * 1024 * elapse_us)),
				(fast_frame_args.pseudo_mode) ? "(PSEUDO)" : "");
		} else {
			OFP_INFO(__R_"<FAIL> '%s'"__0_, filename);
		}

		free(filename);
	}
	sleep(2);

leave_with_shutdown:
	//ofp_shutdown(service_sock, OFP_SHUT_RDWR);
	ofp_close(service_sock);
	sleep(2);
	ofp_term_local();
	OFP_INFO("service end");
	return NULL;
}

