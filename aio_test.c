/* aio_test.c - Test Asynchronous I/O (AIO) on a system */
/* Purpose:	The purpose of this test is to get an idea of
		AIO performance of a particular system given
		a dynamic range of parameters.
   Platform(s):	Linux x86-64
   Version: 	0.1
   Date:	9/19/2012
   Author:	Steve Glass
   Build:	gcc -Wall -lpthread -lrt -o aio_test aio_test.c
*/

#include <sys/time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <aio.h>
#include <signal.h>
#include <unistd.h>
#include <semaphore.h>
#include <errno.h>

#include "replgetopt.h"

#define SLEEP_SEC(x) sleep(x)
#define SLEEP_MSEC(x) \
	do{ \
        	if ((x) >= 1000){ \
                	sleep((x) / 1000); \
                        usleep((x) % 1000 * 1000); \
                } \
                else{ \
                        usleep((x)*1000); \
                } \
	}while (0)

#define SEMAPHORE_INCREMENT(sem) sem_post(&sem)
#define SEMAPHORE_DECREMENT(sem) \
        do { \
                while (sem_wait(&sem) < 0) {} \
        } while(0)
#define SEMAPHORE_GETVALUE(sem,value) sem_getvalue(&sem,&value)


#define MAX_AIO_CBS 		1000000
#define MAX_FDS			25000
#define FILENAME_MAX_LEN	24
#define USECS_IN_SECOND 	1000000

const char Purpose[] = "Test the AIO performance of a particular system.";
const char Usage[] =
"Usage: aio_test [bdfhlprtvwW]\n"
"Available options:\n"
"  -b, --batch          size of maximum batch (default is no batching)\n"
"  -d, --directory      Directory to write files\n"
"  -f, --fds            number of file descriptors\n"
"  -h. --help           print help\n"
"  -l, --length         length of each batch write\n"
"  -p, --pool           pool AIO's to specific fd's\n"
"  -r, --reads          number of max outstanding reads\n"
"  -t, --total          total number of writes\n"
"  -v, --verbose        verbose printing\n"
"  -w, --writes         number of max outstanding writes\n"
"  -W, --wait           wait for W seconds for writes/reads to coplete\n";

const char * OptionString = "b:d:f:w:r:l:t:vhW:p";
const struct option OptionTable[] = {
	{ "batch", required_argument, NULL, 'b'},
	{ "directory", required_argument, NULL, 'd'},
	{ "fds", required_argument, NULL, 'f'},
	{ "help", no_argument, NULL, 'h'},
        { "writes", required_argument, NULL, 'w' },
	{ "reads", required_argument, NULL, 'r'},
	{ "length", required_argument, NULL, 'l'},
	{ "pool", no_argument, NULL, 'p'},
	{ "total", required_argument, NULL, 't'},
	{ "verbose", no_argument, NULL, 'v'},
	{ "Wait", required_argument, NULL, 'W'}
};

/* Queue */
typedef struct Queue
{
        int capacity;
        int size;
        int front;
        int rear;
        int *elements;
	pthread_mutex_t lock;
}Queue;

/* Options struct */
struct Options {
	int batch;
	int fds;
        int writes;
	int reads;
	int length;
	int pool;
	int total;
	int verbose;
	int wait;
	int use_dir;
	char dir[512];
} options;

/* AIO Struct */
typedef struct aio_test_data
{
        struct aiocb my_aiocb;
        int sqn;
        int status;
        int verbose;
        int cur;
        int num_msgs;
	int queued;
        struct timeval start;
        pthread_mutex_t lock;
}aio_test_data;

/* Queue proc struct */
typedef struct queue_proc
{
	Queue *q;
	struct Options *opts;
}queue_proc;

double rmed = 0.0;
double rmin = 999999999;
double rmax = 0.0;
int clear = 0;
int errors = 0;
long int byteCount;
int tot_writes = 0;
int tot_reads = 0;
pthread_mutex_t g_lock;

void normalize_tv(struct timeval *tv)
{
	if (tv->tv_usec >= USECS_IN_SECOND) {
        	/* if more than 5 trips through loop required, then do divide and mod */
                if (tv->tv_usec >= (USECS_IN_SECOND*5)) {
                	tv->tv_sec += (tv->tv_usec / USECS_IN_SECOND);
                        tv->tv_usec = (tv->tv_usec % USECS_IN_SECOND);
                } else {
                	do {
                        	tv->tv_sec++;
                                tv->tv_usec -= USECS_IN_SECOND;
                       	} while (tv->tv_usec >= USECS_IN_SECOND);
                }
        } else if (tv->tv_usec <= -USECS_IN_SECOND) {
                /* if more than 5 trips through loop required, then do divide and mod */
                if (tv->tv_usec <= (-USECS_IN_SECOND*5)) {
                        tv->tv_sec -= (tv->tv_usec / -USECS_IN_SECOND); /* neg / neg = pos so subtract */
                        tv->tv_usec = (tv->tv_usec % -USECS_IN_SECOND); /* neg % neg = neg */
                } else {
                        do {
                                tv->tv_sec--;
                                tv->tv_usec += USECS_IN_SECOND;
                        } while (tv->tv_usec <= -USECS_IN_SECOND);
                }
        }
        if (tv->tv_sec >= 1 && tv->tv_usec < 0) {
                tv->tv_sec--;
                tv->tv_usec += USECS_IN_SECOND;
        } else if (tv->tv_sec < 0 && tv->tv_usec > 0) {
                tv->tv_sec++;
                tv->tv_usec -= USECS_IN_SECOND;
        }
}

/* Utility to return the current time of day */
void current_tv(struct timeval *tv)
{
#if defined(_WIN32) && !defined(WIN32_HIGHRES_TIME)
        struct _timeb tb;
        _ftime(&tb);
        tv->tv_sec = tb.time;
        tv->tv_usec = 1000 * tb.millitm;
#elif defined(_WIN32) && defined(WIN32_HIGHRES_TIME)
        LARGE_INTEGER ticks;
        static LARGE_INTEGER freq;
        static int first = 1;

        if (first) {
                QueryPerformanceFrequency(&freq);
                first = 0;
        }
        QueryPerformanceCounter(&ticks);
        tv->tv_sec = 0;
        tv->tv_usec = (1000000 * ticks.QuadPart / freq.QuadPart);
        normalize_tv(tv);
#else
        gettimeofday(tv,NULL);
#endif /* _WIN32 */
}

/* Queue Stuff */
Queue * createQueue(int maxElements)
{
        /* Create a Queue */
        Queue *Q;
        Q = (Queue *)malloc(sizeof(Queue));
        /* Initialise its properties */
        Q->elements = (int *)malloc(sizeof(int)*maxElements);
        Q->size = 0;
        Q->capacity = maxElements;
        Q->front = 0;
        Q->rear = -1;
        /* Return the pointer */
        return Q;
}

int front(Queue *Q)
{
        if(Q->size==0)
        {
                //printf("Queue is Empty\n");
                //exit(0);
		return 0;
        }
        /* Return the element which is at the front*/
        return Q->elements[Q->front];
}

void Enqueue(Queue *Q,int element)
{
        /* If the Queue is full, we cannot push an element into it as there is no space for it.*/
        if(Q->size == Q->capacity)
        {
                //printf("Queue is Full\n");
        }
        else
        {
                Q->size++;
                Q->rear = Q->rear + 1;
                /* As we fill the queue in circular fashion */
                if(Q->rear == Q->capacity)
                {
                        Q->rear = 0;
                }
                /* Insert the element in its rear side */
                Q->elements[Q->rear] = element;
        }
        return;
}

void Dequeue(Queue *Q)
{
        /* If Queue size is zero then it is empty. So we cannot pop */
        if(Q->size==0)
        {
                //printf("Queue is Empty\n");
                return;
        }
        /* Removing an element is equivalent to incrementing index of front by one */
        else
        {
                Q->size--;
                Q->front++;
                /* As we fill elements in circular fashion */
                if(Q->front==Q->capacity)
                {
                        Q->front=0;
                }
        }
        return;
}


static struct aio_test_data testd[MAX_AIO_CBS];

void aio_completion_handler( sigval_t sigval )
{
	int ret;
	struct aio_test_data *test;
	struct timeval end;
	double msec = 0.0;

	test = (struct aio_test_data *)sigval.sival_ptr;

	/* Did the request complete? */
	ret = aio_return( &(test->my_aiocb) );
	if (ret == test->my_aiocb.aio_nbytes) {
		/* Request completed successfully, get the return status */
		current_tv(&end);
		end.tv_sec -= test->start.tv_sec;
		end.tv_usec -= test->start.tv_usec;
		normalize_tv(&end);
		msec = (double)end.tv_sec + (double)end.tv_usec / 1000.0;

		/* if for some reason this is less than 0, we don't want a negative minimum */
		if (msec > 0)
		{
			rmed += msec;
			if (msec > rmax)
				rmax = msec;

			if (msec < rmin)
				rmin = msec;
		}
		pthread_mutex_lock(&g_lock);
		if (ret > 0)
			byteCount += ret;
		test->status = 0;
		clear += test->num_msgs;
		pthread_mutex_unlock(&g_lock);
		if (test->verbose)
                        printf("Completed. Sqn: %d. AIO: %d. Time: %.04g.\n", test->sqn, test->cur, msec);
  	}
	else
	{
		ret = aio_error( &(test->my_aiocb) );
		if (test->verbose)
			printf("Write Failed. AIO: %d. Sqn: %d. Ret: %d. Err: %u\n", test->cur, test->sqn, ret, errno);
		pthread_mutex_lock(&g_lock);
		clear += test->num_msgs;
		errors += test->num_msgs;
		test->status = 0;
		pthread_mutex_unlock(&g_lock);
	}

	/* Unlock the AIO, now available for re-use */
	pthread_mutex_unlock(&test->lock);

  	return;
}

void print_help_exit(char **argv, int exit_value){
        fprintf(stderr, "%s\n", Purpose);
        fprintf(stderr, Usage);
        exit(exit_value);
}

void process_cmdline(int argc, char **argv, struct Options *opts)
{
        int c, errflag = 0;

        memset(opts, 0, sizeof(*opts));
	/* Set Defaults */
	opts->fds = 1;
	opts->writes = 15;
	opts->reads = 15;
	opts->length = 1024;
	opts->total = 100000;
	opts->wait = 5;

        while ((c = getopt_long(argc, argv, OptionString, OptionTable, NULL)) != EOF) {
                switch (c) {
		case 'b':
			opts->batch = atoi(optarg);
			break;
		case 'd':
			opts->use_dir++;
			strncpy(opts->dir, optarg, 512);
			break;
		case 'f':
			opts->fds = atoi(optarg);
			break;
		case 'h':
			print_help_exit(argv, 1);
			break;
                case 'w':
                        opts->writes = atoi(optarg) - 1;
                        break;
                case 'r':
			opts->reads = atoi(optarg) - 1;
			break;
		case 'l':
			opts->length = atoi(optarg);
			break;
		case 'p':
			opts->pool++;
			break;
		case 't':
			opts->total = atoi(optarg);
			break;
		case 'v':
			opts->verbose++;
			break;
		case 'W':
			opts->wait = atoi(optarg);
			break;
		default:
                        errflag++;
                        break;

		}
	}

	if (errflag != 0)
        {
                /* An error occurred processing the command line - print help and exit */
                print_help_exit(argv, 1);
        }
}

void *queue_write_func(void *proc)
{
	struct queue_proc *write_proc = proc;
	int i = 0;//, cur_write;

	printf("...Write thread created.\n");

	while (clear < (write_proc->opts->total) )
        {
		pthread_mutex_lock(&write_proc->q->lock);
		//cur_write = front(write_proc->q);
		Dequeue(write_proc->q);
		pthread_mutex_unlock(&write_proc->q->lock);
		SLEEP_MSEC(100);
		i++;
	}

	printf("...Write thread exiting.\n");

	return 0;
}

void print_bytes_formatted(long int bytes)
{
	if (bytes < 1000)
		printf ("%ld bytes\n", bytes);
	if (bytes >= 1000 && bytes < 1000000)
		printf("%.2lf kbytes\n", (float) bytes / 1000);
	if (bytes >= 1000000 && bytes < 1000000000)
		printf("%.2lf mbytes\n", (float) bytes / 1000000);
	if (bytes >= 1000000000)
		printf("%.2lf gbytes\n", (float) bytes / 1000000000);
}

int fd[MAX_FDS];
struct Options *opts = &options;
char filename[FILENAME_MAX_LEN];

void  INThandler(int sig)
{
	int i;

        signal(sig, SIG_IGN);
        printf("Process Interrupted!\n");
        printf("Quitting.... deleting cache files\n");

	for (i = 0; i < opts->fds; i++)
        {
		if (!opts->use_dir)
			sprintf(filename, "%d.cache", i);
		else
                	sprintf(filename, "%s/%d.cache", opts->dir, i);

                close(fd[i]);
                unlink(filename);
        }
        exit(0);
}

int main(int argc, char **argv)
{
	int ret, i, cur_fd = 0, aio_fd_ratio, max_per_batch;
	int fd_off[MAX_FDS], fd_bat[MAX_FDS], cur_write;
	//struct Options *opts = &options;
	//char filename[FILENAME_MAX_LEN];
	struct timeval start, end;
	double sec = 0.0, rate = 0.0;
	int seed = time(NULL);
	pthread_t queue_write_thread;
	int h_queue_write;
	Queue *write_q;
	struct queue_proc write_proc;	

	write_q = createQueue(MAX_AIO_CBS);
	
	srand(seed);
	
	/* Process command line options */
        process_cmdline(argc, argv, opts);

	/* Interrupt handler for early termination */
	signal(SIGINT, INThandler);

	write_proc.q = write_q;
	write_proc.opts = opts;

	/* If pooling, we need to have at least 1 AIO per FD */
	if (opts->pool)
	{
		aio_fd_ratio = (opts->writes + 1) / opts->fds;
		if (aio_fd_ratio < 1)
		{
			printf("ERROR: you must have at least 1 AIO per FD with pool\n");
			exit(0);
	 	}
	}

	/* if using batch, must use pool */
	if (opts->batch && !opts->pool)
	{
		printf("ERROR: to use batching, you must enable pooling (-p)\n");
                exit(0);
	}

	/* calculate max number of writes per batch */
	if (opts->batch)
	{
		max_per_batch = opts->batch / opts->length;
		if (max_per_batch < 1)
		{
			printf("ERROR: you must configure the batch to be larger than each write\n");
                        exit(0);
		}
		if (max_per_batch == 1)
		{
			printf("WARNING: only a single write can fit into a batch - a larger batch size would be better\n");
		}
	}

	/* Open all files */
	printf("...Initializing FD's\n");
	for (i = 0; i < opts->fds; i++)
	{
		if (!opts->use_dir)
			sprintf(filename, "%d.cache", i);
		else
			sprintf(filename, "%s/%d.cache",opts->dir, i);
		fd[i] = open( filename, O_RDWR|O_CREAT);
		if (opts->verbose)
			printf("Opening cache file: %s\n", filename);
		if (fd < 0) 
			perror("open");
		fd_off[i] = 0;
		fd_bat[i] = 1;
	}

	/* Set up the AIO requests */
	for (i = 0; i < opts->writes; i++)
	{
        	bzero( (char *)&(testd[i].my_aiocb), sizeof(struct aiocb) );
		testd[i].status = 0;
		pthread_mutex_init(&(testd[i].lock), NULL);
	}
	
	/* Start the queue processing thread */
	h_queue_write = pthread_create(&queue_write_thread, NULL, queue_write_func, (void *) &write_proc);

	/* Init locks */
	pthread_mutex_init(&g_lock, NULL);
	pthread_mutex_init(&write_q->lock, NULL);

	printf("...Starting Test. %d total writes. %d AIO's. %d bytes per write.\n", opts->total, opts->writes + 1, opts->length);
	current_tv(&start);

	i = 0;
	while (tot_writes < opts->total)
	{
		cur_fd = rand() % opts->fds;

		if (opts->pool)
		{
			cur_write = cur_fd * aio_fd_ratio; 
		}
		else if (cur_write > opts->writes)
		{
			cur_write = 0;
		}

		while (1) {
			/* Spin until an AIO becomes available */
			if (testd[cur_write].status == 1)
			{
				if (opts->verbose > 1)
					printf("AIO still in-use: %d\n", cur_write);
				if (opts->pool)
				{
					if (opts->batch)
					{
						cur_write += 1;
						if (cur_write > cur_fd * aio_fd_ratio + aio_fd_ratio - 1)
						{
							/* Since all the AIO's are busy, we'll batch */
                                                        cur_write = cur_fd * aio_fd_ratio;
							if (fd_bat[cur_write] < max_per_batch)
							{
								if (opts->verbose)
									printf("Batch: FD %d. Writes: %d\n", cur_fd, fd_bat[cur_fd] + 1);
								fd_bat[cur_fd]++;
							}
							else
								cur_write += 1;
						}
					}
					else
					{
						cur_write += 1;
						if (cur_write > cur_fd * aio_fd_ratio + aio_fd_ratio - 1)
							cur_write = cur_fd * aio_fd_ratio;
					}
				}
				else
				{
					cur_write += 1;
					if (cur_write > opts->writes)
                        			cur_write = 0;
				}

				continue;
			}
			else
			{
				/* Grab the AIO lock here, release in callback */
				pthread_mutex_lock(&testd[cur_write].lock);

				/* Free the buffer if used previously, then zero out the struct */
				//if (testd[cur_write].sqn != 0)
				free( (void *)testd[cur_write].my_aiocb.aio_buf);
				bzero( (char *)&(testd[cur_write].my_aiocb), sizeof(struct aiocb) );

				/* Assign the message to the AIO */
				testd[cur_write].my_aiocb.aio_fildes = fd[cur_fd];
        			testd[cur_write].my_aiocb.aio_buf = malloc(opts->length * fd_bat[cur_fd]);
        			testd[cur_write].my_aiocb.aio_nbytes = opts->length * fd_bat[cur_fd];
       				testd[cur_write].my_aiocb.aio_offset = fd_off[cur_fd];

        			/* Link the AIO request with a thread callback */
        			testd[cur_write].my_aiocb.aio_sigevent.sigev_notify = SIGEV_THREAD;
        			testd[cur_write].my_aiocb.aio_sigevent.sigev_notify_function = aio_completion_handler;
        			testd[cur_write].my_aiocb.aio_sigevent.sigev_notify_attributes = NULL;
				testd[cur_write].my_aiocb.aio_sigevent.sigev_value.sival_ptr = &testd[cur_write];
			
				testd[cur_write].sqn = i + (1 * fd_bat[cur_fd]);
				testd[cur_write].status = 1;
				testd[cur_write].verbose = opts->verbose;
				testd[cur_write].cur = cur_write;
				testd[cur_write].num_msgs = fd_bat[cur_fd];
	
				/* Add a starting tv - start the write */
				current_tv(&testd[cur_write].start);
				ret = aio_write( &testd[cur_write].my_aiocb );
				if (ret != 0)
				{
					printf("aio_write() error: %s\n", strerror(errno));
				}
				tot_writes += fd_bat[cur_fd];
				fd_off[cur_fd] += opts->length * fd_bat[cur_fd];
				
				pthread_mutex_lock(&write_q->lock);
				Enqueue(write_q, cur_write);
				pthread_mutex_unlock(&write_q->lock);

				if (fd_bat[cur_fd] > 1)
				{
					i += fd_bat[cur_fd] - 1;
					fd_bat[cur_fd] = 1;
				}
				else
					i++;
				break;
			}
		}
		if (opts->verbose)
			printf("seqn: %d written. AIO: %d. Status: %d.\n", i, cur_write, testd[cur_write].status);
	
		cur_write ++;
	}

	printf("...Finished %d writes, will wait up to %d seconds for all CBs to complete\n", tot_writes, opts->wait);

	/* Wait if needed */
	i = 0;
	while (clear < (opts->total) )
	{
		if (i * 10 > opts->wait * 1000)
		{
			printf("...Not all outstanding requests completed. \n");
			break;
		}
		SLEEP_MSEC(10);
		i++;
	}

	/* Make sure write thread is done */
	pthread_join(queue_write_thread, NULL);

	current_tv(&end);
	printf("...Waited %d ms. Cleaning up.\n", i * 10);

	for (i = 0; i < opts->fds; i++)
        {
		if (!opts->use_dir)
                        sprintf(filename, "%d.cache", i);
		else
			sprintf(filename, "%s/%d.cache", opts->dir, i);

		close(fd[i]);
		unlink(filename);
	}
	
	end.tv_sec -= start.tv_sec;
        end.tv_usec -= start.tv_usec;
	normalize_tv(&end);
        sec = (double)end.tv_sec + (double)end.tv_usec / 1000000.0;
	rate = (opts->total / sec);

	printf("\nResults\n");
	printf("********************************************************************\n");
	printf("        Errors (write fails + out. req's):      %d\n", opts->total - clear + errors);
	printf("        Maximum Write Time:                     %.04g ms\n", rmax);
	printf("        Minimum Write Time:                     %.04g ms\n", rmin);
	printf("        Average Time Per Write:                 %.04g ms\n", (double) rmed / clear);	
	printf(" Total Time: %.04g seconds. Total Written: ", sec);
	print_bytes_formatted(byteCount);
	printf(" Average Writes/Second: %.2f (Write Size: %d bytes)\n", rate, opts->length);
	printf(" Average Throughput/Second: ");
	print_bytes_formatted(rate * opts->length);
	printf("********************************************************************\n");

	return 0;
}
