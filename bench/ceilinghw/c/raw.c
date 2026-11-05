#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sched.h>
#include <errno.h>
static double now(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return t.tv_sec+t.tv_nsec/1e9;}
static void check(int ok){if(!ok){perror("raw");exit(1);}}
static void pin(int n){cpu_set_t c;CPU_ZERO(&c);CPU_SET(n,&c);check(!pthread_setaffinity_np(pthread_self(),sizeof(c),&c));}
struct job{int id,bs,direct,append,fsync;const char *dir,*mode;double duration,mbps,p50,p99,p999;uint64_t count;};
static int cmp(const void *a,const void*b){double x=*(double*)a,y=*(double*)b;return (x>y)-(x<y);}
static void *disk(void *arg){struct job*j=arg;pin(j->id);char path[4096];snprintf(path,sizeof(path),"%s/%s.%d",j->dir,j->append?"cycleappend":"pre",j->id);int flags=O_WRONLY|(j->direct?O_DIRECT:0)|(j->append?O_CREAT|O_TRUNC:0);int fd=open(path,flags,0600);check(fd>=0);void *buf;check(!posix_memalign(&buf,4096,j->bs));memset(buf,13,j->bs);size_t cap=1000000;double *lat=malloc(cap*sizeof(double));check(lat!=NULL);double start=now();off_t off=0;uint64_t n=0;while(now()-start<j->duration){if(!j->append&&off+j->bs>(1LL<<30))off=0;double t=now();check(pwrite(fd,buf,j->bs,off)==j->bs);check(!(j->fsync?fsync(fd):fdatasync(fd)));double d=now()-t;if(n<cap)lat[n]=d*1e6;n++;off+=j->bs;}double elapsed=now()-start;check(n>0&&n<=cap);qsort(lat,n,sizeof(double),cmp);j->count=n;j->mbps=n*(double)j->bs/(1<<20)/elapsed;j->p50=lat[n/2];j->p99=lat[(n-1)*99/100];j->p999=lat[(n-1)*999/1000];close(fd);if(j->append)unlink(path);free(buf);free(lat);return NULL;}
static void *memory(void *arg){struct job*j=arg;pin(j->id);size_t n=256UL<<20;char*a,*b;check(!posix_memalign((void**)&a,64,n));check(!posix_memalign((void**)&b,64,n));memset(a,1,n);memset(b,0,n);double start=now();uint64_t count=0;while(now()-start<j->duration){if(!strcmp(j->mode,"copy")){memcpy(b,a,n);asm volatile(""::"r"(b):"memory");}else{memset(b,(int)count,n);asm volatile(""::"r"(b):"memory");}count++;}j->count=count;j->mbps=count*(double)n/(1<<20)/(now()-start);free(a);free(b);return NULL;}
int main(int argc,char**argv){check(argc>=4);int threads=atoi(argv[2]);check(threads>=1&&threads<=4);struct job j[4]={0};pthread_t t[4];int mem=!strcmp(argv[1],"memory");for(int i=0;i<threads;i++){j[i].id=i;j[i].duration=mem?3:2;if(mem){j[i].mode=argv[3];}else{check(argc==8);j[i].dir=argv[3];j[i].bs=atoi(argv[4]);j[i].direct=atoi(argv[5]);j[i].append=atoi(argv[6]);j[i].fsync=atoi(argv[7]);}check(!pthread_create(&t[i],NULL,mem?memory:disk,&j[i]));}double total=0;for(int i=0;i<threads;i++){check(!pthread_join(t[i],NULL));total+=j[i].mbps;if(!mem)printf("{\"writer\":%d,\"writes\":%lu,\"mib_per_s\":%.3f,\"p50_us\":%.3f,\"p99_us\":%.3f,\"p999_us\":%.3f}\n",i,j[i].count,j[i].mbps,j[i].p50,j[i].p99,j[i].p999);}if(mem)printf("{\"mode\":\"%s\",\"threads\":%d,\"useful_mib_per_s\":%.3f,\"minimum_traffic_mib_per_s\":%.3f}\n",argv[3],threads,total,total*(!strcmp(argv[3],"copy")?2:1));else printf("{\"total_mib_per_s\":%.3f}\n",total);return 0;}
