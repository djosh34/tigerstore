#define _GNU_SOURCE
#include <liburing.h>
#include <linux/errqueue.h>
#include <linux/tls.h>
#include <linux/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sched.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <poll.h>
#define CHECK(x) do { if(!(x)){perror(#x);exit(1);} }while(0)
static double clockns(clockid_t id){struct timespec t;clock_gettime(id,&t);return t.tv_sec*1e9+t.tv_nsec;}
static void pin(int cpu){cpu_set_t c;CPU_ZERO(&c);CPU_SET(cpu,&c);CHECK(sched_setaffinity(0,sizeof(c),&c)==0);}
static void pair(int *a,int *b){int l=socket(AF_INET,SOCK_STREAM,0);CHECK(l>=0);struct sockaddr_in sa={.sin_family=AF_INET,.sin_addr.s_addr=htonl(INADDR_LOOPBACK)};CHECK(bind(l,(void*)&sa,sizeof(sa))==0);CHECK(listen(l,64)==0);socklen_t n=sizeof(sa);CHECK(getsockname(l,(void*)&sa,&n)==0);*a=socket(AF_INET,SOCK_STREAM,0);CHECK(connect(*a,(void*)&sa,n)==0);*b=accept(l,0,0);CHECK(*b>=0);close(l);int one=1;setsockopt(*a,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));}
static void complete(struct io_uring *r,int n,int expected){for(int j=0;j<n;j++){struct io_uring_cqe *c;int e=io_uring_wait_cqe(r,&c);if(e<0 || c->res!=expected){fprintf(stderr,"completion e=%d res=%d expected=%d\n",e,e<0?0:c->res,expected);exit(1);}io_uring_cqe_seen(r,c);}}
static void nettest(int size,int batch,int mode,struct io_uring *r){
 int tx[32],rx[32];for(int j=0;j<32;j++)pair(&tx[j],&rx[j]);
 pid_t child=fork();CHECK(child>=0);
 if(!child){pin(1);struct pollfd p[32];for(int j=0;j<32;j++){close(tx[j]);p[j]=(struct pollfd){.fd=rx[j],.events=POLLIN};}char *buf=malloc(1<<20);int live=32;while(live){CHECK(poll(p,32,-1)>0);for(int j=0;j<32;j++){if(p[j].revents){ssize_t n=read(p[j].fd,buf,1<<20);CHECK(n>=0);if(n==0){close(p[j].fd);p[j].fd=-1;live--;}}}}_exit(0);}
 for(int j=0;j<32;j++)close(rx[j]);
 char *buf;CHECK(posix_memalign((void*)&buf,4096,size)==0);memset(buf,42,size);
 if(mode==2){int one=1;for(int j=0;j<32;j++)CHECK(setsockopt(tx[j],SOL_SOCKET,SO_ZEROCOPY,&one,sizeof(one))==0);}
 int groups=(32*1024*1024)/(size*batch);long long calls=0,copied=0,events=0;double t=clockns(CLOCK_MONOTONIC),cpu=clockns(CLOCK_PROCESS_CPUTIME_ID);
 for(int i=0;i<groups;i++){
  for(int j=0;j<batch;j++){
   if(mode==1){struct io_uring_sqe *s=io_uring_get_sqe(r);CHECK(s);io_uring_prep_send(s,tx[j],buf,size,MSG_NOSIGNAL);}
   else {ssize_t n=send(tx[j],buf,size,MSG_NOSIGNAL|(mode==2?MSG_ZEROCOPY:0));CHECK(n==size);calls++;}
  }
  if(mode==1){CHECK(io_uring_submit_and_wait(r,batch)>=0);calls++;complete(r,batch,size);}
 }
 double cp=clockns(CLOCK_PROCESS_CPUTIME_ID)-cpu,wall=clockns(CLOCK_MONOTONIC)-t;
 // Every message references the same immutable buffer until close; never reuse it before completion.
 if(mode==2){for(int j=0;j<batch;j++){unsigned done=0;int tries=0;while(done<(unsigned)groups && tries++<10000){char ctrl[512],x;struct iovec iov={&x,1};struct msghdr msg={.msg_iov=&iov,.msg_iovlen=1,.msg_control=ctrl,.msg_controllen=sizeof(ctrl)};if(recvmsg(tx[j],&msg,MSG_ERRQUEUE)<0){CHECK(errno==EAGAIN);usleep(1000);continue;}for(struct cmsghdr *cm=CMSG_FIRSTHDR(&msg);cm;cm=CMSG_NXTHDR(&msg,cm)){if(cm->cmsg_level==SOL_IP && cm->cmsg_type==IP_RECVERR){struct sock_extended_err *e=(void*)CMSG_DATA(cm);if(e->ee_origin==SO_EE_ORIGIN_ZEROCOPY){unsigned k=e->ee_data-e->ee_info+1;done+=k;events+=k;if(e->ee_code==SO_EE_CODE_ZEROCOPY_COPIED)copied+=k;}}}}CHECK(done==(unsigned)groups);}}
 printf("net mode=%s size=%d batch=%d sends=%d sender_cpu_ns/send=%.1f wall_GB/s=%.3f submit_or_send_calls=%lld copied_completions=%lld/%lld\n",mode==0?"send":mode==1?"uring_send":"MSG_ZEROCOPY",size,batch,groups*batch,cp/(groups*batch),(double)groups*batch*size/wall,calls,copied,events);
 for(int j=0;j<32;j++)close(tx[j]);int status;waitpid(child,&status,0);CHECK(status==0);free(buf);
}
static void disktest(char *path,int batch,int mode,int direct,struct io_uring *r){
 int fd=open(path,O_CREAT|O_RDWR|O_TRUNC|(direct?O_DIRECT:0),0600);CHECK(fd>=0);CHECK(posix_fallocate(fd,0,1<<20)==0);
 char *buf;CHECK(posix_memalign((void*)&buf,4096,1<<20)==0);memset(buf,7,1<<20);CHECK(pwrite(fd,buf,1<<20,0)==1<<20);CHECK(fdatasync(fd)==0);
 struct iovec v={buf,1<<20};if(mode==2){int e=io_uring_register_buffers(r,&v,1);if(e<0){printf("register_buffers failed=%s\n",strerror(-e));close(fd);free(buf);return;}}
 double t=clockns(CLOCK_MONOTONIC),cpu=clockns(CLOCK_PROCESS_CPUTIME_ID);int groups=64;
 for(int i=0;i<groups;i++){
  for(int j=0;j<batch;j++){
   if(mode==0)CHECK(pwrite(fd,buf+j*4096,4096,j*4096)==4096);
   else {struct io_uring_sqe *s=io_uring_get_sqe(r);CHECK(s);if(mode==2)io_uring_prep_write_fixed(s,fd,buf+j*4096,4096,j*4096,0);else io_uring_prep_write(s,fd,buf+j*4096,4096,j*4096);}
  }
  if(mode){CHECK(io_uring_submit_and_wait(r,batch)>=0);complete(r,batch,4096);struct io_uring_sqe *s=io_uring_get_sqe(r);io_uring_prep_fsync(s,fd,IORING_FSYNC_DATASYNC);CHECK(io_uring_submit_and_wait(r,1)>=0);complete(r,1,0);}else CHECK(fdatasync(fd)==0);
 }
 double cp=clockns(CLOCK_PROCESS_CPUTIME_ID)-cpu,wall=clockns(CLOCK_MONOTONIC)-t;
 printf("disk mode=%s direct=%d batch4k=%d groups=%d issuer_cpu_us/group=%.3f durable_us/group=%.3f MiB/s=%.2f\n",mode==0?"pwrite+fdatasync":mode==1?"uring":"uring_fixed",direct,batch,groups,cp/groups/1e3,wall/groups/1e3,groups*batch*4096.0/(wall/1e9)/(1<<20));
 if(mode==2)io_uring_unregister_buffers(r);close(fd);unlink(path);free(buf);
}
static void ktls(){int tx,rx;pair(&tx,&rx);int e=setsockopt(tx,IPPROTO_TCP,TCP_ULP,"tls",sizeof("tls"));printf("ktls TCP_ULP ret=%d errno=%d\n",e,e?errno:0);if(e==0){struct tls12_crypto_info_aes_gcm_128 info={.info={.version=TLS_1_3_VERSION,.cipher_type=TLS_CIPHER_AES_GCM_128}};e=setsockopt(tx,SOL_TLS,TLS_TX,&info,sizeof(info));printf("ktls TLS1.3 AES128 TX ret=%d errno=%d\n",e,e?errno:0);}close(tx);close(rx);}
int main(int argc,char **argv){setbuf(stdout,0);CHECK(argc==2);pin(0);ktls();double t=clockns(CLOCK_PROCESS_CPUTIME_ID);for(int i=0;i<1000000;i++)syscall(SYS_getpid);printf("getpid_cpu_ns=%.1f\n",(clockns(CLOCK_PROCESS_CPUTIME_ID)-t)/1e6);
 struct io_uring r;int e=io_uring_queue_init(512,&r,0);printf("io_uring_queue_init=%d (%s)\n",e,e<0?strerror(-e):"OK");int have=e==0;
 if(have){for(int batch=1;batch<=32;batch*=8){t=clockns(CLOCK_PROCESS_CPUTIME_ID);for(int i=0;i<8192;i++){for(int j=0;j<batch;j++)io_uring_prep_nop(io_uring_get_sqe(&r));CHECK(io_uring_submit_and_wait(&r,batch)>=0);complete(&r,batch,0);}printf("uring_nop batch=%d cpu_ns/op=%.1f\n",batch,(clockns(CLOCK_PROCESS_CPUTIME_ID)-t)/(8192*batch));}t=clockns(CLOCK_PROCESS_CPUTIME_ID);for(int i=0;i<8192;i++){for(int j=0;j<32;j++)io_uring_prep_nop(io_uring_get_sqe(&r));CHECK(io_uring_submit_and_wait(&r,32)>=0);complete(&r,32,0);}printf("uring_nop batch=32 cpu_ns/op=%.1f\n",(clockns(CLOCK_PROCESS_CPUTIME_ID)-t)/(8192*32));}
 for(int size=1024;size<=65536;size*= (size==1024?16:4)){nettest(size,1,0,&r);nettest(size,32,0,&r);if(have){nettest(size,1,1,&r);nettest(size,32,1,&r);}nettest(size,32,2,&r);}
 for(int direct=0;direct<=1;direct++){for(int batch=1;batch<=256;batch*=16){disktest(argv[1],batch,0,direct,&r);if(have){disktest(argv[1],batch,1,direct,&r);disktest(argv[1],batch,2,direct,&r);}}}
 if(have)io_uring_queue_exit(&r);return 0;}
