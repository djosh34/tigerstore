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
#include <signal.h>
#include <poll.h>
#include <openssl/evp.h>
#define CHECK(x) do { if(!(x)){fprintf(stderr,"FAIL line=%d %s errno=%d %s\n",__LINE__,#x,errno,strerror(errno));exit(1);} }while(0)
#define NC 32
static long calls;
static double ns(clockid_t id){struct timespec t;clock_gettime(id,&t);return t.tv_sec*1e9+t.tv_nsec;}
static void pin(int cpu){cpu_set_t c;CPU_ZERO(&c);CPU_SET(cpu,&c);CHECK(sched_setaffinity(0,sizeof(c),&c)==0);}
static void *allocbuf(size_t n){void *p;CHECK(posix_memalign(&p,4096,n)==0);memset(p,42,n);return p;}
static void pair(int *a,int *b){int l=socket(AF_INET,SOCK_STREAM,0);CHECK(l>=0);struct sockaddr_in sa={.sin_family=AF_INET,.sin_addr.s_addr=htonl(INADDR_LOOPBACK)};CHECK(bind(l,(void*)&sa,sizeof(sa))==0);CHECK(listen(l,64)==0);socklen_t n=sizeof(sa);CHECK(getsockname(l,(void*)&sa,&n)==0);*a=socket(AF_INET,SOCK_STREAM,0);CHECK(connect(*a,(void*)&sa,n)==0);*b=accept(l,0,0);CHECK(*b>=0);close(l);int one=1;CHECK(setsockopt(*a,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one))==0);}
static void tls(int tx,int rx){struct tls12_crypto_info_aes_gcm_128 info={.info={.version=TLS_1_3_VERSION,.cipher_type=TLS_CIPHER_AES_GCM_128}};CHECK(setsockopt(tx,IPPROTO_TCP,TCP_ULP,"tls",4)==0);CHECK(setsockopt(rx,IPPROTO_TCP,TCP_ULP,"tls",4)==0);CHECK(setsockopt(tx,SOL_TLS,TLS_TX,&info,sizeof(info))==0);CHECK(setsockopt(rx,SOL_TLS,TLS_RX,&info,sizeof(info))==0);}
static unsigned rf(int f){return f==1?IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN|IORING_SETUP_COOP_TASKRUN:f==2?IORING_SETUP_SQPOLL|IORING_SETUP_SQ_AFF:0;}
static void ringinit(struct io_uring *r,int f){struct io_uring_params p={.flags=rf(f),.sq_thread_cpu=2,.sq_thread_idle=1000};int e=io_uring_queue_init_params(512,r,&p);if(e){fprintf(stderr,"ringinit flags=%x error=%s\n",p.flags,strerror(-e));exit(2);}}
// Count actual blocking enter calls ourselves; submit API count is exact enters except SQPOLL.
static void submit(struct io_uring *r){int e=io_uring_submit(r);CHECK(e>=0);calls++;}
static struct io_uring_cqe *get(struct io_uring *r){struct io_uring_cqe *c;while(io_uring_peek_cqe(r,&c)==-EAGAIN){int e=io_uring_enter(r->ring_fd,0,1,IORING_ENTER_GETEVENTS,NULL);calls++;CHECK(e>=0);}return c;}
struct rxresult {double cpu;long calls,cqes;unsigned long long bytes;};
static void receiver(int *rx,int size,int mode,int flags,int out,int ready){
 pin(1);struct io_uring r;struct io_uring_buf_ring *br=NULL;char *buf=allocbuf(65536),*pool=NULL;const int nb=256,bs=65536;struct pollfd p[NC];
 for(int j=0;j<NC;j++)p[j]=(struct pollfd){.fd=rx[j],.events=POLLIN};
 if(mode==2){ringinit(&r,flags);int e; br=io_uring_setup_buf_ring(&r,nb,1,0,&e);CHECK(br);pool=allocbuf((size_t)nb*bs);for(int j=0;j<nb;j++)io_uring_buf_ring_add(br,pool+(size_t)j*bs,bs,j,nb-1,j);io_uring_buf_ring_advance(br,nb);for(int j=0;j<NC;j++){struct io_uring_sqe *s=io_uring_get_sqe(&r);io_uring_prep_recv_multishot(s,rx[j],NULL,0,0);s->flags|=IOSQE_BUFFER_SELECT;s->buf_group=1;s->user_data=j;}submit(&r);}
 calls=0;struct rxresult res={0};double c=ns(CLOCK_PROCESS_CPUTIME_ID);CHECK(write(ready,"x",1)==1);close(ready);int live=NC;
 if(mode==2){while(live){struct io_uring_cqe *q=get(&r);int n=q->res;unsigned fl=q->flags;int j=q->user_data;res.cqes++;io_uring_cqe_seen(&r,q);if(n>0)res.bytes+=n;if(fl&IORING_CQE_F_BUFFER){int bid=fl>>IORING_CQE_BUFFER_SHIFT;io_uring_buf_ring_add(br,pool+(size_t)bid*bs,bs,bid,nb-1,0);io_uring_buf_ring_advance(br,1);}if(!(fl&IORING_CQE_F_MORE)){if(n==0){close(rx[j]);live--;}else {CHECK(n>0 || n==-ENOBUFS);struct io_uring_sqe *s=io_uring_get_sqe(&r);io_uring_prep_recv_multishot(s,rx[j],NULL,0,0);s->flags|=IOSQE_BUFFER_SELECT;s->buf_group=1;s->user_data=j;submit(&r);}}}}
 else {while(live){calls++;CHECK(poll(p,NC,-1)>0);for(int j=0;j<NC;j++)if(p[j].revents){calls++;ssize_t n=read(p[j].fd,buf,mode==0?size:65536);CHECK(n>=0);if(n==0){close(p[j].fd);p[j].fd=-1;live--;}else res.bytes+=n;}}}
 res.cpu=ns(CLOCK_PROCESS_CPUTIME_ID)-c;res.calls=calls;CHECK(write(out,&res,sizeof(res))==sizeof(res));_exit(0);
}
static void drainzc(int *tx,long *done,long *copied,long *errcalls){for(int j=0;j<NC;j++)for(;;){char ctrl[512],v;struct iovec iov={&v,1};struct msghdr m={.msg_iov=&iov,.msg_iovlen=1,.msg_control=ctrl,.msg_controllen=sizeof(ctrl)};(*errcalls)++;if(recvmsg(tx[j],&m,MSG_ERRQUEUE|MSG_DONTWAIT)<0){CHECK(errno==EAGAIN);break;}for(struct cmsghdr *cm=CMSG_FIRSTHDR(&m);cm;cm=CMSG_NXTHDR(&m,cm))if(cm->cmsg_level==SOL_IP && cm->cmsg_type==IP_RECVERR){struct sock_extended_err *e=(void*)CMSG_DATA(cm);if(e->ee_origin==SO_EE_ORIGIN_ZEROCOPY){long n=e->ee_data-e->ee_info+1;*done+=n;if(e->ee_code==SO_EE_CODE_ZEROCOPY_COPIED)*copied+=n;}}}}
// modes: write, writev32, bigwrite32, uring, fixed, send_zc, send_zc_fixed, msgzc, sendfile, splice, evp+write.
static const char *mn[]={"write","writev32","bigwrite32","uring","fixed","send_zc","send_zc_fixed","msgzc","sendfile","splice","evp_write"};
static void nettest(int size,int mode,int batch,int flags,int ktls,int rmode){
 int tx[NC],rx[NC],out[2],ready[2];for(int j=0;j<NC;j++){pair(&tx[j],&rx[j]);if(ktls)tls(tx[j],rx[j]);if(ktls==2){int one=1;CHECK(setsockopt(tx[j],SOL_TLS,TLS_TX_ZEROCOPY_RO,&one,sizeof(one))==0);}}CHECK(pipe(out)==0);CHECK(pipe(ready)==0);
 pid_t child=fork();CHECK(child>=0);if(!child){for(int j=0;j<NC;j++)close(tx[j]);close(out[0]);close(ready[0]);receiver(rx,size,rmode,flags,out[1],ready[1]);}
 for(int j=0;j<NC;j++)close(rx[j]);close(out[1]);close(ready[1]);char v;CHECK(read(ready[0],&v,1)==1);close(ready[0]);
 struct io_uring r;int ur=mode>=3&&mode<=6,zc=mode==5||mode==6;char *buf=allocbuf((size_t)size*32+64);if(ur){ringinit(&r,flags);if(mode==4||mode==6){struct iovec iov={buf,(size_t)size*32+64};int e=io_uring_register_buffers(&r,&iov,1);CHECK(e==0);}}
 if(mode==7){int one=1;for(int j=0;j<NC;j++)CHECK(setsockopt(tx[j],SOL_SOCKET,SO_ZEROCOPY,&one,sizeof(one))==0);}
 int file=-1,pp[2];if(mode==8||mode==9){file=memfd_create("payload",0);CHECK(file>=0);CHECK(write(file,buf,size)==size);if(mode==9){CHECK(pipe(pp)==0);}}
 EVP_CIPHER_CTX *evp=NULL;unsigned char key[16]={0},iv[12]={0};char *encrypted=NULL;if(mode==10){evp=EVP_CIPHER_CTX_new();CHECK(evp);CHECK(EVP_EncryptInit_ex(evp,EVP_aes_128_gcm(),NULL,key,iv)==1);encrypted=allocbuf(size+64);}
 // Equal message count per connection, enough calls to time small sizes; immutable source until all notifications.
 int messages=getenv("SHORT")?1024:((16<<20)/size);if(messages<4096&&!getenv("SHORT"))messages=4096;messages=(messages+1023)/1024*1024;
 int chunk=(mode==1||mode==2)?32:1,groups=messages/(chunk*batch);long sendcalls=0,shorts=0,zc_done=0,copied=0,zc_sends=0,errcalls=0,notif=0,notcopied=0,neednotif=0;calls=0;double t=ns(CLOCK_MONOTONIC),c=ns(CLOCK_PROCESS_CPUTIME_ID),thread=ns(CLOCK_THREAD_CPUTIME_ID);
 for(int i=0;i<groups;i++){
  if(ur){int remain[NC],offset[NC],pending=batch;for(int j=0;j<batch;j++){remain[j]=size;offset[j]=0;}while(pending){int n=0;for(int j=0;j<batch;j++)if(remain[j]){int fd=tx[(i*batch+j)%NC];struct io_uring_sqe *s=io_uring_get_sqe(&r);CHECK(s);if(zc){if(mode==6)io_uring_prep_send_zc_fixed(s,fd,buf+offset[j],remain[j],MSG_NOSIGNAL,IORING_SEND_ZC_REPORT_USAGE,0);else io_uring_prep_send_zc(s,fd,buf+offset[j],remain[j],MSG_NOSIGNAL,IORING_SEND_ZC_REPORT_USAGE);}else {io_uring_prep_send(s,fd,buf+offset[j],remain[j],MSG_NOSIGNAL);if(mode==4){s->ioprio|=IORING_RECVSEND_FIXED_BUF;s->buf_index=0;}}s->user_data=j;n++;}submit(&r);int completions=0;while(completions<n){struct io_uring_cqe *q=get(&r);int res=q->res,j=q->user_data;unsigned fl=q->flags;io_uring_cqe_seen(&r,q);if(fl&IORING_CQE_F_NOTIF){notif++;if(!(res&IORING_NOTIF_USAGE_ZC_COPIED))notcopied++;continue;}if(res<=0){fprintf(stderr,"CQE res=%d error=%s mode=%s\n",res,res<0?strerror(-res):"zero",mn[mode]);exit(1);}completions++;if(fl&IORING_CQE_F_MORE)neednotif++;if(res<remain[j])shorts++;remain[j]-=res;offset[j]+=res;CHECK(remain[j]>=0);}pending=0;for(int j=0;j<batch;j++)if(remain[j])pending++;}}
  else for(int j=0;j<batch;j++){int fd=tx[(i*batch+j)%NC];int total=size*chunk,off=0;char *src=buf;if(mode==10){int len,fin;uint64_t nonce=(uint64_t)i*batch+j;memcpy(iv,&nonce,8);CHECK(EVP_EncryptInit_ex(evp,NULL,NULL,NULL,iv)==1);CHECK(EVP_EncryptUpdate(evp,(void*)encrypted,&len,(void*)buf,size)==1);CHECK(EVP_EncryptFinal_ex(evp,(void*)(encrypted+len),&fin)==1);CHECK(EVP_CIPHER_CTX_ctrl(evp,EVP_CTRL_GCM_GET_TAG,16,encrypted+size)==1);src=encrypted;total+=16;}
   while(off<total){ssize_t n;if(mode==8){off_t pos=off;n=sendfile(fd,file,&pos,total-off);}else if(mode==9){off_t pos=off;int left=total-off;ssize_t k=splice(file,&pos,pp[1],NULL,left,0);CHECK(k>0);n=0;while(n<k){ssize_t z=splice(pp[0],NULL,fd,NULL,k-n,0);CHECK(z>0);n+=z;sendcalls++;}sendcalls++;}else if(mode==1&&off==0){struct iovec vs[32];for(int k=0;k<32;k++)vs[k]=(struct iovec){src+(size_t)k*size,size};n=writev(fd,vs,32);}else if(mode==7){n=send(fd,src+off,total-off,MSG_ZEROCOPY|MSG_NOSIGNAL);zc_sends++;}else n=write(fd,src+off,total-off);CHECK(n>0);if(mode!=9)sendcalls++;if(n<total-off)shorts++;off+=n;}}
  if(mode==7 && (i%16==0))drainzc(tx,&zc_done,&copied,&errcalls);
 }
 if(zc)while(notif<neednotif){struct io_uring_cqe *q=get(&r);CHECK(q->flags&IORING_CQE_F_NOTIF);notif++;if(!(q->res&IORING_NOTIF_USAGE_ZC_COPIED))notcopied++;io_uring_cqe_seen(&r,q);}
 if(mode==7){int tries=0;while(zc_done<zc_sends&&tries++<10000){drainzc(tx,&zc_done,&copied,&errcalls);if(zc_done<zc_sends)usleep(100);}CHECK(zc_done==zc_sends);}
 double cpu=ns(CLOCK_PROCESS_CPUTIME_ID)-c,wall=ns(CLOCK_MONOTONIC)-t;thread=ns(CLOCK_THREAD_CPUTIME_ID)-thread;long enters=calls;for(int j=0;j<NC;j++)close(tx[j]);struct rxresult rr;CHECK(read(out[0],&rr,sizeof(rr))==sizeof(rr));int status;CHECK(waitpid(child,&status,0)==child);CHECK(status==0);unsigned long long expected=(unsigned long long)messages*(size+(mode==10?16:0));CHECK(rr.bytes==expected);
 printf("NET mode=%s size=%d batch=%d flags=%d tls=%d rx=%d tx_pid=%d rx_pid=%d messages=%d tx_ns=%.2f rx_ns=%.2f wall_ns=%.2f issuer_ns=%.2f tx_calls=%.5f rx_calls=%.5f shorts=%ld zc_copied=%ld zc_done=%ld zc_notif=%ld zc_notcopied=%ld rx_cqes=%ld\n",mn[mode],size,batch,flags,ktls,rmode,getpid(),child,messages,cpu/messages,rr.cpu/messages,wall/messages,thread/messages,(sendcalls+errcalls+enters)/(double)messages,rr.calls/(double)messages,shorts,copied,zc_done,notif,notcopied,rr.cqes);
 if(ur)io_uring_queue_exit(&r);if(evp)EVP_CIPHER_CTX_free(evp);free(encrypted);free(buf);if(file>=0)close(file);if(mode==9){close(pp[0]);close(pp[1]);}close(out[0]);
}
static void disk(char *path,int size,int mode,int direct){int fd=open(path,O_CREAT|O_RDWR|O_TRUNC|(direct?O_DIRECT:0),0600);CHECK(fd>=0);const int span=64<<20;CHECK(posix_fallocate(fd,0,span)==0);char *buf=allocbuf(size);CHECK(pwrite(fd,buf,size,0)==size);CHECK(fdatasync(fd)==0);struct io_uring r;if(mode){ringinit(&r,0);if(mode>=2){struct iovec v={buf,size};CHECK(io_uring_register_buffers(&r,&v,1)==0);}if(mode==3)CHECK(io_uring_register_files(&r,&fd,1)==0);}
 int groups=128;calls=0;double cpu=ns(CLOCK_PROCESS_CPUTIME_ID),wall=ns(CLOCK_MONOTONIC),thread=ns(CLOCK_THREAD_CPUTIME_ID);for(int i=0;i<groups;i++){off_t pos=(i*size)%(span-size+1);pos=pos/4096*4096;if(!mode){CHECK(pwrite(fd,buf,size,pos)==size);CHECK(fdatasync(fd)==0);calls+=2;}else{struct io_uring_sqe *s=io_uring_get_sqe(&r);if(mode>=2)io_uring_prep_write_fixed(s,mode==3?0:fd,buf,size,pos,0);else io_uring_prep_write(s,fd,buf,size,pos);s->flags|=IOSQE_IO_LINK;if(mode==3)s->flags|=IOSQE_FIXED_FILE;s->user_data=1;s=io_uring_get_sqe(&r);io_uring_prep_fsync(s,mode==3?0:fd,IORING_FSYNC_DATASYNC);if(mode==3)s->flags|=IOSQE_FIXED_FILE;s->user_data=2;CHECK(io_uring_submit_and_wait(&r,2)>=0);calls++;for(int k=0;k<2;k++){struct io_uring_cqe *q=get(&r);CHECK(q->res==(q->user_data==1?size:0));io_uring_cqe_seen(&r,q);}}}
 cpu=ns(CLOCK_PROCESS_CPUTIME_ID)-cpu;wall=ns(CLOCK_MONOTONIC)-wall;thread=ns(CLOCK_THREAD_CPUTIME_ID)-thread;printf("DISK size=%d mode=%d direct=%d groups=%d cpu_ns=%.2f issuer_ns=%.2f wall_ns=%.2f calls=%.5f\n",size,mode,direct,groups,cpu/groups,thread/groups,wall/groups,calls/(double)groups);if(mode)io_uring_queue_exit(&r);free(buf);close(fd);unlink(path);}
static void config(void){int tx,rx;pair(&tx,&rx);tls(tx,rx);int one=1;int z=setsockopt(tx,SOL_TLS,TLS_TX_ZEROCOPY_RO,&one,sizeof(one));printf("KTLS TLS_TX_ZEROCOPY_RO ret=%d errno=%d\n",z,z?errno:0);close(tx);close(rx);struct io_uring_params p={0};struct io_uring r;int e=io_uring_queue_init_params(512,&r,&p);printf("CONFIG init=%d features=0x%x sq=%u cq=%u\n",e,p.features,p.sq_entries,p.cq_entries);CHECK(e==0);struct io_uring_probe *pr=io_uring_get_probe_ring(&r);for(int op=0;pr&&op<=pr->last_op;op++)if(io_uring_opcode_supported(pr,op))printf("OP %d\n",op);io_uring_free_probe(pr);double c=ns(CLOCK_PROCESS_CPUTIME_ID);for(int i=0;i<1000000;i++)syscall(SYS_getpid);printf("GETPID ns=%.2f\n",(ns(CLOCK_PROCESS_CPUTIME_ID)-c)/1e6);for(int b=1;b<=32;b=(b==1?8:32)){calls=0;c=ns(CLOCK_PROCESS_CPUTIME_ID);for(int i=0;i<8192;i++){for(int j=0;j<b;j++)io_uring_prep_nop(io_uring_get_sqe(&r));submit(&r);for(int j=0;j<b;j++){struct io_uring_cqe *q=get(&r);CHECK(q->res==0);io_uring_cqe_seen(&r,q);}}printf("NOP batch=%d ns=%.2f calls=%.5f\n",b,(ns(CLOCK_PROCESS_CPUTIME_ID)-c)/(8192*b),calls/(8192.0*b));if(b==32)break;}io_uring_queue_exit(&r);}
int main(int argc,char **argv){setbuf(stdout,NULL);pin(0);alarm(45);if(argc==2&&!strcmp(argv[1],"config"))config();else if(argc==9&&!strcmp(argv[1],"net"))nettest(atoi(argv[2]),atoi(argv[3]),atoi(argv[4]),atoi(argv[5]),atoi(argv[6]),atoi(argv[7]));else if(argc==6&&!strcmp(argv[1],"disk"))disk(argv[2],atoi(argv[3]),atoi(argv[4]),atoi(argv[5]));else{fprintf(stderr,"usage: config | net size mode batch flags tls rx unused | disk path size mode direct\n");return 1;}return 0;}
