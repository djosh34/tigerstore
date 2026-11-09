#define _GNU_SOURCE
#include <openssl/evp.h>
#include <linux/if_alg.h>
#include <sys/socket.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#define CHECK(x) do { if(!(x)){fprintf(stderr,"line=%d %s errno=%d\n",__LINE__,#x,errno);exit(1);} }while(0)
static double ns(void){struct timespec t;clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t);return t.tv_sec*1e9+t.tv_nsec;}
int main(int argc,char **argv){CHECK(argc==2);setbuf(stdout,NULL);int alg=!strcmp(argv[1],"afalg");for(int bits=128;bits<=256;bits*=2)for(int size=256;size<=65536;size*=4){char *src=calloc(1,size+13),*dst=malloc(size+64);unsigned char key[32]={0},iv[12]={0};int fd=-1;EVP_CIPHER_CTX *ctx=NULL;char ctrl[CMSG_SPACE(sizeof(int))+CMSG_SPACE(sizeof(struct af_alg_iv)+12)+CMSG_SPACE(sizeof(int))];memset(ctrl,0,sizeof(ctrl));struct iovec iov={src,size+13};struct msghdr msg={.msg_iov=&iov,.msg_iovlen=1,.msg_control=ctrl,.msg_controllen=sizeof(ctrl)};
 if(alg){int s=socket(AF_ALG,SOCK_SEQPACKET,0);CHECK(s>=0);struct sockaddr_alg sa={.salg_family=AF_ALG};strcpy((void*)sa.salg_type,"aead");strcpy((void*)sa.salg_name,"gcm(aes)");CHECK(bind(s,(void*)&sa,sizeof(sa))==0);CHECK(setsockopt(s,SOL_ALG,ALG_SET_KEY,key,bits/8)==0);CHECK(setsockopt(s,SOL_ALG,ALG_SET_AEAD_AUTHSIZE,NULL,16)==0);fd=accept(s,NULL,NULL);CHECK(fd>=0);close(s);struct cmsghdr *cm=CMSG_FIRSTHDR(&msg);cm->cmsg_level=SOL_ALG;cm->cmsg_type=ALG_SET_OP;cm->cmsg_len=CMSG_LEN(sizeof(int));*(int*)CMSG_DATA(cm)=ALG_OP_ENCRYPT;cm=CMSG_NXTHDR(&msg,cm);cm->cmsg_level=SOL_ALG;cm->cmsg_type=ALG_SET_IV;cm->cmsg_len=CMSG_LEN(sizeof(struct af_alg_iv)+12);struct af_alg_iv *aiv=(void*)CMSG_DATA(cm);aiv->ivlen=12;cm=CMSG_NXTHDR(&msg,cm);cm->cmsg_level=SOL_ALG;cm->cmsg_type=ALG_SET_AEAD_ASSOCLEN;cm->cmsg_len=CMSG_LEN(sizeof(int));*(int*)CMSG_DATA(cm)=13;}
 else {ctx=EVP_CIPHER_CTX_new();CHECK(ctx);CHECK(EVP_EncryptInit_ex(ctx,bits==128?EVP_aes_128_gcm():EVP_aes_256_gcm(),NULL,key,iv)==1);}
 int n=16384;double t=ns();for(int i=0;i<n;i++){if(alg){struct cmsghdr *cm=CMSG_NXTHDR(&msg,CMSG_FIRSTHDR(&msg));struct af_alg_iv *aiv=(void*)CMSG_DATA(cm);memcpy(aiv->iv,&i,sizeof(i));CHECK(sendmsg(fd,&msg,0)==size+13);CHECK(read(fd,dst,size+29)==size+29);}else{int len,last;memcpy(iv,&i,sizeof(i));CHECK(EVP_EncryptInit_ex(ctx,NULL,NULL,NULL,iv)==1);CHECK(EVP_EncryptUpdate(ctx,NULL,&len,(void*)src,13)==1);CHECK(EVP_EncryptUpdate(ctx,(void*)dst,&len,(void*)src,size)==1);CHECK(EVP_EncryptFinal_ex(ctx,(void*)(dst+len),&last)==1);CHECK(EVP_CIPHER_CTX_ctrl(ctx,EVP_CTRL_GCM_GET_TAG,16,dst+size)==1);}}
 t=ns()-t;printf("AES mode=%s bits=%d size=%d cpu_ns=%.2f ns_byte=%.5f GBs=%.4f calls=%d\n",argv[1],bits,size,t/n,t/(n*size),(double)n*size/t,alg?2:0);if(ctx)EVP_CIPHER_CTX_free(ctx);if(fd>=0)close(fd);free(src);free(dst);}
 return 0;}
