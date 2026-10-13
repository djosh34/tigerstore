// Runner-only ptrace clock wrapper. Product executable bytes are unchanged.
// Clear AT_SYSINFO_EHDR in the child's initial auxv so Go uses real syscalls,
// then offset only successful CLOCK_REALTIME results. Monotonic stays native.
#define _GNU_SOURCE
#include <sys/ptrace.h>
#include <linux/ptrace.h>
#include <sys/wait.h>
#include <sys/uio.h>
#include <sys/user.h>
#include <sys/syscall.h>
#include <elf.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#ifdef __aarch64__
#include <asm/ptrace.h>
#endif
static const char *offset_path;
static FILE *evidence;
struct pending { pid_t tid; uintptr_t addr; int active; } slots[4096];
static void die(const char *s) { perror(s); exit(2); }
static unsigned long peek(pid_t p, uintptr_t addr) {
    errno=0; long v=ptrace(PTRACE_PEEKDATA,p,(void*)addr,0);
    if(errno) die("peek");
    return (unsigned long)v;
}
static void poke(pid_t p, uintptr_t addr, unsigned long v) {
    if(ptrace(PTRACE_POKEDATA,p,(void*)addr,(void*)v)) die("poke");
}
static struct pending *slot(pid_t tid) {
    for(int i=0;i<4096;i++) if(slots[i].tid==tid) return &slots[i];
    for(int i=0;i<4096;i++) if(!slots[i].tid) { slots[i].tid=tid; return &slots[i]; }
    errno=E2BIG; die("threads"); return 0;
}
static void auxv(pid_t tid) {
#ifdef __x86_64__
    struct user_regs_struct regs;
#else
    struct user_pt_regs regs;
#endif
    struct iovec io={&regs,sizeof(regs)};
    if(ptrace(PTRACE_GETREGSET,tid,(void*)NT_PRSTATUS,&io)) die("regs");
#ifdef __x86_64__
    uintptr_t p=regs.rsp;
#else
    uintptr_t p=regs.sp;
#endif
    unsigned long argc=peek(tid,p); p += sizeof(long)*(argc+2);
    while(peek(tid,p)) p+=sizeof(long);
    p+=sizeof(long); int found=0;
    while(peek(tid,p)!=AT_NULL) {
        if(peek(tid,p)==AT_SYSINFO_EHDR) {
            fprintf(evidence,"{\"event\":\"auxv\",\"tid\":%d,\"original\":%lu,\"replacement\":0}\n",tid,peek(tid,p+sizeof(long)));
            poke(tid,p+sizeof(long),0); found=1;
        }
        p+=2*sizeof(long);
    }
    if(!found) { errno=ENOENT; die("AT_SYSINFO_EHDR"); }
}
static long offset(void) {
    FILE *f=fopen(offset_path,"r"); long n=0;
    if(!f || fscanf(f,"%ld",&n)!=1) die("offset");
    fclose(f); return n;
}
int main(int argc,char **argv) {
    if(argc<4) { fprintf(stderr,"clockwrap OFFSET EVIDENCE EXEC ARGS...\n"); return 2; }
    if(!getenv("GITHUB_ACTIONS") || strcmp(getenv("GITHUB_ACTIONS"),"true")) return 2;
    offset_path=argv[1]; evidence=fopen(argv[2],"a"); if(!evidence) die("evidence");
    setvbuf(evidence,0,_IOLBF,0);
    pid_t child=fork(); if(child<0) die("fork");
    if(child==0) { if(ptrace(PTRACE_TRACEME,0,0,0)) die("traceme"); raise(SIGSTOP); execvp(argv[3],argv+3); die("exec"); }
    int st; if(waitpid(child,&st,0)<0) die("wait");
    long opts=PTRACE_O_TRACESYSGOOD|PTRACE_O_TRACECLONE|PTRACE_O_TRACEEXEC|PTRACE_O_EXITKILL;
    if(ptrace(PTRACE_SETOPTIONS,child,0,(void*)opts)) die("options");
    if(ptrace(PTRACE_SYSCALL,child,0,0)) die("resume");
    while(1) {
        pid_t tid=waitpid(-1,&st,__WALL); if(tid<0) { if(errno==EINTR) continue; die("wait all"); }
        struct pending *s=slot(tid);
        if(WIFEXITED(st)||WIFSIGNALED(st)) {
            s->tid=0; s->active=0;
            if(tid==child) return WIFEXITED(st)?WEXITSTATUS(st):128+WTERMSIG(st);
            continue;
        }
        int sig=WSTOPSIG(st), event=st>>16, deliver=0;
        if(event==PTRACE_EVENT_EXEC) auxv(tid);
        else if(sig==(SIGTRAP|0x80)) {
            struct ptrace_syscall_info info={0};
            if(ptrace(PTRACE_GET_SYSCALL_INFO,tid,sizeof(info),&info)<0) die("syscall info");
            if(info.op==PTRACE_SYSCALL_INFO_ENTRY) {
                s->active=info.entry.nr==SYS_clock_gettime && info.entry.args[0]==CLOCK_REALTIME;
                s->addr=info.entry.args[1];
            } else if(info.op==PTRACE_SYSCALL_INFO_EXIT && s->active) {
                s->active=0;
                if(info.exit.rval==0) {
                    long before=(long)peek(tid,s->addr), delta=offset();
                    if(delta) poke(tid,s->addr,(unsigned long)(before+delta));
                    fprintf(evidence,"{\"event\":\"realtime\",\"tid\":%d,\"native_sec\":%ld,\"offset_sec\":%ld,\"returned_sec\":%ld}\n",tid,before,delta,before+delta);
                }
            }
        } else if(sig!=SIGTRAP && sig!=SIGSTOP) deliver=sig;
        if(ptrace(PTRACE_SYSCALL,tid,0,(void*)(long)deliver) && errno!=ESRCH) die("continue");
    }
}
