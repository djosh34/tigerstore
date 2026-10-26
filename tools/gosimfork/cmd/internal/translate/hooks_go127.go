package translate

var hooksGo127 = map[packageSelector]packageSelector{
	{Pkg: "golang.org/x/sys/unix", Selector: "RawSyscall"}:        {Pkg: hooksGo127Package},
	{Pkg: "golang.org/x/sys/unix", Selector: "RawSyscall6"}:       {Pkg: hooksGo127Package},
	{Pkg: "golang.org/x/sys/unix", Selector: "RawSyscallNoError"}: {Pkg: hooksGo127Package},
	{Pkg: "golang.org/x/sys/unix", Selector: "Syscall"}:           {Pkg: hooksGo127Package},
	{Pkg: "golang.org/x/sys/unix", Selector: "Syscall6"}:          {Pkg: hooksGo127Package},
	{Pkg: "golang.org/x/sys/unix", Selector: "SyscallNoError"}:    {Pkg: hooksGo127Package},

	// amd64 only?
	{Pkg: "golang.org/x/sys/unix", Selector: "gettimeofday"}: {Pkg: hooksGo127Package},

	{Pkg: "hash/maphash", Selector: "runtime_memhash"}: {Pkg: hooksGo127Package},
	{Pkg: "hash/maphash", Selector: "runtime_rand"}:    {Pkg: hooksGo127Package},

	{Pkg: "internal/abi", Selector: "FuncPCABI0"}:        {Pkg: hooksGo127Package},
	{Pkg: "internal/abi", Selector: "FuncPCABIInternal"}: {Pkg: hooksGo127Package},

	{Pkg: "internal/chacha8rand", Selector: "block"}: {Pkg: hooksGo127Package},

	{Pkg: "internal/bytealg", Selector: "Compare"}:                        {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "Count"}:                          {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "CountString"}:                    {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "Equal"}:                          {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "Index"}:                          {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "IndexByte"}:                      {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "IndexByteString"}:                {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "IndexString"}:                    {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "MakeNoZero"}:                     {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "abigen_runtime_cmpstring"}:       {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "abigen_runtime_memequal"}:        {Pkg: hooksGo127Package},
	{Pkg: "internal/bytealg", Selector: "abigen_runtime_memequal_varlen"}: {Pkg: hooksGo127Package},

	// arm only?
	{Pkg: "internal/cpu", Selector: "getMIDR"}:  {Pkg: hooksGo127Package},
	{Pkg: "internal/cpu", Selector: "getisar0"}: {Pkg: hooksGo127Package},

	// amd only?
	{Pkg: "internal/cpu", Selector: "cpuid"}:           {Pkg: hooksGo127Package},
	{Pkg: "internal/cpu", Selector: "xgetbv"}:          {Pkg: hooksGo127Package},
	{Pkg: "internal/cpu", Selector: "getGOAMD64level"}: {Pkg: hooksGo127Package},

	{Pkg: "internal/godebug", Selector: "registerMetric"}:      {Pkg: hooksGo127Package},
	{Pkg: "internal/godebug", Selector: "setNewIncNonDefault"}: {Pkg: hooksGo127Package},
	{Pkg: "internal/godebug", Selector: "setUpdate"}:           {Pkg: hooksGo127Package},
	{Pkg: "internal/godebug", Selector: "write"}:               {Pkg: hooksGo127Package},

	{Pkg: "internal/poll", Selector: "runtimeNano"}:                    {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_Semacquire"}:             {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_Semrelease"}:             {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_isPollServerDescriptor"}: {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollClose"}:              {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollOpen"}:               {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollReset"}:              {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollServerInit"}:         {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollSetDeadline"}:        {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollUnblock"}:            {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollWait"}:               {Pkg: hooksGo127Package},
	{Pkg: "internal/poll", Selector: "runtime_pollWaitCanceled"}:       {Pkg: hooksGo127Package},

	{Pkg: "internal/runtime/syscall/linux", Selector: "Syscall6"}: {Pkg: hooksGo127Package},

	{Pkg: "internal/syscall/unix", Selector: "GetRandom"}: {Pkg: hooksGo127Package},
	{Pkg: "internal/syscall/unix", Selector: "fcntl"}:     {Pkg: hooksGo127Package},
	{Pkg: "internal/syscall/unix", Selector: "Openat"}:    {Pkg: hooksGo127Package},
	{Pkg: "internal/syscall/unix", Selector: "Unlinkat"}:  {Pkg: hooksGo127Package},

	{Pkg: "maps", Selector: "clone"}: {Pkg: hooksGo127Package},

	{Pkg: "math/rand", Selector: "runtime_rand"}: {Pkg: hooksGo127Package},

	{Pkg: "math/rand/v2", Selector: "runtime_rand"}: {Pkg: hooksGo127Package},

	{Pkg: "net", Selector: "runtime_rand"}: {Pkg: hooksGo127Package},

	{Pkg: "os", Selector: "runtime_args"}:       {Pkg: hooksGo127Package},
	{Pkg: "os", Selector: "runtime_beforeExit"}: {Pkg: hooksGo127Package},
	{Pkg: "os", Selector: "runtime_rand"}:       {Pkg: hooksGo127Package},
	{Pkg: "os", Selector: "sigpipe"}:            {Pkg: hooksGo127Package},
	{Pkg: "os", Selector: "checkClonePidfd"}:    {Pkg: hooksGo127Package},
	{Pkg: "os", Selector: "ignoreSIGSYS"}:       {Pkg: hooksGo127Package},
	{Pkg: "os", Selector: "restoreSIGSYS"}:      {Pkg: hooksGo127Package},

	{Pkg: "os/signal", Selector: "signalWaitUntilIdle"}: {Pkg: hooksGo127Package},
	{Pkg: "os/signal", Selector: "signal_disable"}:      {Pkg: hooksGo127Package},
	{Pkg: "os/signal", Selector: "signal_enable"}:       {Pkg: hooksGo127Package},
	{Pkg: "os/signal", Selector: "signal_ignore"}:       {Pkg: hooksGo127Package},
	{Pkg: "os/signal", Selector: "signal_ignored"}:      {Pkg: hooksGo127Package},
	{Pkg: "os/signal", Selector: "signal_recv"}:         {Pkg: hooksGo127Package},

	{Pkg: "runtime/debug", Selector: "SetTraceback"}:    {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "WriteHeapDump"}:   {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "freeOSMemory"}:    {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "modinfo"}:         {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "readGCStats"}:     {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "setGCPercent"}:    {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "setMaxStack"}:     {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "setMaxThreads"}:   {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "setMemoryLimit"}:  {Pkg: hooksGo127Package},
	{Pkg: "runtime/debug", Selector: "setPanicOnFault"}: {Pkg: hooksGo127Package},

	{Pkg: "runtime/trace", Selector: "userLog"}:        {Pkg: hooksGo127Package},
	{Pkg: "runtime/trace", Selector: "userRegion"}:     {Pkg: hooksGo127Package},
	{Pkg: "runtime/trace", Selector: "userTaskCreate"}: {Pkg: hooksGo127Package},
	{Pkg: "runtime/trace", Selector: "userTaskEnd"}:    {Pkg: hooksGo127Package},

	{Pkg: "crypto/fips140", Selector: "isBypassed"}:                 {Pkg: hooksGo127Package},
	{Pkg: "crypto/fips140", Selector: "setBypass"}:                  {Pkg: hooksGo127Package},
	{Pkg: "crypto/fips140", Selector: "unsetBypass"}:                {Pkg: hooksGo127Package},
	{Pkg: "crypto/internal/fips140deps/time", Selector: "monoTime"}: {Pkg: hooksGo127Package},
	{Pkg: "crypto/internal/constanttime", Selector: "boolToUint8"}:  {Pkg: hooksGo127Package},
	{Pkg: "crypto/internal/fips140", Selector: "fatal"}:             {Pkg: hooksGo127Package},
	{Pkg: "crypto/internal/fips140", Selector: "getIndicator"}:      {Pkg: hooksGo127Package},
	{Pkg: "crypto/internal/fips140", Selector: "setIndicator"}:      {Pkg: hooksGo127Package},
	{Pkg: "crypto/internal/sysrand", Selector: "fatal"}:             {Pkg: hooksGo127Package},
	{Pkg: "crypto/rand", Selector: "fatal"}:                         {Pkg: hooksGo127Package},

	{Pkg: "internal/runtime/atomic", Selector: "LoadAcquintptr"}:  {Pkg: hooksGo127Package},
	{Pkg: "internal/runtime/atomic", Selector: "StoreReluintptr"}: {Pkg: hooksGo127Package},

	{Pkg: "internal/sync", Selector: "fatal"}:                   {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "runtime_SemacquireMutex"}: {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "runtime_Semrelease"}:      {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "runtime_canSpin"}:         {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "runtime_doSpin"}:          {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "runtime_nanotime"}:        {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "runtime_rand"}:            {Pkg: hooksGo127Package},
	{Pkg: "internal/sync", Selector: "throw"}:                   {Pkg: hooksGo127Package},

	{Pkg: "internal/synctest", Selector: "IsInBubble"}:   {Pkg: hooksGo127Package},
	{Pkg: "internal/synctest", Selector: "associate"}:    {Pkg: hooksGo127Package},
	{Pkg: "internal/synctest", Selector: "disassociate"}: {Pkg: hooksGo127Package},
	{Pkg: "internal/synctest", Selector: "isAssociated"}: {Pkg: hooksGo127Package},

	{Pkg: "sync", Selector: "fatal"}:                       {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_Semacquire"}:          {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_SemacquireRWMutex"}:   {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_SemacquireRWMutexR"}:  {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_SemacquireWaitGroup"}: {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_Semrelease"}:          {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_notifyListAdd"}:       {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_notifyListCheck"}:     {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_notifyListNotifyAll"}: {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_notifyListNotifyOne"}: {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_notifyListWait"}:      {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_procPin"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_procUnpin"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_randn"}:               {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "runtime_registerPoolCleanup"}: {Pkg: hooksGo127Package},
	{Pkg: "sync", Selector: "throw"}:                       {Pkg: hooksGo127Package},

	{Pkg: "sync/atomic", Selector: "AddInt32"}:              {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AddInt64"}:              {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AddUint32"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AddUint64"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AddUintptr"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AndInt32"}:              {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AndInt64"}:              {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AndUint32"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AndUint64"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "AndUintptr"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "CompareAndSwapInt32"}:   {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "CompareAndSwapInt64"}:   {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "CompareAndSwapPointer"}: {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "CompareAndSwapUint32"}:  {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "CompareAndSwapUint64"}:  {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "CompareAndSwapUintptr"}: {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "LoadInt32"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "LoadInt64"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "LoadPointer"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "LoadUint32"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "LoadUint64"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "LoadUintptr"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "OrInt32"}:               {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "OrInt64"}:               {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "OrUint32"}:              {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "OrUint64"}:              {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "OrUintptr"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "StoreInt32"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "StoreInt64"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "StorePointer"}:          {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "StoreUint32"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "StoreUint64"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "StoreUintptr"}:          {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "SwapInt32"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "SwapInt64"}:             {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "SwapPointer"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "SwapUint32"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "SwapUint64"}:            {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "SwapUintptr"}:           {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "runtime_procPin"}:       {Pkg: hooksGo127Package},
	{Pkg: "sync/atomic", Selector: "runtime_procUnpin"}:     {Pkg: hooksGo127Package},

	{Pkg: "syscall", Selector: "Exit"}:                        {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "Getpagesize"}:                 {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "RawSyscall6"}:                 {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "cgocaller"}:                   {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "hasWaitingReaders"}:           {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "rawSyscallNoError"}:           {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "rawVforkSyscall"}:             {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtimeSetenv"}:               {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtimeUnsetenv"}:             {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_AfterExec"}:           {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_AfterFork"}:           {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_AfterForkInChild"}:    {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_BeforeExec"}:          {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_BeforeFork"}:          {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_doAllThreadsSyscall"}: {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_entersyscall"}:        {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_envs"}:                {Pkg: hooksGo127Package},
	{Pkg: "syscall", Selector: "runtime_exitsyscall"}:         {Pkg: hooksGo127Package},

	{Pkg: "syscall", Selector: "FcntlFlock"}: {Pkg: hooksGo127Package},

	// amd64 only?
	{Pkg: "syscall", Selector: "gettimeofday"}: {Pkg: hooksGo127Package},

	{Pkg: "weak", Selector: "runtime_makeStrongFromWeak"}:  {Pkg: hooksGo127Package},
	{Pkg: "weak", Selector: "runtime_registerWeakPointer"}: {Pkg: hooksGo127Package},

	{Pkg: "time", Selector: "Sleep"}:            {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "now"}:              {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "newTimer"}:         {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "resetTimer"}:       {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "runtimeIsBubbled"}: {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "runtimeNano"}:      {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "runtimeNow"}:       {Pkg: hooksGo127Package},
	{Pkg: "time", Selector: "stopTimer"}:        {Pkg: hooksGo127Package},
}

var hooksGensyscallGo127ByArch = map[string]map[packageSelector]packageSelector{}

// acceptedgo127Linknames are linknames to packages gosim does not translate
// that the translated code keeps.
var acceptedgo127Linknames = map[packageSelector]packageSelector{}

// nativeLinknamePackages are packages whose functions without a body call the
// runtime's implementation for the original package. The race detector's
// functions work on memory addresses, so translated code can call them.
var nativeLinknamePackages = map[string]bool{
	"internal/race": true,
}

// pushedLinknames are functions without a body whose body a translated package
// pushes to them with its own linkname.
var pushedLinknames = map[packageSelector]bool{
	// Pushed by crypto/sha3.fips140hash_sha3Unwrap.
	{Pkg: "crypto/internal/fips140hash", Selector: "sha3Unwrap"}: true,
}
