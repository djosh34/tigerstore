package go127

func OsSignal_signalWaitUntilIdle() {
	// noop
}

func OsSignal_signal_disable(uint32) {}

func OsSignal_signal_enable(uint32) {}

func OsSignal_signal_ignore(uint32) {}

func OsSignal_signal_ignored(uint32) bool {
	return false
}

func OsSignal_signal_recv() uint32 {
	// block forever
	select {}
}
