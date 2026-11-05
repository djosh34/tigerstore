package simfs

// Exp473 counts the whole-disk copies and the bytes they clone (research
// #473, exp branch only): Files, Durable and Copy.
var Exp473 struct {
	Files, FilesBytes, Durables, DurableBytes, Copies, CopyBytes int64
}
