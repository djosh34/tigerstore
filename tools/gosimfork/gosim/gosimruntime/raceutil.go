package gosimruntime

// TestRaceToken is a publicly accessible RaceToken to allow synchronization
// between different machines that otherwise have no common happens-before
// edges.
//
// Machines do not have any happens-before edges between each other. This is
// fine in most cases because machines do not share any memory and have their
// own globals.
//
// In low-level gosim tests, though, it can be convenient to share some memory
// (eg. atomic.Bool indicating success). To do that without triggering the race
// detector, call TestRaceToken.Release before creating the machine and then
// TestRaceToken.Acquire inside the machine.
var TestRaceToken = MakeRaceToken()
