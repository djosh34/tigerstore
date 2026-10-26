/*
Package gosimruntime simulates the go runtime for programs tested using gosim.

The gosimruntime API is internal to gosim and programs should not directly use
it. This package is exported only because translated code must be able to
import it. Instead, use the public API in the gosim package.
*/
package gosimruntime
