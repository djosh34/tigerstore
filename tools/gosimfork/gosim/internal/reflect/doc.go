// Package reflect is a shim of the built-in reflect package.
//
// The built-in reflect package does not know how to handle gosim's replacement
// Map type. This shim package special-cases gosim's replaced types and
// otherwise forwards to the built-in reflect package.
package reflect
