//go:build !faultinject

// Package fault oferece pontos de injeção de falhas usados pelos testes de
// recuperação. No build padrão todas as funções são no-op e não têm custo;
// com a build tag "faultinject" elas passam a obedecer às variáveis de ambiente
// FAULT_CRASH_POINTS e FAULT_ERROR_POINTS (ver fault_enabled.go).
package fault

// Crash encerra o processo abruptamente (SIGKILL) se o ponto estiver ativo.
func Crash(string) {}

// Error devolve um erro transitório injetado se o ponto estiver ativo.
func Error(string) error { return nil }

// Enabled informa se a injeção de falhas foi compilada no binário.
func Enabled() bool { return false }
