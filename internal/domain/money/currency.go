package money

import "fmt"

// Currency representa um código de moeda ISO 4217.
//
// O campo é privado para que valores não inicializados (Currency{}) possam ser
// detectados e rejeitados: só é possível obter uma moeda válida por meio de
// ParseCurrency ou das constantes exportadas.
type Currency struct {
	code string
}

// Moedas suportadas. Todas possuem exatamente duas casas decimais (minor unit = 2),
// o que permite a escala fixa exigida pelo contrato externo.
var (
	BRL = Currency{code: "BRL"}
	USD = Currency{code: "USD"}
	EUR = Currency{code: "EUR"}
)

// supportedCurrencies lista os códigos ISO 4217 aceitos com escala de duas casas.
// Moedas com outra escala (por exemplo JPY=0 ou KWD=3) são rejeitadas de forma
// explícita, em vez de serem arredondadas silenciosamente.
var supportedCurrencies = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {},
	"COP": {}, "PEN": {}, "UYU": {}, "CAD": {}, "AUD": {}, "CHF": {},
}

// ParseCurrency valida um código ISO 4217 (três letras maiúsculas) suportado.
// Não há normalização de caixa: "brl" é rejeitado para manter o hash de
// idempotência determinístico sem transformações implícitas.
func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return Currency{}, fmt.Errorf("%w: código %q deve ter 3 letras", ErrInvalidCurrency, code)
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return Currency{}, fmt.Errorf("%w: código %q deve conter apenas letras maiúsculas", ErrInvalidCurrency, code)
		}
	}
	if _, ok := supportedCurrencies[code]; !ok {
		return Currency{}, fmt.Errorf("%w: moeda %q não suportada", ErrInvalidCurrency, code)
	}
	return Currency{code: code}, nil
}

// MustCurrency é um atalho para testes e constantes conhecidas; entra em pânico
// apenas por erro de programação (nunca para entradas externas).
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

// Code devolve o código ISO 4217.
func (c Currency) Code() string { return c.code }

// String implementa fmt.Stringer.
func (c Currency) String() string { return c.code }

// IsZero indica se a moeda não foi inicializada.
func (c Currency) IsZero() bool { return c.code == "" }

// Validate rejeita moedas não inicializadas.
func (c Currency) Validate() error {
	if c.code == "" {
		return fmt.Errorf("%w: moeda não inicializada", ErrUninitialized)
	}
	return nil
}
