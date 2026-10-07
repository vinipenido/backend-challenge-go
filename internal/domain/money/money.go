// Package money implementa o value object Money com aritmética exata.
//
// Representação: inteiro de 64 bits com sinal (int64) em unidades mínimas
// (centavos) + moeda ISO 4217 com escala fixa de duas casas.
// Limites: de -92233720368547758.08 a 92233720368547758.07 em qualquer moeda.
// Nenhuma operação usa float32/float64; overflow é detectado e devolvido como erro.
package money

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Erros classificáveis via errors.Is.
var (
	ErrInvalidAmount    = errors.New("money: valor inválido")
	ErrInvalidCurrency  = errors.New("money: moeda inválida")
	ErrCurrencyMismatch = errors.New("money: moedas incompatíveis")
	ErrOverflow         = errors.New("money: overflow numérico")
	ErrUninitialized    = errors.New("money: valor não inicializado")
)

// Scale é a quantidade fixa de casas decimais do contrato.
const Scale = 2

// Money é imutável: todas as operações devolvem um novo valor.
type Money struct {
	minor    int64
	currency Currency
}

// Zero devolve o valor zero na moeda informada.
func Zero(c Currency) (Money, error) {
	if err := c.Validate(); err != nil {
		return Money{}, err
	}
	return Money{currency: c}, nil
}

// FromMinor reconstrói um valor a partir de unidades mínimas (usado na
// reidratação do banco, onde o valor é armazenado em BIGINT).
func FromMinor(minor int64, c Currency) (Money, error) {
	if err := c.Validate(); err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse interpreta uma string decimal estrita com exatamente duas casas.
//
// Gramática aceita: -?(0|[1-9][0-9]*)\.[0-9]{2}
// Rejeita: vazio, espaços, "+", NaN, Infinity, notação científica, separador
// de milhar, vírgula, zeros à esquerda, escala diferente de 2 e "-0.00".
// Não há arredondamento: qualquer entrada fora da gramática é erro.
func Parse(amount string, currencyCode string) (Money, error) {
	c, err := ParseCurrency(currencyCode)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

// ParseNonNegative aplica Parse e rejeita valores negativos (entradas externas).
func ParseNonNegative(amount string, currencyCode string) (Money, error) {
	m, err := Parse(amount, currencyCode)
	if err != nil {
		return Money{}, err
	}
	if m.minor < 0 {
		return Money{}, fmt.Errorf("%w: valor negativo não é permitido em entradas externas", ErrInvalidAmount)
	}
	return m, nil
}

func parseMinor(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: vazio", ErrInvalidAmount)
	}
	neg := false
	body := s
	if body[0] == '-' {
		neg = true
		body = body[1:]
	}
	dot := len(body) - Scale - 1
	if dot < 1 || body[dot] != '.' {
		return 0, fmt.Errorf("%w: %q deve ter exatamente %d casas decimais", ErrInvalidAmount, s, Scale)
	}
	intPart, fracPart := body[:dot], body[dot+1:]
	if !allDigits(intPart) || !allDigits(fracPart) {
		return 0, fmt.Errorf("%w: %q não é um decimal válido", ErrInvalidAmount, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: %q possui zeros à esquerda", ErrInvalidAmount, s)
	}

	// Acumula em uint64 para tratar o limite assimétrico de int64 (MinInt64).
	const limitPos = uint64(math.MaxInt64)
	const limitNeg = uint64(math.MaxInt64) + 1
	limit := limitPos
	if neg {
		limit = limitNeg
	}
	var acc uint64
	for _, part := range []string{intPart, fracPart} {
		for i := 0; i < len(part); i++ {
			d := uint64(part[i] - '0')
			if acc > (limit-d)/10 {
				return 0, fmt.Errorf("%w: %q excede o limite de int64 em unidades mínimas", ErrOverflow, s)
			}
			acc = acc*10 + d
		}
	}
	if neg {
		if acc == 0 {
			return 0, fmt.Errorf("%w: %q (zero negativo) não é canônico", ErrInvalidAmount, s)
		}
		if acc == limitNeg {
			return math.MinInt64, nil
		}
		return -int64(acc), nil
	}
	return int64(acc), nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Validate rejeita valores não inicializados (Money{}).
func (m Money) Validate() error {
	if m.currency.IsZero() {
		return fmt.Errorf("%w: Money sem moeda", ErrUninitialized)
	}
	return nil
}

// Minor devolve o valor em unidades mínimas.
func (m Money) Minor() int64 { return m.minor }

// Currency devolve a moeda.
func (m Money) Currency() Currency { return m.currency }

// IsZero indica valor igual a zero.
func (m Money) IsZero() bool { return m.minor == 0 }

// IsPositive indica valor estritamente maior que zero.
func (m Money) IsPositive() bool { return m.minor > 0 }

// IsNegative indica valor estritamente menor que zero.
func (m Money) IsNegative() bool { return m.minor < 0 }

func (m Money) compatible(o Money) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if err := o.Validate(); err != nil {
		return err
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s e %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add soma dois valores da mesma moeda, detectando overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, fmt.Errorf("%w: soma de %s e %s", ErrOverflow, m, o)
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub subtrai dois valores da mesma moeda, detectando overflow.
// O resultado pode ser negativo (diferenças internas); a regra de saldo não
// negativo pertence à carteira.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, fmt.Errorf("%w: subtração de %s e %s", ErrOverflow, m, o)
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Neg devolve o valor com sinal invertido; MinInt64 não possui oposto em int64.
func (m Money) Neg() (Money, error) {
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	if m.minor == math.MinInt64 {
		return Money{}, fmt.Errorf("%w: negação de %s", ErrOverflow, m)
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp compara valores da mesma moeda: -1 se m < o, 0 se iguais, 1 se m > o.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal compara valor e moeda.
func (m Money) Equal(o Money) bool {
	return m.minor == o.minor && m.currency == o.currency
}

// Amount devolve a representação decimal canônica, por exemplo "25.00" ou "-0.50".
func (m Money) Amount() string {
	neg := m.minor < 0
	var abs uint64
	if neg {
		abs = uint64(-(m.minor + 1)) + 1 // evita overflow em MinInt64
	} else {
		abs = uint64(m.minor)
	}
	digits := strconv.FormatUint(abs, 10)
	for len(digits) <= Scale {
		digits = "0" + digits
	}
	out := digits[:len(digits)-Scale] + "." + digits[len(digits)-Scale:]
	if neg {
		return "-" + out
	}
	return out
}

// String implementa fmt.Stringer no formato "25.00 BRL".
func (m Money) String() string {
	return m.Amount() + " " + m.currency.code
}

// jsonMoney é o formato externo: {"amount":"25.00","currency":"BRL"}.
type jsonMoney struct {
	Amount   *string `json:"amount"`
	Currency *string `json:"currency"`
}

// MarshalJSON serializa como strings decimais, nunca como número JSON.
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	a, c := m.Amount(), m.currency.code
	return json.Marshal(jsonMoney{Amount: &a, Currency: &c})
}

// UnmarshalJSON aceita apenas o objeto {"amount":"<decimal>","currency":"<ISO>"}
// com amount obrigatoriamente em string (números JSON são rejeitados para evitar
// qualquer passagem por ponto flutuante) e sem campos desconhecidos.
func (m *Money) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw jsonMoney
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("%w: formato JSON inválido (%v)", ErrInvalidAmount, err)
	}
	if raw.Amount == nil {
		return fmt.Errorf("%w: campo amount ausente", ErrInvalidAmount)
	}
	if raw.Currency == nil {
		return fmt.Errorf("%w: campo currency ausente", ErrInvalidCurrency)
	}
	parsed, err := Parse(*raw.Amount, *raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
