package money_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
)

func mustParse(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("Parse(%q,%q): %v", amount, currency, err)
	}
	return m
}

func TestParseValid(t *testing.T) {
	cases := []struct {
		in    string
		minor int64
	}{
		{"0.00", 0},
		{"0.01", 1},
		{"25.00", 2500},
		{"1000.00", 100000},
		{"-0.50", -50},
		{"92233720368547758.07", math.MaxInt64},
		{"-92233720368547758.08", math.MinInt64},
	}
	for _, tc := range cases {
		m := mustParse(t, tc.in, "BRL")
		if m.Minor() != tc.minor {
			t.Errorf("Parse(%q) = %d, esperado %d", tc.in, m.Minor(), tc.minor)
		}
		if m.Amount() != tc.in {
			t.Errorf("Amount() = %q, esperado %q (ida e volta)", m.Amount(), tc.in)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	invalid := []string{
		"", " ", "25", "25.0", "25.000", "25.", ".50", "+25.00", "025.00", "00.00",
		"-0.00", "NaN", "nan", "Infinity", "-Infinity", "Inf", "1e2", "1E+02", "2.5e1",
		"25,00", "1,000.00", "1_000.00", " 25.00", "25.00 ", "0x10.00", "--1.00", "1.-0",
	}
	for _, in := range invalid {
		if _, err := money.Parse(in, "BRL"); err == nil {
			t.Errorf("Parse(%q) deveria falhar", in)
		} else if !errors.Is(err, money.ErrInvalidAmount) && !errors.Is(err, money.ErrOverflow) {
			t.Errorf("Parse(%q) erro não classificado: %v", in, err)
		}
	}
}

func TestParseOverflow(t *testing.T) {
	for _, in := range []string{"92233720368547758.08", "-92233720368547758.09", "99999999999999999999.00"} {
		_, err := money.Parse(in, "BRL")
		if !errors.Is(err, money.ErrOverflow) {
			t.Errorf("Parse(%q): esperado ErrOverflow, obtido %v", in, err)
		}
	}
}

func TestParseNonNegative(t *testing.T) {
	if _, err := money.ParseNonNegative("-1.00", "BRL"); !errors.Is(err, money.ErrInvalidAmount) {
		t.Fatalf("esperado ErrInvalidAmount para negativo, obtido %v", err)
	}
	if m, err := money.ParseNonNegative("0.00", "BRL"); err != nil || !m.IsZero() {
		t.Fatalf("zero deveria ser aceito: %v", err)
	}
}

func TestCurrencyValidation(t *testing.T) {
	for _, code := range []string{"", "BR", "BRLL", "brl", "Brl", "XXX", "JPY", "12A"} {
		if _, err := money.Parse("1.00", code); !errors.Is(err, money.ErrInvalidCurrency) {
			t.Errorf("moeda %q: esperado ErrInvalidCurrency, obtido %v", code, err)
		}
	}
	if _, err := money.Zero(money.Currency{}); !errors.Is(err, money.ErrUninitialized) {
		t.Fatalf("moeda não inicializada deveria ser rejeitada: %v", err)
	}
}

func TestArithmetic(t *testing.T) {
	a := mustParse(t, "100.00", "BRL")
	b := mustParse(t, "80.00", "BRL")

	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "180.00" {
		t.Fatalf("Add = %v, %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Amount() != "-20.00" || !diff.IsNegative() {
		t.Fatalf("Sub = %v, %v", diff, err)
	}
	neg, err := a.Neg()
	if err != nil || neg.Amount() != "-100.00" {
		t.Fatalf("Neg = %v, %v", neg, err)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("Cmp(100,80) = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Fatalf("Cmp(80,100) = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Fatalf("Cmp(a,a) = %d", c)
	}
	// Imutabilidade: os operandos não mudam.
	if a.Amount() != "100.00" || b.Amount() != "80.00" {
		t.Fatalf("operandos foram alterados: %v %v", a, b)
	}
	z, _ := money.Zero(money.BRL)
	if !z.IsZero() || z.Amount() != "0.00" {
		t.Fatalf("Zero = %v", z)
	}
}

func TestArithmeticOverflow(t *testing.T) {
	max, _ := money.FromMinor(math.MaxInt64, money.BRL)
	min, _ := money.FromMinor(math.MinInt64, money.BRL)
	one, _ := money.FromMinor(1, money.BRL)

	if _, err := max.Add(one); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("max+1: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("min-1: %v", err)
	}
	negOne, _ := one.Neg()
	if _, err := max.Sub(negOne); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("max-(-1): %v", err)
	}
	if _, err := min.Add(negOne); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("min+(-1): %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("-min: %v", err)
	}
	if min.Amount() != "-92233720368547758.08" {
		t.Errorf("Amount(min) = %s", min.Amount())
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := mustParse(t, "10.00", "BRL")
	usd := mustParse(t, "10.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Errorf("valores em moedas diferentes não podem ser iguais")
	}
}

func TestUninitializedMoney(t *testing.T) {
	var zero money.Money
	ok := mustParse(t, "1.00", "BRL")
	if _, err := zero.Add(ok); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Add: %v", err)
	}
	if _, err := ok.Sub(zero); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Neg: %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Errorf("Marshal de Money não inicializado deveria falhar")
	}
}

func TestJSON(t *testing.T) {
	m := mustParse(t, "25.00", "BRL")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("Marshal = %s", b)
	}
	var back money.Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("Unmarshal = %v, %v", back, err)
	}

	invalid := []string{
		`{"amount":25.00,"currency":"BRL"}`,
		`{"amount":"25.00"}`,
		`{"currency":"BRL"}`,
		`{"amount":"2.5e1","currency":"BRL"}`,
		`{"amount":"25.001","currency":"BRL"}`,
		`{"amount":"NaN","currency":"BRL"}`,
		`{"amount":"25.00","currency":"BRL","extra":1}`,
		`{"amount":null,"currency":"BRL"}`,
		`"25.00"`,
	}
	for _, in := range invalid {
		var x money.Money
		if err := json.Unmarshal([]byte(in), &x); err == nil {
			t.Errorf("Unmarshal(%s) deveria falhar", in)
		}
	}
}
