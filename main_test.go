package main

import (
	"testing"

	"github.com/dadosjusbr/proto/coleta"
	"github.com/matryer/is"
)

func TestCalcCompletenessScore(t *testing.T) {
	inputA := coleta.Metadados{
		TemMatricula:   true,
		TemLotacao:     true,
		TemCargo:       true,
		ReceitaBase:    coleta.Metadados_DETALHADO,
		OutrasReceitas: coleta.Metadados_DETALHADO,
		Despesas:       coleta.Metadados_DETALHADO,
	}

	inputB := coleta.Metadados{
		TemMatricula:   false,
		TemLotacao:     false,
		TemCargo:       false,
		ReceitaBase:    coleta.Metadados_AUSENCIA,
		OutrasReceitas: coleta.Metadados_AUSENCIA,
		Despesas:       coleta.Metadados_AUSENCIA,
	}

	data := []struct {
		Desc     string
		Input    coleta.Metadados
		Expected float64
	}{
		{"Best", inputA, float64(1)},
		{"Worst", inputB, float64(0)},
	}

	for _, d := range data {
		t.Run(d.Desc, func(t *testing.T) {
			is := is.New(t)
			b := calcCompletenessScore(d.Input)
			is.Equal(b, d.Expected)
		})
	}
}

func TestCalcEasinessScore(t *testing.T) {
	inputA := coleta.Metadados{
		NaoRequerLogin:      true,
		NaoRequerCaptcha:    true,
		Acesso:              coleta.Metadados_ACESSO_DIRETO,
		FormatoConsistente:  true,
		EstritamenteTabular: true,
	}

	inputB := coleta.Metadados{
		NaoRequerLogin:      false,
		NaoRequerCaptcha:    false,
		Acesso:              coleta.Metadados_NECESSITA_SIMULACAO_USUARIO,
		FormatoConsistente:  false,
		EstritamenteTabular: false,
	}

	data := []struct {
		Desc     string
		Input    coleta.Metadados
		Expected float64
	}{
		{"Best", inputA, float64(1)},
		{"Worst", inputB, float64(0)},
	}

	for _, d := range data {
		t.Run(d.Desc, func(t *testing.T) {
			is := is.New(t)
			b := calcEasinessScore(d.Input)
			is.Equal(b, d.Expected)
		})
	}
}
