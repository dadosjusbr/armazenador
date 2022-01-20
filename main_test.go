package main

import (
	"testing"

	"github.com/dadosjusbr/proto/coleta"
	"github.com/matryer/is"
)

func TestCalcCompletenessScore(t *testing.T) {
	data := []struct {
		Desc     string
		Input    coleta.Metadados
		Expected float64
	}{
		{"Sempre positivo", coleta.Metadados{
			TemMatricula:   true,
			TemLotacao:     true,
			TemCargo:       true,
			ReceitaBase:    coleta.Metadados_DETALHADO,
			OutrasReceitas: coleta.Metadados_DETALHADO,
			Despesas:       coleta.Metadados_DETALHADO,
		}, 1.0},
		{"Sempre negativo", coleta.Metadados{
			TemMatricula:   false,
			TemLotacao:     false,
			TemCargo:       false,
			ReceitaBase:    coleta.Metadados_AUSENCIA,
			OutrasReceitas: coleta.Metadados_AUSENCIA,
			Despesas:       coleta.Metadados_AUSENCIA,
		}, 0.0},
		{"CNJ-2020", coleta.Metadados{
			TemMatricula:   false,
			TemLotacao:     false,
			TemCargo:       false,
			ReceitaBase:    coleta.Metadados_DETALHADO,
			OutrasReceitas: coleta.Metadados_DETALHADO,
			Despesas:       coleta.Metadados_DETALHADO,
		}, 0.5},
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
	data := []struct {
		Desc     string
		Input    coleta.Metadados
		Expected float64
	}{
		{"Sempre positivo", coleta.Metadados{
			NaoRequerLogin:      true,
			NaoRequerCaptcha:    true,
			Acesso:              coleta.Metadados_ACESSO_DIRETO,
			FormatoConsistente:  true,
			EstritamenteTabular: true,
		}, 1.0},
		{"Sempre negativo", coleta.Metadados{
			NaoRequerLogin:      false,
			NaoRequerCaptcha:    false,
			Acesso:              coleta.Metadados_NECESSITA_SIMULACAO_USUARIO,
			FormatoConsistente:  false,
			EstritamenteTabular: false,
		}, 0.0},
		{"CNJ-2020", coleta.Metadados{
			NaoRequerLogin:      true,
			NaoRequerCaptcha:    true,
			Acesso:              coleta.Metadados_NECESSITA_SIMULACAO_USUARIO,
			FormatoConsistente:  true,
			EstritamenteTabular: true,
		}, 0.8},
	}

	for _, d := range data {
		t.Run(d.Desc, func(t *testing.T) {
			is := is.New(t)
			b := calcEasinessScore(d.Input)
			is.Equal(b, d.Expected)
		})
	}
}

func TestCalcScore(t *testing.T) {
	data := []struct {
		Desc     string
		Input    coleta.Metadados
		Expected float64
	}{
		{"Sempre positivo", coleta.Metadados{
			TemMatricula:        true,
			TemLotacao:          true,
			TemCargo:            true,
			ReceitaBase:         coleta.Metadados_DETALHADO,
			OutrasReceitas:      coleta.Metadados_DETALHADO,
			Despesas:            coleta.Metadados_DETALHADO,
			NaoRequerLogin:      true,
			NaoRequerCaptcha:    true,
			Acesso:              coleta.Metadados_ACESSO_DIRETO,
			FormatoConsistente:  true,
			EstritamenteTabular: true,
		}, 1.0},
		{"Sempre negativo", coleta.Metadados{
			TemMatricula:        false,
			TemLotacao:          false,
			TemCargo:            false,
			ReceitaBase:         coleta.Metadados_AUSENCIA,
			OutrasReceitas:      coleta.Metadados_AUSENCIA,
			Despesas:            coleta.Metadados_AUSENCIA,
			NaoRequerLogin:      false,
			NaoRequerCaptcha:    false,
			Acesso:              coleta.Metadados_NECESSITA_SIMULACAO_USUARIO,
			FormatoConsistente:  false,
			EstritamenteTabular: false,
		}, 0.0},
		{"CNJ-2020", coleta.Metadados{
			TemMatricula:        false,
			TemLotacao:          false,
			TemCargo:            false,
			ReceitaBase:         coleta.Metadados_DETALHADO,
			OutrasReceitas:      coleta.Metadados_DETALHADO,
			Despesas:            coleta.Metadados_DETALHADO,
			NaoRequerLogin:      true,
			NaoRequerCaptcha:    true,
			Acesso:              coleta.Metadados_NECESSITA_SIMULACAO_USUARIO,
			FormatoConsistente:  true,
			EstritamenteTabular: true,
		}, 0.65},
	}

	for _, d := range data {
		t.Run(d.Desc, func(t *testing.T) {
			is := is.New(t)
			b := calcScore(d.Input)
			is.Equal(b, d.Expected)
		})
	}
}
