package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/indice"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/dadosjusbr/storage"
	"github.com/kelseyhightower/envconfig"
	"google.golang.org/protobuf/encoding/prototext"
)

type config struct {
	MongoURI    string `envconfig:"MONGODB_URI" required:"true"`
	DBName      string `envconfig:"MONGODB_DBNAME" required:"true"`
	MongoMICol  string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol  string `envconfig:"MONGODB_AGCOL" required:"true"`
	MongoPkgCol string `envconfig:"MONGODB_PKGCOL" required:"true"`

	// Swift Conf
	SwiftUsername  string `envconfig:"SWIFT_USERNAME" required:"true"`
	SwiftAPIKey    string `envconfig:"SWIFT_APIKEY" required:"true"`
	SwiftAuthURL   string `envconfig:"SWIFT_AUTHURL" required:"true"`
	SwiftDomain    string `envconfig:"SWIFT_DOMAIN" required:"true"`
	SwiftContainer string `envconfig:"SWIFT_CONTAINER" required:"true"`
	// Backup conf
	IgnoreBackups bool `envconfig:"IGNORE_BACKUPS" required:"false" default:"false"`
}

func main() {
	var c config
	if err := envconfig.Process("", &c); err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error loading config values from .env: %v", err.Error())))
	}

	db, err := storage.NewDBClient(c.MongoURI, c.DBName, c.MongoMICol, c.MongoAgCol, c.MongoPkgCol)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("error setting up db: %s", err)))
	}
	client, err := storage.NewClient(
		db,
		storage.NewCloudClient(c.SwiftUsername, c.SwiftAPIKey, c.SwiftAuthURL, c.SwiftDomain, c.SwiftContainer),
	)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("error setting up storage client: %s", err)))
	}
	var er pipeline.ResultadoExecucao
	erIN, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error reading execution result: %v", err)))
	}
	if err = prototext.Unmarshal(erIN, &er); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error reading execution result: %v", err)))
	}

	// Package.
	if er.Pr.Pacote == "" {
		status.ExitFromError(status.NewError(status.InvalidInput, fmt.Errorf("there is no package to store. PackageResult:%+v", er.Pr)))
	}
	packBackup, err := client.Cloud.UploadFile(er.Pr.Pacote, er.Rc.Coleta.Orgao)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to get Backup package files: %v, error: %v", er.Pr.Pacote, err)))
	}

	/*
		// Backup.
		if !c.IgnoreBackups && len(er.Cr.Files) == 0 {
			status.ExitFromError(status.NewError(2, fmt.Errorf("no backup files found: CrawlingResult:%+v", er.Cr)))
		}
	*/
	backup, err := client.Cloud.Backup(er.Rc.Coleta.Arquivos, er.Rc.Coleta.Orgao)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to get Backup files: %v, error: %v", er.Rc.Coleta.Arquivos, err)))
	}

	score := indice.CalcScore(*er.Rc.Metadados)

	agmi := storage.AgencyMonthlyInfo{
		AgencyID:          er.Rc.Coleta.Orgao,
		Month:             int(er.Rc.Coleta.Mes),
		Year:              int(er.Rc.Coleta.Ano),
		CrawlerID:         er.Rc.Coleta.RepositorioColetor,
		CrawlerVersion:    er.Rc.Coleta.VersaoColetor,
		CrawlerDir:        er.Rc.Coleta.DirColetor,
		Summary:           summary(er.Rc.Folha.ContraCheque),
		Backups:           backup,
		CrawlingTimestamp: er.Rc.Coleta.TimestampColeta,
		CrawlerRepo:       er.Rc.Coleta.RepositorioColetor,
		Meta: &storage.Meta{
			NoLoginRequired:   er.Rc.Metadados.NaoRequerLogin,
			NoCaptchaRequired: er.Rc.Metadados.NaoRequerCaptcha,
			Access:            er.Rc.Metadados.Acesso.String(),
			Extension:         er.Rc.Metadados.Extensao.String(),
			StrictlyTabular:   er.Rc.Metadados.EstritamenteTabular,
			ConsistentFormat:  er.Rc.Metadados.FormatoConsistente,
			HaveEnrollment:    er.Rc.Metadados.TemMatricula,
			ThereIsACapacity:  er.Rc.Metadados.TemLotacao,
			HasPosition:       er.Rc.Metadados.TemCargo,
			BaseRevenue:       er.Rc.Metadados.ReceitaBase.String(),
			OtherRecipes:      er.Rc.Metadados.OutrasReceitas.String(),
			Expenditure:       er.Rc.Metadados.Despesas.String(),
		},
		Score: &storage.Score{
			Score:             score.Score,
			EasinessScore:     score.EasinessScore,
			CompletenessScore: score.EasinessScore,
		},
		ProcInfo: er.Rc.Procinfo,
		Package:  packBackup,
	}
	if er.Rc.Procinfo != nil && er.Rc.Procinfo.Status != 0 {
		agmi.ProcInfo = er.Rc.Procinfo
	}
	if err = client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store agmi: %v", err)))
	}
	fmt.Println("Store Executed...")
}

// summary aux func to make all necessary calculations to DataSummary Struct
func summary(employees []*coleta.ContraCheque) storage.Summary {
	memberActive := storage.Summary{
		IncomeHistogram: map[int]int{10000: 0, 20000: 0, 30000: 0, 40000: 0, 50000: 0, -1: 0},
	}
	for _, emp := range employees {
		// checking if the employee instance has the required data to build the summary
		if emp.Remuneracoes == nil {
			status.ExitFromError(status.NewError(status.InvalidInput, fmt.Errorf("employee %+v is invalid. It does not have 'income' field", emp)))
		}
		updateSummary(&memberActive, *emp)
	}
	if memberActive.Count == 0 {
		return storage.Summary{}
	}
	return memberActive
}

//updateSummary auxiliary function that updates the summary data at each employee value
func updateSummary(s *storage.Summary, emp coleta.ContraCheque) {
	updateData := func(d *storage.DataSummary, value float64, count int) {
		if count == 1 {
			d.Min = value
			d.Max = value
		} else {
			d.Min = math.Min(d.Min, value)
			d.Max = math.Max(d.Max, value)
		}
		d.Total += value
		d.Average = d.Total / float64(count)
	}

	// Income histogram.
	s.Count++
	salaryBase, benefits := calcBaseSalary(emp)
	var salaryRange int
	if salaryBase <= 10000 {
		salaryRange = 10000
	} else if salaryBase <= 20000 {
		salaryRange = 20000
	} else if salaryBase <= 30000 {
		salaryRange = 30000
	} else if salaryBase <= 40000 {
		salaryRange = 40000
	} else if salaryBase <= 50000 {
		salaryRange = 50000
	} else {
		salaryRange = -1 // -1 is maker when the salary is over 50000
	}
	s.IncomeHistogram[salaryRange]++

	updateData(&s.BaseRemuneration, salaryBase, s.Count)
	updateData(&s.OtherRemunerations, benefits, s.Count)
}

func calcBaseSalary(emp coleta.ContraCheque) (float64, float64) {
	var salaryBase float64
	var benefits float64
	for _, v := range emp.Remuneracoes.Remuneracao {
		if v.TipoReceita == coleta.Remuneracao_B && v.Natureza == coleta.Remuneracao_R {
			salaryBase += v.Valor
		} else if v.TipoReceita == coleta.Remuneracao_O && v.Natureza == coleta.Remuneracao_R {
			benefits += v.Valor
		}
	}
	return salaryBase, benefits
}
