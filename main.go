package main

import (
	"fmt"
	"github.com/dadosjusbr/storage/repositories/database/postgres"
	"github.com/dadosjusbr/storage/repositories/fileStorage"
	"io/ioutil"
	"math"
	"os"
	"strings"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/dadosjusbr/storage"
	"github.com/dadosjusbr/storage/models"
	"github.com/dadosjusbr/storage/repositories/database/mongo"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"google.golang.org/protobuf/encoding/prototext"
)

type config struct {
	MongoURI    string `envconfig:"MONGODB_URI" required:"true"`
	DBName      string `envconfig:"MONGODB_DBNAME" required:"true"`
	MongoMICol  string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol  string `envconfig:"MONGODB_AGCOL" required:"true"`
	MongoPkgCol string `envconfig:"MONGODB_PKGCOL" required:"true"`
	MongoRevCol string `envconfig:"MONGODB_REVCOL" required:"true"`

	PostgresUser     string `envconfig:"POSTGRES_USER" required:"true"`
	PostgresPassword string `envconfig:"POSTGRES_PASSWORD" required:"true"`
	PostgresDBName   string `envconfig:"POSTGRES_DBNAME" required:"true"`
	PostgresHost     string `envconfig:"POSTGRES_HOST" required:"true"`
	PostgresPort     string `envconfig:"POSTGRES_PORT" required:"true"`

	AWSRegion    string `envconfig:"AWS_REGION" required:"true"`
	S3Bucket     string `envconfig:"S3_BUCKET" required:"true"`
	AWSAccessKey string `envconfig:"AWS_ACCESS_KEY_ID" required:"true"`
	AWSSecretKey string `envconfig:"AWS_SECRET_ACCESS_KEY" required:"true"`

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
	godotenv.Load()
	if err := envconfig.Process("", &c); err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error loading config values from .env: %v", err.Error())))
	}

	// Criando o client do MongoDB
	mongoDb, err := mongo.NewMongoDB(c.MongoURI, c.DBName, c.MongoMICol, c.MongoAgCol, c.MongoPkgCol, c.MongoRevCol)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error creating MongoDB client: %v", err.Error())))
	}
	mongoDb.Collection(c.MongoMICol)

	// Criando o client do Postgres
	postgresDB, err := postgres.NewPostgresDB(c.PostgresUser, c.PostgresPassword, c.PostgresDBName, c.PostgresHost, c.PostgresPort)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error creating PostgresDB client: %v", err.Error())))
	}
	// Criando o client do S3
	s3Client, err := fileStorage.NewS3Client(c.AWSRegion, c.S3Bucket)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error creating S3 client: %v", err.Error())))
	}

	// Criando client do storage a partir do banco postgres e do client do s3
	pgS3Client, err := storage.NewClient(postgresDB, s3Client)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("error setting up postgres storage client: %s", err)))
	}
	defer pgS3Client.Db.Disconnect()

	// Criando o client do storage a partir do banco mongodb e do client do s3
	mgoS3Client, err := storage.NewClient(mongoDb, s3Client)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("error setting up mongo storage client: %s", err)))
	}
	defer mgoS3Client.Db.Disconnect()

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

	dstKey := fmt.Sprintf("%s/datapackage/%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes)
	s3Backup, err := pgS3Client.Cloud.UploadFile(er.Pr.Pacote, dstKey)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to get Backup package files from S3: %v, error: %v", er.Pr.Pacote, err)))
	}
	/*
		// Backup.
		if !c.IgnoreBackups && len(er.Cr.Files) == 0 {
			status.ExitFromError(status.NewError(2, fmt.Errorf("no backup files found: CrawlingResult:%+v", er.Cr)))
		}
	*/
	dstKey = fmt.Sprintf("%s/backups/%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes)
	s3Backups, err := pgS3Client.Cloud.UploadFile(er.Rc.Coleta.Arquivos[0], dstKey)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to get Backup files from S3: %v, error: %v", er.Rc.Coleta.Arquivos, err)))
	}

	dstKey = fmt.Sprintf("%s/remuneracoes/%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes)
	_, err = mgoS3Client.Cloud.UploadFile(er.Pr.Remuneracoes, dstKey)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to upload Remunerations zip in S3: %v, error: %v", er.Pr.Remuneracoes, err)))
	}

	var parserRepository string
	var parserVersion string
	for _, v := range []string{"mpma", "mprr", "mpms", "mpsc", "mpam", "mpac", "mprs", "mpap", "mpro", "cnj"} {
		if strings.Contains(er.Rc.Coleta.Orgao, v) {
			parserRepository = fmt.Sprintf("https://github.com/dadosjusbr/parser-%s", er.Rc.Coleta.Orgao)
			parserVersion = "unspecified"
			break
		}
	}

	agmi := models.AgencyMonthlyInfo{
		AgencyID:          er.Rc.Coleta.Orgao,
		Month:             int(er.Rc.Coleta.Mes),
		Year:              int(er.Rc.Coleta.Ano),
		CrawlerRepo:       er.Rc.Coleta.RepositorioColetor,
		CrawlerVersion:    er.Rc.Coleta.VersaoColetor,
		ParserRepo:        parserRepository,
		ParserVersion:     parserVersion,
		CrawlingTimestamp: er.Rc.Coleta.TimestampColeta,
		Summary:           summary(er.Rc.Folha.ContraCheque),
		Backups:           *s3Backups,
		Meta: &models.Meta{
			OpenFormat:       er.Rc.Metadados.FormatoAberto,
			Access:           er.Rc.Metadados.Acesso.String(),
			Extension:        er.Rc.Metadados.Extensao.String(),
			StrictlyTabular:  er.Rc.Metadados.EstritamenteTabular,
			ConsistentFormat: er.Rc.Metadados.FormatoConsistente,
			HaveEnrollment:   er.Rc.Metadados.TemMatricula,
			ThereIsACapacity: er.Rc.Metadados.TemLotacao,
			HasPosition:      er.Rc.Metadados.TemCargo,
			BaseRevenue:      er.Rc.Metadados.ReceitaBase.String(),
			OtherRecipes:     er.Rc.Metadados.OutrasReceitas.String(),
			Expenditure:      er.Rc.Metadados.Despesas.String(),
		},
		Score: &models.Score{
			Score:             float64(er.Rc.Metadados.IndiceTransparencia),
			EasinessScore:     float64(er.Rc.Metadados.IndiceFacilidade),
			CompletenessScore: float64(er.Rc.Metadados.IndiceCompletude),
		},
		ProcInfo: er.Rc.Procinfo,
		Package:  s3Backup,
	}
	if er.Rc.Procinfo != nil && er.Rc.Procinfo.Status != 0 {
		agmi.ProcInfo = er.Rc.Procinfo
	}
	if err = mgoS3Client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store agmi: %v", err)))
	}
	if err = pgS3Client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store 'coleta': %v", err)))
	}
	fmt.Println("Store Executed...")
}

// summary aux func to make all necessary calculations to DataSummary Struct
func summary(employees []*coleta.ContraCheque) models.Summary {
	memberActive := models.Summary{
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
		return models.Summary{}
	}
	return memberActive
}

//updateSummary auxiliary function that updates the summary data at each employee value
func updateSummary(s *models.Summary, emp coleta.ContraCheque) {
	updateData := func(d *models.DataSummary, value float64, count int) {
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
