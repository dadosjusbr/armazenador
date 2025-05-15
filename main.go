package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/dadosjusbr/status"
	"github.com/dadosjusbr/storage"
	"github.com/dadosjusbr/storage/models"
	"github.com/dadosjusbr/storage/repo/database"
	"github.com/dadosjusbr/storage/repo/file_storage"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
)

type config struct {
	PostgresUser     string `envconfig:"POSTGRES_USER" required:"true"`
	PostgresPassword string `envconfig:"POSTGRES_PASSWORD" required:"true"`
	PostgresDBName   string `envconfig:"POSTGRES_DBNAME" required:"true"`
	PostgresHost     string `envconfig:"POSTGRES_HOST" required:"true"`
	PostgresPort     string `envconfig:"POSTGRES_PORT" required:"true"`

	AWSRegion    string `envconfig:"AWS_REGION" required:"true"`
	S3Bucket     string `envconfig:"S3_BUCKET" required:"true"`
	AWSAccessKey string `envconfig:"AWS_ACCESS_KEY_ID" required:"true"`
	AWSSecretKey string `envconfig:"AWS_SECRET_ACCESS_KEY" required:"true"`

	// Backup conf
	IgnoreBackups bool `envconfig:"IGNORE_BACKUPS" required:"false" default:"false"`
	// Tempo inicial da coleta
	StartTime string `envconfig:"START_TIME" required:"false"`
}

func main() {
	var c config
	if err := envconfig.Process("", &c); err != nil {
		status.ExitFromError(status.NewError(status.DataUnavailable, fmt.Errorf("error loading config values from .env: %v", err.Error())))
	}

	// Criando o client do Postgres
	postgresDB, err := database.NewPostgresDB(c.PostgresUser, c.PostgresPassword, c.PostgresDBName, c.PostgresHost, c.PostgresPort)
	if err != nil {
		status.ExitFromError(status.NewError(status.DataUnavailable, fmt.Errorf("error creating PostgresDB client: %v", err.Error())))
	}
	// Criando o client do S3
	s3Client, err := file_storage.NewS3Client(c.AWSRegion, c.S3Bucket)
	if err != nil {
		status.ExitFromError(status.NewError(status.DataUnavailable, fmt.Errorf("error creating S3 client: %v", err.Error())))
	}

	// Criando client do storage a partir do banco postgres e do client do s3
	pgS3Client, err := storage.NewClient(postgresDB, s3Client)
	if err != nil {
		status.ExitFromError(status.NewError(status.ConnectionError, fmt.Errorf("error setting up postgres storage client: %s", err)))
	}
	defer pgS3Client.Db.Disconnect()

	var er pipeline.ResultadoExecucao
	erIN, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error reading execution result: %v", err)))
	}
	if err = prototext.Unmarshal(erIN, &er); err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error reading execution result: %v", err)))
	}

	// Package.
	if er.Pr.Pacote == "" {
		status.ExitFromError(status.NewError(status.InvalidInput, fmt.Errorf("there is no package to store. PackageResult:%+v", er.Pr)))
	}

	// Armazenando os datapackages no S3
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
	// Armazenando os backups no S3
	dstKey = fmt.Sprintf("%s/backups/%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes)
	s3Backups, err := pgS3Client.Cloud.UploadFile(er.Rc.Coleta.Arquivos[0], dstKey)
	if err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error trying to get Backup files from S3: %v, error: %v", er.Rc.Coleta.Arquivos, err)))
	}

	//Armazenando as remuneracoes no S3 e no postgres
	dstKey = fmt.Sprintf("%s/remuneracoes/%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes)
	_, err = pgS3Client.Cloud.UploadFile(er.Pr.Remuneracoes.ZipUrl, dstKey)
	if err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error trying to upload Remunerations zip in S3: %v, error: %v", er.Pr.Remuneracoes, err)))
	}
	err = pgS3Client.StoreRemunerations(models.Remunerations{
		AgencyID:     er.Rc.Coleta.Orgao,
		Year:         int(er.Rc.Coleta.Ano),
		Month:        int(er.Rc.Coleta.Mes),
		NumBase:      int(er.Pr.Remuneracoes.NumBase),
		NumOther:     int(er.Pr.Remuneracoes.NumOutras),
		NumDiscounts: int(er.Pr.Remuneracoes.NumDescontos),
		ZipUrl:       fmt.Sprintf("https://%s.s3.amazonaws.com/%s", c.S3Bucket, dstKey),
	})
	if err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error trying to store Remunerations zip in Postgres: %v, error: %v", er.Pr.Remuneracoes, err)))
	}

	var paychecks []models.Paycheck
	var remunerations []models.PaycheckItem
	m, _ := regexp.Compile("[A-Za-z]")

	// Mapeando as rubricas distintas da folha de contracheque
	itemValues := make(map[string]float64)

	// Contracheques
	for id, p := range er.Rc.Folha.ContraCheque {
		salary, benefits, discounts, remuneration := calcBaseSalary(*p)
		paychecks = append(paychecks, models.Paycheck{
			ID:            id + 1,
			Agency:        er.Rc.Coleta.Orgao,
			Month:         int(er.Rc.Coleta.Mes),
			Year:          int(er.Rc.Coleta.Ano),
			CollectKey:    er.Rc.Coleta.ChaveColeta,
			Name:          p.Nome,
			RegisterID:    p.Matricula,
			Role:          p.Funcao,
			Workplace:     p.LocalTrabalho,
			Salary:        salary,
			Benefits:      benefits,
			Discounts:     discounts,
			Remuneration:  remuneration,
			Situation:     ativoInativo(p.Ativo, er.Rc.Coleta.Orgao),
			SanitizedName: sanitizarItem(p.Nome),
		})
		// Detalhamento das despesas
		i := 1
		for _, r := range p.Remuneracoes.Remuneracao {
			if r.Valor != 0 {
				remunerations = append(remunerations, models.PaycheckItem{
					ID:         i,
					PaycheckID: id + 1,
					Agency:     er.Rc.Coleta.Orgao,
					Month:      int(er.Rc.Coleta.Mes),
					Year:       int(er.Rc.Coleta.Ano),
					Category:   r.Categoria,
					Item:       r.Item,
				})
				if r.Natureza == coleta.Remuneracao_D {
					remunerations[len(remunerations)-1].Type = "D"
					remunerations[len(remunerations)-1].Value = math.Abs(r.Valor)
				} else {
					remunerations[len(remunerations)-1].Type = r.Natureza.String() + "/" + r.TipoReceita.String()
					remunerations[len(remunerations)-1].Value = r.Valor
				}
				// rubrica inconsistente
				if !m.MatchString(r.Item) {
					remunerations[len(remunerations)-1].Inconsistent = true
					if r.TipoReceita == coleta.Remuneracao_O && r.Natureza == coleta.Remuneracao_R {
						itemValues["outras"] += r.Valor
					}
				} else {
					// Se a rubrica não for inconsistente, faremos uma cópia sanitizada na coluna item_sanitizado.
					itemSanitizado := sanitizarItem(r.Item)
					// agregamos o valor por rubrica (apenas benefícios)
					if r.TipoReceita == coleta.Remuneracao_O && r.Natureza == coleta.Remuneracao_R {
						itemValues[itemSanitizado] += r.Valor
					}
					remunerations[len(remunerations)-1].SanitizedItem = &itemSanitizado
				}
				i++
			}
		}
	}

	// A variável de ambiente só será passada quando a coleta for manual
	_, manualCollection := os.LookupEnv("MANUAL_COLLECTION")
	if manualCollection {
		er.Rc.Metadados.IndiceTransparencia = 0
		er.Rc.Metadados.IndiceFacilidade = 0
		er.Rc.Metadados.IndiceCompletude = 0
	}

	agmi := models.AgencyMonthlyInfo{
		AgencyID:          er.Rc.Coleta.Orgao,
		Month:             int(er.Rc.Coleta.Mes),
		Year:              int(er.Rc.Coleta.Ano),
		CrawlerRepo:       er.Rc.Coleta.RepositorioColetor,
		CrawlerVersion:    er.Rc.Coleta.VersaoColetor,
		ParserRepo:        er.Rc.Coleta.RepositorioParser,
		ParserVersion:     er.Rc.Coleta.VersaoParser,
		CrawlingTimestamp: er.Rc.Coleta.TimestampColeta,
		Summary:           summary(er.Rc.Folha.ContraCheque, itemValues),
		Backups:           []models.Backup{*s3Backups},
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
		ProcInfo:         er.Rc.Procinfo,
		Package:          s3Backup,
		ManualCollection: manualCollection,
	}
	// Calculando o tempo de execução da coleta
	if c.StartTime != "" {
		const layout = "2006-01-02 15:04:05.000000" // formato data-hora
		t, err := time.Parse(layout, c.StartTime)   // transformando a hora (string) para o tipo time.Time
		if err != nil {
			status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error calculating collection time: %v", err)))
		}
		Duration := time.Since(t) // Calcula a diferença da hora dada com a hora atual (UTC+0)
		agmi.Duration = Duration.Seconds()
	}
	if er.Rc.Procinfo != nil && er.Rc.Procinfo.Status != 0 {
		agmi.ProcInfo = er.Rc.Procinfo
	}

	if err = pgS3Client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error trying to store 'coleta': %v", err)))
	}

	if err := pgS3Client.StorePaychecks(paychecks, remunerations); err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error trying to store 'contracheques' and 'remuneracoes': %v", err)))
	}

	// O verificador-integridade-dados aguarda os dados no formato Json
	jsonEr, err := protojson.Marshal(&er)
	if err != nil {
		err = status.NewError(status.SystemError, fmt.Errorf("error marshalling execution result:%q", err))
		status.ExitFromError(err)
	}

	// Adicionando o campo sumário, que é calculado no próprio armazenador e não adicionado ao proto.
	var data map[string]interface{}
	if err := json.Unmarshal(jsonEr, &data); err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("failed to unmarshal JSON: %v", err)))
	}
	data["sumario"] = agmi.Summary

	newJsonEr, err := json.Marshal(data)
	if err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("failed to marshal JSON: %v", err)))
	}

	fmt.Printf("%s\n", newJsonEr)
}

// summary aux func to make all necessary calculations to DataSummary Struct
func summary(employees []*coleta.ContraCheque, itemValues map[string]float64) *models.Summary {
	itemSummary := aggregatingItems(itemValues)
	memberActive := models.Summary{
		IncomeHistogram: map[int]int{10000: 0, 20000: 0, 30000: 0, 40000: 0, 50000: 0, -1: 0},
		ItemSummary:     itemSummary,
	}
	for _, emp := range employees {
		// checking if the employee instance has the required data to build the summary
		if emp.Remuneracoes == nil {
			status.ExitFromError(status.NewError(status.InvalidInput, fmt.Errorf("employee %+v is invalid. It does not have 'income' field", emp)))
		}
		updateSummary(&memberActive, *emp)
	}
	if memberActive.Count == 0 {
		return &models.Summary{}
	}
	return &memberActive
}

// updateSummary auxiliary function that updates the summary data at each employee value
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
	salaryBase, benefits, discounts, remuneration := calcBaseSalary(emp)
	var remunerationRange int
	if remuneration <= 10000 {
		remunerationRange = 10000
	} else if remuneration <= 20000 {
		remunerationRange = 20000
	} else if remuneration <= 30000 {
		remunerationRange = 30000
	} else if remuneration <= 40000 {
		remunerationRange = 40000
	} else if remuneration <= 50000 {
		remunerationRange = 50000
	} else {
		remunerationRange = -1 // -1 is maker when the salary is over 50000
	}
	s.IncomeHistogram[remunerationRange]++

	updateData(&s.BaseRemuneration, salaryBase, s.Count)
	updateData(&s.OtherRemunerations, benefits, s.Count)
	updateData(&s.Discounts, discounts, s.Count)
	updateData(&s.Remunerations, remuneration, s.Count)
}

func calcBaseSalary(emp coleta.ContraCheque) (float64, float64, float64, float64) {
	var salaryBase float64
	var benefits float64
	var discounts float64
	for _, v := range emp.Remuneracoes.Remuneracao {
		if v.TipoReceita == coleta.Remuneracao_B && v.Natureza == coleta.Remuneracao_R {
			salaryBase += v.Valor
		} else if v.TipoReceita == coleta.Remuneracao_O && v.Natureza == coleta.Remuneracao_R {
			benefits += v.Valor
		} else if v.Natureza == coleta.Remuneracao_D {
			discounts += v.Valor
		}
	}
	discounts = math.Abs(discounts)
	remuneration := salaryBase + benefits - discounts
	return salaryBase, benefits, discounts, remuneration
}

func ativoInativo(ativo bool, orgao string) *string {
	// Atualmente conseguimos distinguir membros ativos apenas nos MPs
	if ativo && strings.Contains(orgao, "mp") {
		s := "A"
		return &s
	} else {
		return nil
	}
}

// Sanitizando as rubricas e nomes:
// deixando-as em minúsculo, sem acentos, pontuações, caracteres especiais e espaços duplos
func sanitizarItem(item string) string {
	// Converte para minúsculas
	item = strings.ToLower(item)

	// Remove acentos
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	item, _, _ = transform.String(t, item)

	// Remove pontuação
	item = strings.Map(func(r rune) rune {
		if strings.ContainsRune(".,;:!?-", r) {
			return -1
		}
		return r
	}, item)

	// Remove caracteres especiais
	re := regexp.MustCompile("[^a-zA-Z0-9 ]")
	item = re.ReplaceAllString(item, "")

	// Remove espaços duplos e espaços no início/final da string
	item = strings.Join(strings.Fields(item), " ")

	return item
}

// Realiza o download do json com as rubricas desambiguadas
func getItems() map[string]string {
	// json com rubricas desambiguadas
	const url = "https://raw.githubusercontent.com/dadosjusbr/desambiguador/refs/heads/main/desambiguacao_macro.json"

	res, err := http.Get(url)
	if err != nil {
		status.ExitFromError(status.NewError(status.ConnectionError, err))
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		status.ExitFromError(status.NewError(status.SystemError, err))
	}

	var itemJson map[string]string

	// unmarshall
	if err := json.Unmarshal(body, &itemJson); err != nil {
		status.ExitFromError(status.NewError(status.SystemError, fmt.Errorf("error unmarshalling 'rubricas.json': %w", err)))
	}

	return itemJson
}

// Com a lista de rubricas distintas da folha de contracheque (e seu somatório),
// comparamos com a lista de rubricas desambiguadas e criamos o json da coluna 'resumo'
// alocando o valor de cada rubrica a seu respectivo grupo.
func aggregatingItems(itemValues map[string]float64) models.ItemSummary {
	// Acessando o json com as rubricas desambiguadas
	items := getItems()

	rubricas := make(map[string]float64)

	// Iterando sobre as rubricas da folha de pagamento
	// e alocando o valor de cada rubrica a seu respectivo grupo (desambiguação macro)
	// Se a rubrica não estiver no json (variável items), i.e. não desambiguada,
	// alocamos o valor a "outras"
	for item, value := range itemValues {
		key, ok := items[item]
		if !ok {
			key = "outras"
		}
		key = strings.ReplaceAll(key, "-", "_")

		rubricas[key] += value
	}
	return rubricas
}
