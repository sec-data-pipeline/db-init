package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/sec-data-pipeline/db-init/request"
	"github.com/sec-data-pipeline/db-init/storage"
)

var db storage.Database

func main() {
	err := db.CreateTables()
	if err != nil {
		panic(err)
	}
	for _, header := range headers {
		err := db.InsertHeader(header)
		if err != nil {
			panic(err)
		}
	}
	for _, cik := range sp500 {
		company, err := request.GetCompany(cik)
		if err != nil {
			panic(err)
		}
		err = db.InsertCompany(company)
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func init() {
	var secrets storage.Secrets
	var err error
	region := os.Getenv("REGION")
	if len(region) > 0 {
		awsSession, err := session.NewSession(&aws.Config{
			Region: aws.String(region),
		})
		if err != nil {
			panic(err)
		}
		secrets = storage.NewSecretsManager(awsSession, envOrPanic("SECRETS"))
	} else {
		secrets, err = storage.NewEnvLoader()
		if err != nil {
			panic(err)
		}
	}
	params, err := secrets.GetConnParams()
	if err != nil {
		panic(err)
	}
	db, err = storage.NewPostgres(params)
	if err != nil {
		panic(err)
	}
}

func envOrPanic(key string) string {
	value := os.Getenv(key)
	if len(value) < 1 {
		panic(errors.New(fmt.Sprintf("Environment variable '%s' must be specified", key)))
	}
	return value
}

var headers [][]string = [][]string{
	{"cash flow", "statement"},
	{"balance", "sheet"},
	{"earning", "statement"},
	{"operation", "statement"},
	{"income", "statement"},
}

var sp500 []string = []string{
	"0000066740",
	"0000091142",
	"0000001800",
	"0001551152",
	"0001467373",
	"0000796343",
	"0000002488",
	"0000874761",
	"0000004977",
	"0001090872",
	"0000002969",
	"0001559720",
	"0001086222",
	"0000915913",
	"0001035443",
	"0001097149",
	"0001579241",
	"0000352541",
	"0000899051",
	"0001652044",
	"0001652044",
	"0000764180",
	"0001018724",
	"0001748790",
	"0001002910",
	"0000006201",
	"0000004904",
	"0000004962",
	"0000005272",
	"0001053507",
	"0001410636",
	"0000820027",
	"0001037868",
	"0000318154",
	"0000820313",
	"0000006281",
	"0001013462",
	"0000315293",
	"0001841666",
	"0000320193",
	"0000006951",
	"0001521332",
	"0000947484",
	"0000007084",
	"0001596532",
	"0000354190",
	"0001267238",
	"0000732717",
	"0000731802",
	"0000769397",
	"0000008670",
	"0000866787",
	"0000915912",
	"0000008818",
	"0001069183",
	"0001701605",
	"0000009389",
	"0000070858",
	"0001390777",
	"0000701985",
	"0000010456",
	"0000010795",
	"0001067983",
	"0000764478",
	"0000012208",
	"0000842023",
	"0000875045",
	"0001364742",
	"0001393818",
	"0000012927",
	"0001075531",
	"0000908255",
	"0001037540",
	"0000885725",
	"0000014272",
	"0001730168",
	"0001383312",
	"0000079282",
	"0000014693",
	"0001316835",
	"0001996862",
	"0000813672",
	"0001590895",
	"0000906345",
	"0000016732",
	"0000927628",
	"0000721371",
	"0001170010",
	"0000815097",
	"0001783180",
	"0001596783",
	"0000018230",
	"0001374310",
	"0001138118",
	"0001402057",
	"0001306830",
	"0001140859",
	"0001071739",
	"0001130310",
	"0001324404",
	"0001043277",
	"0001100682",
	"0000316709",
	"0001091667",
	"0000093410",
	"0001058090",
	"0000896159",
	"0000313927",
	"0001739940",
	"0000020286",
	"0000723254",
	"0000858877",
	"0000831001",
	"0000759944",
	"0000021076",
	"0001156375",
	"0000811156",
	"0000021344",
	"0001058290",
	"0000021665",
	"0001166691",
	"0000028412",
	"0000023217",
	"0001163165",
	"0001047862",
	"0000016918",
	"0001868275",
	"0000711404",
	"0000900075",
	"0000024741",
	"0001755672",
	"0001057352",
	"0000909832",
	"0000858470",
	"0001051470",
	"0000277948",
	"0000026172",
	"0000064803",
	"0000313616",
	"0000940944",
	"0000927066",
	"0001725057",
	"0000315189",
	"0000027904",
	"0000818479",
	"0001090012",
	"0001093557",
	"0001539838",
	"0001297996",
	"0001393612",
	"0000029534",
	"0000935703",
	"0000715957",
	"0001286681",
	"0000029905",
	"0001751788",
	"0000882184",
	"0000936340",
	"0001326160",
	"0001666700",
	"0000915389",
	"0001551182",
	"0001065088",
	"0000031462",
	"0000827052",
	"0001099800",
	"0000712515",
	"0001156039",
	"0000059478",
	"0000032604",
	"0001463101",
	"0000065984",
	"0000821189",
	"0001352010",
	"0000033213",
	"0000033185",
	"0001101239",
	"0000906107",
	"0000920522",
	"0001001250",
	"0001370637",
	"0001095073",
	"0001711269",
	"0000072741",
	"0001109357",
	"0001324424",
	"0000746515",
	"0001289490",
	"0000034088",
	"0001048695",
	"0001013237",
	"0000814547",
	"0000815556",
	"0000034903",
	"0001048911",
	"0001136893",
	"0000035527",
	"0001274494",
	"0001031296",
	"0000798354",
	"0001175454",
	"0000037785",
	"0000037996",
	"0001262039",
	"0001659166",
	"0001754301",
	"0001754301",
	"0000038777",
	"0000831259",
	"0001121788",
	"0000749251",
	"0001932393",
	"0000849399",
	"0001474735",
	"0000040533",
	"0000040545",
	"0000040704",
	"0001467858",
	"0000040987",
	"0000882095",
	"0001123360",
	"0000320335",
	"0000886982",
	"0000045012",
	"0000874766",
	"0000046080",
	"0000860730",
	"0000765880",
	"0001000228",
	"0000047111",
	"0000004447",
	"0001645590",
	"0001585689",
	"0000859737",
	"0000354950",
	"0000773840",
	"0000048465",
	"0001070750",
	"0000004281",
	"0000047217",
	"0000048898",
	"0000049071",
	"0000049196",
	"0001501585",
	"0000051143",
	"0000832101",
	"0000874716",
	"0000049826",
	"0001110803",
	"0000879169",
	"0001699150",
	"0001145197",
	"0000050863",
	"0001571949",
	"0000051253",
	"0000051434",
	"0000051644",
	"0000896878",
	"0001035267",
	"0000914208",
	"0001687229",
	"0001478242",
	"0001020569",
	"0000728535",
	"0000898293",
	"0000779152",
	"0000052988",
	"0000200406",
	"0000833444",
	"0000019617",
	"0001043604",
	"0000055067",
	"0001944048",
	"0001418135",
	"0000091576",
	"0001601046",
	"0000055785",
	"0000879101",
	"0001506307",
	"0000319201",
	"0001637459",
	"0000056873",
	"0000202058",
	"0000920148",
	"0000707549",
	"0001679273",
	"0001300514",
	"0001336920",
	"0000920760",
	"0001707925",
	"0001335258",
	"0001065696",
	"0000936468",
	"0000060086",
	"0000060667",
	"0001397187",
	"0001489393",
	"0000036270",
	"0000101778",
	"0001510295",
	"0001278021",
	"0001048286",
	"0000062709",
	"0000916076",
	"0000062996",
	"0001141391",
	"0000891103",
	"0000063754",
	"0000063908",
	"0000927653",
	"0001613103",
	"0000310158",
	"0001326801",
	"0001099219",
	"0001037646",
	"0000789570",
	"0000827054",
	"0000723125",
	"0000789019",
	"0000912595",
	"0001682852",
	"0000851968",
	"0001179929",
	"0000024545",
	"0001103982",
	"0001280452",
	"0000865752",
	"0001059556",
	"0000895421",
	"0001285785",
	"0000068505",
	"0001408198",
	"0001120193",
	"0001002047",
	"0001065280",
	"0001164727",
	"0001564708",
	"0001564708",
	"0000753308",
	"0000320187",
	"0001111711",
	"0000072331",
	"0000702165",
	"0000073124",
	"0001133421",
	"0001513761",
	"0001013871",
	"0000073309",
	"0001045810",
	"0000906163",
	"0001413447",
	"0000898173",
	"0000797468",
	"0000878927",
	"0000029989",
	"0001097864",
	"0001039684",
	"0001341439",
	"0001781335",
	"0000075362",
	"0000075677",
	"0001327567",
	"0000813828",
	"0000076334",
	"0000723531",
	"0001590955",
	"0001633917",
	"0000077360",
	"0000077476",
	"0000078003",
	"0001004980",
	"0001413329",
	"0001534701",
	"0000764622",
	"0001038357",
	"0000713676",
	"0000945841",
	"0000079879",
	"0000922224",
	"0001126328",
	"0000080424",
	"0000080661",
	"0001045609",
	"0001137774",
	"0000788784",
	"0000857005",
	"0001393311",
	"0000822416",
	"0001604778",
	"0001050915",
	"0000804328",
	"0001022079",
	"0001037038",
	"0000720005",
	"0000101829",
	"0000726728",
	"0000910606",
	"0000872589",
	"0001281761",
	"0001060391",
	"0000943819",
	"0000031791",
	"0000315213",
	"0001024478",
	"0000084839",
	"0000882835",
	"0000745732",
	"0000884887",
	"0000064040",
	"0001108524",
	"0001034054",
	"0000087347",
	"0001137789",
	"0001032208",
	"0001373715",
	"0000089800",
	"0001063761",
	"0000004127",
	"0000091419",
	"0000091440",
	"0000092122",
	"0000092380",
	"0000093556",
	"0000829224",
	"0000093751",
	"0001022671",
	"0001757898",
	"0000310764",
	"0001601712",
	"0000883241",
	"0000096021",
	"0001283699",
	"0001113169",
	"0000946581",
	"0001116132",
	"0001389170",
	"0000027419",
	"0001385157",
	"0001094285",
	"0000096943",
	"0000097210",
	"0001318605",
	"0000097476",
	"0000217346",
	"0000097745",
	"0000109198",
	"0000916365",
	"0001466258",
	"0001260221",
	"0000086312",
	"0000864749",
	"0000092230",
	"0000860731",
	"0000100493",
	"0000036104",
	"0001543151",
	"0000074208",
	"0001403568",
	"0000100885",
	"0000100517",
	"0001090727",
	"0001067701",
	"0000731766",
	"0000352915",
	"0001035002",
	"0000740260",
	"0001967680",
	"0001014473",
	"0001442145",
	"0000732712",
	"0000875320",
	"0000103379",
	"0001792044",
	"0001705696",
	"0001403161",
	"0001396009",
	"0000011544",
	"0000943452",
	"0001618921",
	"0000104169",
	"0001744489",
	"0001437107",
	"0000823768",
	"0001000697",
	"0000783325",
	"0000072971",
	"0000766704",
	"0000105770",
	"0000106040",
	"0001732845",
	"0000106535",
	"0000106640",
	"0000107263",
	"0001140536",
	"0000277135",
	"0001174922",
	"0000072903",
	"0001524472",
	"0001041061",
	"0000877212",
	"0001136869",
	"0000109380",
	"0001555280",
	"0001477333",
}
