package plugin

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"context"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/plugin-sdk/v3/plugins/source"
	"github.com/cloudquery/plugin-sdk/v3/schema"

	"github.com/cloudquery/plugin-pb-go/specs"
	"github.com/cloudquery/plugin-sdk/v3/types"

	// 	"github.com/cloudquery/plugin-sdk/v3/transformers"

	"github.com/rs/zerolog"
	// "reflect"
	// "errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jmespath/go-jmespath"
)

var (
	Version = "Development"
)

// @todo support s3 source
type FileSpec struct {
	Table    string          `json:"table,omitempty"`
	Path     string          `json:"path,omitempty"`
	Jmespath string          `json:"jmespath,omitempty"`
	Indexes  map[string]bool `json:"indexes,omitempty"`
	Except   []string        `json:"except,omitempty"`
	Only     []string        `json:"only,omitempty"`
}

type Spec struct {
	Files []FileSpec `json:"files,omitempty"`
}

func (s Spec) Validate() error {
	return nil
}

func NewClient(ctx context.Context, logger zerolog.Logger, s specs.Source, opts source.Options) (schema.ClientMeta, error) {
	fmt.Println("NEW CLIENT")

	fmt.Println(s)
	spec := &Spec{}
	if err := s.UnmarshalSpec(spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal gitlab spec: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	fmt.Println(spec.Files)

	return &Client{
		logger: logger,
		spec:   s,
		Files:  spec.Files,
	}, nil
}

type Client struct {
	logger zerolog.Logger
	spec   specs.Source
	Files  []FileSpec
}

func (c *Client) Logger() *zerolog.Logger {
	return &c.logger
}

func (c *Client) ID() string {
	return c.spec.Name
}

func getColumn(name string, x interface{}) schema.Column {
	var vtype arrow.DataType
	suffix := ":?"
	switch x.(type) {
	case bool:
		vtype = arrow.FixedWidthTypes.Boolean
		suffix = ":bool"
	case int:
		vtype = arrow.PrimitiveTypes.Int64
		suffix = ":int"
	case float64:
		vtype = arrow.PrimitiveTypes.Float64
		suffix = ":float"
	case string:
		vtype = arrow.BinaryTypes.String
		suffix = ":str"
	case chan int:
		vtype = arrow.PrimitiveTypes.Int64
		suffix = ":int"
	case map[string]interface{}:
		vtype = types.ExtensionTypes.JSON
		suffix = ":json"
	case []interface{}:
		vtype = types.ExtensionTypes.JSON
		suffix = ":json"
	default:
		vtype = arrow.BinaryTypes.String
		suffix = ":str"
	}

	return schema.Column{
		Name:     name + suffix,
		Type:     vtype,
		Resolver: schema.PathResolver(name),
	}
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func getColumns(result interface{}, spec FileSpec) []schema.Column {
	var one map[string]interface{}
	switch result.(type) {
	case []interface{}:
		tmp := result.([]interface{})
		if len(tmp) == 0 {
			return []schema.Column{}
		}
		one = tmp[0].(map[string]interface{})
	case map[string]interface{}:
		one = result.(map[string]interface{})
	}

	columns := []schema.Column{}
	for k, v := range one {
		// Is key explicitly omitted?
		if stringInSlice(k, spec.Except) {
			continue
		}

		// Is key part of explicitly included via only?
		if !stringInSlice(k, spec.Only) {
			continue
		}
		col := getColumn(k, v)
		columns = append(columns, col)
	}
	return columns
}

func readFromSpec(spec FileSpec) (interface{}, error) {
	var readable io.Reader
	var err error
	if strings.HasPrefix(spec.Path, "s3://") {
		parts := strings.Split(spec.Path, "/")
		bucket := parts[2]
		key := strings.Join(parts[3:], "/")

		sess, err := session.NewSession()

		if err != nil {
			return nil, err
		}

		svc := s3.New(sess, &aws.Config{
			DisableRestProtocolURICleaning: aws.Bool(true),
		})
		r, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

		if err != nil {
			return nil, err
		}
		readable = r.Body
	} else {
		readable, err = os.Open(spec.Path)
		defer readable.(*os.File).Close()
		if err != nil {
			return nil, err
		}
		fmt.Println("Successfully opened " + spec.Path)
	}

	data, err := decodeJson(readable)
	return jmespath.Search(spec.Jmespath, data)
}

func decodeCsv(r io.Reader) (interface{}, error) {
	reader := csv.NewReader(r)

	readall, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	results := []map[string]string{}
	header := []string{}
	for lineNum, record := range readall {

		// for first row, build the header slice
		if lineNum == 0 {
			for i := 0; i < len(record); i++ {
				header = append(header, strings.TrimSpace(record[i]))
			}
		} else {
			line := map[string]string{}
			for i := 0; i < len(record); i++ {
				line[header[i]] = record[i]
			}
			results = append(results, line)
		}
	}

	return results, nil
}

func decodeJson(r io.Reader) (interface{}, error) {
	values := []interface{}{}
	d := json.NewDecoder(r)
	for {
		// Decode one JSON document.
		var v interface{}
		err := d.Decode(&v)

		if err != nil {
			// io.EOF is expected at end of stream.
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}

		values = append(values, v)
	}
	return values, nil
}

func dynamicTables(ctx context.Context, c schema.ClientMeta) (schema.Tables, error) {
	client := c.(*Client)
	tables := []*schema.Table{}

	for _, spec := range client.Files {
		result, errJson := readFromSpec(spec)

		if errJson != nil {
			panic(errJson)
		}

		tables = append(tables, &schema.Table{
			Name: spec.Table,
			Resolver: func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- any) error {
				if errJson == nil {
					res <- result
				}
				return errJson
			},
			Columns: getColumns(result, spec),
		})
	}

	return tables, nil
}

func Plugin() *source.Plugin {

	p := source.NewPlugin(
		"file",
		Version,
		[]*schema.Table{},
		NewClient,
		source.WithDynamicTableOption(dynamicTables),
	)

	return p
}
