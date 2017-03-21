package decode

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type DecodeSpec struct {
	Input          string
	ExpectedOutput map[string]interface{}
	ExpectedError  error
}

func TestKayveeDecoding(t *testing.T) {
	assert := assert.New(t)

	specs := []DecodeSpec{
		DecodeSpec{
			Input: `{"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": "",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		DecodeSpec{
			Input: `prefix {"a":"b"}`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": "",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		DecodeSpec{
			Input: `{"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "",
				"postfix": " postfix",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		DecodeSpec{
			Input: `prefix {"a":"b"} postfix`,
			ExpectedOutput: map[string]interface{}{
				"prefix":  "prefix ",
				"postfix": " postfix",
				"a":       "b",
				"Type":    "Kayvee",
			},
			ExpectedError: nil,
		},
		DecodeSpec{
			Input:          `prefix { postfix`, // Just a bracket, but no JSON in body
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  fmt.Errorf(""),
		},
		DecodeSpec{
			Input:          `prefix {"a:"b"} postfix`, // JSON missing a quote
			ExpectedOutput: map[string]interface{}{},
			ExpectedError:  &json.SyntaxError{},
		},
	}

	for _, spec := range specs {
		t.Log(fmt.Sprintf("Verifying spec with input: %s", spec.Input))
		fields, err := FieldsFromKayvee(spec.Input)
		if spec.ExpectedError != nil {
			assert.Error(err)
			assert.IsType(spec.ExpectedError, err)
		} else {
			assert.NoError(err)
		}
		assert.Equal(spec.ExpectedOutput, fields)
	}
}
