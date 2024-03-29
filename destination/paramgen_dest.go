// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-sdk/cmd/paramgen

package destination

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"aws.accessKeyId": {
			Default:     "",
			Description: "AWS access key id.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"aws.bucket": {
			Default:     "",
			Description: "the AWS S3 bucket name.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"aws.region": {
			Default:     "",
			Description: "the AWS S3 bucket region",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"aws.secretAccessKey": {
			Default:     "",
			Description: "AWS secret access key.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"format": {
			Default:     "",
			Description: "the destination format, either \"json\" or \"parquet\".",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
				sdk.ValidationInclusion{List: []string{"parquet", "json"}},
			},
		},
		"prefix": {
			Default:     "",
			Description: "the S3 key prefix.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
	}
}
