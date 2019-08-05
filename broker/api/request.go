package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// GetJSONBodyFromRequest gets json from request body and then parses into specified struct
func GetJSONBodyFromRequest(r *http.Request, t interface{}) error {
	decoder := json.NewDecoder(r.Body)
	if decoder != nil {
		err := decoder.Decode(&t)
		return err
	}
	return fmt.Errorf("could not parse request body")
}

// GetParamsFromRequest gets parameter value from the request。
// If the request method is neither GET or POST,returns an error.
// If there are multiple parameters with the same paramsName,returns the first value.
// If there does not have the value and required is false, returns an error, otherwise returns the defaultValue
func GetParamsFromRequest(paramsName string, r *http.Request, defaultValue string, required bool) (string, error) {
	if len(paramsName) == 0 {
		return "", fmt.Errorf("the params name must not be null")
	}
	var value string
	method := r.Method
	//get value from different object according to different request methods
	switch method {
	// the parameter value need to be parsed from the form when the request method is POST
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			return "", err
		}
		values := r.PostForm[paramsName]
		if len(values) > 0 {
			value = values[0]
		}
	// the parameter value need to be parsed from the url when the request method is GET
	case http.MethodGet, http.MethodDelete:
		values := r.URL.Query()[paramsName]
		if len(values) > 0 {
			value = values[0]
		}
	// default return error
	default:
		return "", fmt.Errorf("only GET/POST/DELETE methods are supported")
	}
	if len(value) > 0 {
		return value, nil
	}
	if !required {
		return defaultValue, nil
	}
	return "", fmt.Errorf("please input %s", paramsName)
}
