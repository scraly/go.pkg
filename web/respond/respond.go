package respond

import (
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/scraly/go.pkg/log"
)

// -----------------------------------------------------------------------------

// With serialize the data with matching requested encoding
func With(w http.ResponseWriter, r *http.Request, code int, data interface{}) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	// Marshal response as json
	js, err := json.Marshal(data)
	if err != nil {
		WithError(w, r, http.StatusInternalServerError, err)
		return
	}

	// Set content type header
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// Write status
	w.WriteHeader(code)

	// Write response
	_, err = w.Write(js)
	log.CheckErrCtx(r.Context(), "Unable to write response", err)
}

// WithError serialize an error
func WithError(w http.ResponseWriter, r *http.Request, code int, err interface{}) {
	switch errObj := err.(type) {
	case string:
		With(w, r, code, &Status{
			Resource: &Resource{
				Type: "Error",
			},
			Code:    code,
			Message: errObj,
		})
	default:
		With(w, r, code, &Status{
			Resource: &Resource{
				Type: "Error",
			},
			Code:    code,
			Message: "Unable to process this request",
		})
	}
}
