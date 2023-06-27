package urchin_status

const (
	Succeed         = 1001
	NotFound        = 1002
	Exist           = 1003
	PartialUploaded = 1004
	UnKnown         = 1404
)

var (
	StatusEnum2Json = map[int]string{
		1001: "Succeed",
		1002: "NotFound",
		1003: "Exist",
		1004: "Partial Uploaded",
		1404: "UnKnown",
	}
)

type UrchinStatus struct {
	StatusCode    int
	StatusMessage string
}

func NewStatus(code int) UrchinStatus {
	switch code {
	case Succeed:
		return UrchinStatus{
			StatusCode:    code,
			StatusMessage: StatusEnum2Json[code],
		}
	case NotFound:
		return UrchinStatus{
			StatusCode:    code,
			StatusMessage: StatusEnum2Json[code],
		}
	case Exist:
		return UrchinStatus{
			StatusCode:    code,
			StatusMessage: StatusEnum2Json[code],
		}
	case PartialUploaded:
		return UrchinStatus{
			StatusCode:    code,
			StatusMessage: StatusEnum2Json[code],
		}
	}

	return UrchinStatus{
		StatusCode:    UnKnown,
		StatusMessage: StatusEnum2Json[UnKnown],
	}
}
