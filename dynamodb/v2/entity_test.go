package dy_test

type entity struct {
	Id        string `json:"id"`
	GroupID   *int   `json:"groupID"`
	Enabled   *bool  `json:"enabled"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

func (e entity) IsEmpty() bool {
	return len(e.Id) == 0 && e.GroupID == nil && e.Enabled == nil && len(e.FirstName) == 0 && len(e.LastName) == 0
}
