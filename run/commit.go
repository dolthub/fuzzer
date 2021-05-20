package run

// Commit represents either a commit or the working set in dolt.
type Commit struct {
	Hash        string
	Parents     []*Commit
	Tables      []*Table
	ForeignKeys []*ForeignKey
}

// Copy returns a deep copy of the calling commit.
func (c *Commit) Copy() (*Commit, error) {
	var err error
	parents := make([]*Commit, len(c.Parents))
	copy(parents, c.Parents)
	tables := make([]*Table, len(c.Tables))
	for i := 0; i < len(c.Tables); i++ {
		tables[i], err = c.Tables[i].Copy()
		if err != nil {
			return &Commit{}, nil
		}
	}
	foreignKeys := make([]*ForeignKey, len(c.ForeignKeys))
	for i := 0; i < len(c.ForeignKeys); i++ {
		foreignKeys[i] = c.ForeignKeys[i].Copy()
	}
	return &Commit{
		Hash:        c.Hash,
		Parents:     parents,
		Tables:      tables,
		ForeignKeys: foreignKeys,
	}, nil
}
