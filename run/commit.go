package run

// Commit represents either a commit or the working set in dolt.
type Commit struct {
	Parent      *Branch
	Hash        string
	Parents     []*Commit
	Tables      []*Table
	ForeignKeys []*ForeignKey
}

// Copy returns a deep copy of the calling commit.
func (c *Commit) Copy() (*Commit, error) {
	var err error
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
		Parent:      c.Parent,
		Hash:        c.Hash,
		Parents:     c.Parents,
		Tables:      tables,
		ForeignKeys: foreignKeys,
	}, nil
}
