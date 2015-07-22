package storage

import "github.com/pjvds/tidy"

type IndexEntry struct {
	// The id of the message.
	Id MessageId
	// The name of the file that contains the message.
	Filename string
	// The file offset in bytes relative to that file.
	Offset int
	// The length in bytes.
	Length int
}

type Index struct {
	items []IndexEntry
}

func (this *Index) Append(messages *MessageSet) {
	for _, entry := range messages.entries {
		entry := IndexEntry{
			Id:     entry.Id,
			Offset: entry.Offset, // TODO: this should not equal the set offset
			Length: entry.Length,
		}

		this.items = append(this.items, entry)
		logger.With("entry", tidy.Stringify(entry)).Debug("index item appended")
	}
}

func (this *Index) FindLastLessOrEqual(id MessageId) (IndexEntry, bool) {
	// TODO: add better search algoritm, maybe something as easy as binary search.
	for index, entry := range this.items {
		if entry.Id < id {
			continue
		}
		if entry.Id == id {
			return entry, true
		}
		if index > 0 {
			return this.items[index-1], true
		}
	}

	logger.Withs(tidy.Fields{
		"id":      id,
		"entries": tidy.Stringify(this.items),
	}).Debug("no index entry found")

	return IndexEntry{}, false
}
