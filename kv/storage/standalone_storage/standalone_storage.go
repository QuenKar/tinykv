package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	saPath := filepath.Join(dbPath, "standalone")

	return &StandAloneStorage{
		engine: engine_util.NewEngines(engine_util.CreateDB(saPath, false), nil, saPath, ""),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// TODO: start what?
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engine.Kv.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	//defer txn.Discard()
	return NewStandaloneReader(txn, s.engine.Kv), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	bs := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			bs.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			bs.DeleteCF(delete.Cf, delete.Key)
		}
	}

	err := s.engine.WriteKV(bs)
	if err != nil {
		return err
	}

	return nil
}
