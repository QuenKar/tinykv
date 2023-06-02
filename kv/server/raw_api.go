package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	resp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.GetContext())

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		resp.NotFound = true
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.NotFound = false
	resp.Value = val

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	modifies := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}

	resp := new(kvrpcpb.RawPutResponse)

	err := server.storage.Write(req.GetContext(), modifies)
	if err != nil {
		resp.Error = err.Error()
		return nil, err
	}

	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	modifies := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}

	resp := new(kvrpcpb.RawDeleteResponse)

	err := server.storage.Write(req.GetContext(), modifies)
	if err != nil {
		resp.Error = err.Error()
		return nil, err
	}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	resp := new(kvrpcpb.RawScanResponse)

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return nil, err
	}

	it := reader.IterCF(req.GetCf())
	defer it.Close()

	num := 0
	for it.Seek([]byte{1}); it.Valid(); it.Next() {
		if num >= int(req.GetLimit()) {
			break
		}

		item := it.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			resp.Error = err.Error()
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})

		num += 1
	}

	return resp, nil
}
