package server

import (
	"context"
	"fmt"
	"net"

	pulse "github.com/bbvtaev/pulse-core"
	pb "github.com/bbvtaev/pulse-core/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server реализует gRPC-интерфейс PulseDB поверх storage engine.
type Server struct {
	pb.UnimplementedPulseDBServer
	db *pulse.DB
}

func New(db *pulse.DB) *Server {
	return &Server{db: db}
}

// Listen запускает gRPC-сервер на заданном адресе (например, ":50051").
func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterPulseDBServer(grpcSrv, s)

	return grpcSrv.Serve(lis)
}

func (s *Server) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	var written int32

	for _, ser := range req.Series {
		points := make([]pulse.Point, len(ser.Points))
		for i, p := range ser.Points {
			points[i] = pulse.Point{Timestamp: p.Timestamp, Value: p.Value}
		}
		if err := s.db.WriteBatch(ser.Metric, ser.Labels, points); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "write: %v", err)
		}
		written++
	}

	return &pb.WriteResponse{Written: written}, nil
}

func (s *Server) Query(_ context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	results, err := s.db.Query(req.Metric, req.Labels, req.From, req.To)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	pbSeries := make([]*pb.Series, 0, len(results))
	for _, r := range results {
		pts := make([]*pb.DataPoint, len(r.Points))
		for i, p := range r.Points {
			pts[i] = &pb.DataPoint{Timestamp: p.Timestamp, Value: p.Value}
		}
		pbSeries = append(pbSeries, &pb.Series{
			Metric: r.Metric,
			Labels: r.Labels,
			Points: pts,
		})
	}

	return &pb.QueryResponse{Series: pbSeries}, nil
}

func (s *Server) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.db.Delete(req.Metric, req.Labels, req.From, req.To); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &pb.DeleteResponse{}, nil
}

// Subscribe — server-side streaming: шлёт DataPoint в реальном времени.
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.PulseDB_SubscribeServer) error {
	id, ch := s.db.Subscribe(req.Metric, req.Labels)
	defer s.db.Unsubscribe(id)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case p, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.DataPoint{
				Timestamp: p.Timestamp,
				Value:     p.Value,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Status:  "ok",
		Version: pulse.Version,
	}, nil
}
