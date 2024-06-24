package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net"
	"regexp"
	"sync"
	"time"
)

type Helper struct {
	AccessInfo map[string][]string
	UnimplementedAdminServer
	lastIDLogging      int
	SubscribersLogging *SubscribersEvents
	SubscribersStat    *SubscribersStat
	lastIDStat         int
}

type SubscribersEvents struct {
	sync.Mutex
	data map[int][]*Event
}

type SubscribersStat struct {
	sync.Mutex
	data map[int]*Stat
}

type Biz struct {
	UnimplementedBizServer
}

func StartMyMicroservice(ctx context.Context, addr string, data string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("can`t listen port: %v", err)
	}

	helper := NewHelper()

	err = json.Unmarshal([]byte(data), &helper.AccessInfo)
	if err != nil {
		lis.Close()
		return fmt.Errorf("can`t unmarshal ACL: %v", err)
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(helper.authUnaryInterceptor),
		grpc.StreamInterceptor(helper.authStreamInterceptor),
	)

	RegisterBizServer(server, NewBiz())
	RegisterAdminServer(server, helper)

	go func() {
		errServer := server.Serve(lis)
		if errServer != nil {
			server.Stop()
		}
	}()

	go func() {
		<-ctx.Done()
		// При завершении контекста приходит пустой интерфейс
		server.Stop()
	}()

	return nil
}

func (h *Helper) authUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "incorrect metadata")
	}
	consumer := md["consumer"]
	if len(consumer) != 1 {
		return nil, status.Error(codes.Unauthenticated, "incorrect metadata consumer")
	}

	err := h.authLogic(consumer[0], info.FullMethod)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	err = h.createEventLogic(consumer[0], info.FullMethod, ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	h.statLogic(consumer[0], info.FullMethod)

	return handler(ctx, req)
}

func (h *Helper) authStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	md, _ := metadata.FromIncomingContext(ss.Context())
	consumer := md["consumer"]

	err := h.authLogic(consumer[0], info.FullMethod)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	err = h.createEventLogic(consumer[0], info.FullMethod, ss.Context())
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	h.statLogic(consumer[0], info.FullMethod)

	return handler(srv, ss)
}

func (h *Helper) authLogic(consumer string, callMethod string) (err error) {

	method, ok := h.AccessInfo[consumer]
	if !ok {
		return errors.New("unknown consumer")
	}

	flag := false
	for _, val := range method {
		if val == callMethod {
			flag = true
			break
		}
		re := regexp.MustCompile(`^/main\.\w+/`)
		matches := re.FindStringSubmatch(callMethod)
		if len(matches) == 1 {
			temp := matches[0] + "*"
			if temp == val {
				flag = true
				break
			}
		}
	}
	if !flag {
		return errors.New("consumer haven`t access")
	}
	return nil
}

func (h *Helper) createEventLogic(consumer string, callMethod string, ctx context.Context) (err error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return errors.New("incorrect host")
	}
	host := p.Addr.String()
	newEvent := Event{
		Consumer:  consumer,
		Method:    callMethod,
		Timestamp: time.Now().Unix(),
		Host:      host,
	}
	h.SubscribersLogging.Lock()
	for ind := range h.SubscribersLogging.data {
		h.SubscribersLogging.data[ind] = append(h.SubscribersLogging.data[ind], &newEvent)
	}
	h.SubscribersLogging.Unlock()
	return nil
}

func (h *Helper) statLogic(consumer string, callMethod string) {

	h.SubscribersStat.Lock()
	for ind := range h.SubscribersStat.data {
		h.SubscribersStat.data[ind].Timestamp = time.Now().Unix()
		_, ok := h.SubscribersStat.data[ind].ByMethod[callMethod]
		if !ok {
			h.SubscribersStat.data[ind].ByMethod[callMethod] = 1
		} else {
			h.SubscribersStat.data[ind].ByMethod[callMethod]++
		}
		_, ok = h.SubscribersStat.data[ind].ByConsumer[consumer]
		if !ok {
			h.SubscribersStat.data[ind].ByConsumer[consumer] = 1
		} else {
			h.SubscribersStat.data[ind].ByConsumer[consumer]++
		}
	}
	h.SubscribersStat.Unlock()
}

func NewBiz() *Biz {
	return new(Biz)
}

func (b *Biz) Check(context.Context, *Nothing) (*Nothing, error) {
	return new(Nothing), nil
}

func (b *Biz) Add(context.Context, *Nothing) (*Nothing, error) {
	return new(Nothing), nil
}

func (b *Biz) Test(context.Context, *Nothing) (*Nothing, error) {
	return new(Nothing), nil
}

func NewHelper() *Helper {
	return &Helper{
		lastIDLogging: -1,
		lastIDStat:    -1,
		SubscribersLogging: &SubscribersEvents{
			data: map[int][]*Event{},
		},
		SubscribersStat: &SubscribersStat{
			data: map[int]*Stat{},
		},
	}
}

func (h *Helper) Logging(nothing *Nothing, in Admin_LoggingServer) error {

	h.SubscribersLogging.Lock()
	h.lastIDLogging++
	id := h.lastIDLogging
	h.SubscribersLogging.data[id] = make([]*Event, 0)
	h.SubscribersLogging.Unlock()

	for {
		h.SubscribersLogging.Lock()
		if len(h.SubscribersLogging.data[id]) != 0 {
			for _, event := range h.SubscribersLogging.data[id] {
				err := in.Send(event)
				if err != nil {
					return err
				}
			}
			h.SubscribersLogging.data[id] = h.SubscribersLogging.data[id][:0]
		}
		h.SubscribersLogging.Unlock()
	}
}

func (h *Helper) Statistics(int *StatInterval, in Admin_StatisticsServer) error {

	h.SubscribersStat.Lock()
	h.lastIDStat++
	id := h.lastIDStat
	statInit := &Stat{
		ByConsumer: map[string]uint64{},
		ByMethod:   map[string]uint64{},
	}
	h.SubscribersStat.data[id] = statInit
	h.SubscribersStat.Unlock()

	for {
		h.SubscribersStat.Lock()
		err := in.Send(h.SubscribersStat.data[id])
		if err != nil {
			return err
		}
		newStat := &Stat{
			ByConsumer: map[string]uint64{},
			ByMethod:   map[string]uint64{},
		}
		h.SubscribersStat.data[id] = newStat
		h.SubscribersStat.Unlock()
		time.Sleep(time.Duration(int.IntervalSeconds) * time.Second)
	}
}
