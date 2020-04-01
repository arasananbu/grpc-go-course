package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/arasananbu/grpc-go-course/greet/greetpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct{}

func (s *server) GreetWithDeadline(ctx context.Context, req *greetpb.GreetWithDeadlineRequest) (*greetpb.GreetWithDeadlineResponse, error) {
	fmt.Printf("Greet function with %v\n", req)
	firstName := req.GetGreeting().GetFirstName()
	// lastName := req.GetGreeting().GetLastName()

	for i := 0; i < 3; i++ {
		//fmt.Println(ctx.Err())
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("The client cancelled the request")
			return nil, status.Error(codes.Canceled, fmt.Sprintf("The client cancelled the request"))
		}
		time.Sleep(1 * time.Second)
	}

	resp := "Hello " + firstName
	res := greetpb.GreetWithDeadlineResponse{
		Result: resp,
	}
	return &res, nil

}

func (s *server) GreetEveryone(stream greetpb.GreetService_GreetEveryoneServer) error {
	fmt.Println("Inside GreetEveryone")

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Client Done sending
			return nil
		} else if err != nil {
			log.Fatalf("Error getting req GreetEveryone: %v", err)
			return err
		}
		firstName := req.GetGreeting().GetFirstName()
		sendErr := stream.Send(&greetpb.GreetEveryoneResponse{
			Result: "Hello " + firstName + "! ",
		})
		if sendErr != nil {
			log.Fatalf("Error while sending response to client: %v\n", sendErr)
			return sendErr
		}
	}
}

func (s *server) LongGreet(stream greetpb.GreetService_LongGreetServer) error {
	fmt.Println("LongGreet function")
	result := ""
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Reached EOF
			stream.SendAndClose(&greetpb.LongGreetResponse{
				Result: result,
			})
			break
		} else if err != nil {
			log.Fatalf("Error whike reading LogGreet req: %v", err)
		}
		firstName := req.GetGreeting().GetFirstName()
		result += "Hello " + firstName + "! "
	}
	return nil
}

func (s *server) Greet(ctx context.Context, req *greetpb.GreetRequest) (*greetpb.GreetResponse, error) {
	fmt.Printf("Greet function with %v\n", req)
	firstName := req.GetGreeting().GetFirstName()
	// lastName := req.GetGreeting().GetLastName()

	resp := "Hello " + firstName
	res := greetpb.GreetResponse{
		Result: resp,
	}
	return &res, nil
}

func (s *server) GreetManyTimes(req *greetpb.GreetManyTimesRequest, stream greetpb.GreetService_GreetManyTimesServer) error {
	fmt.Printf("GreetManyTimes with %v\n", req)
	firstName := req.GetGreeting().GetFirstName()
	// lastName := req.GetGreeting().GetLastName()

	for i := 0; i < 10; i++ {
		resp := "Hello " + firstName + " number " + strconv.Itoa(i)
		res := &greetpb.GreetManyTimesResponse{
			Result: resp,
		}
		stream.Send(res)
		time.Sleep(1000 * time.Millisecond)
	}
	return nil
}

type calcService struct{}

func (c *calcService) SquareRoot(ctx context.Context, req *greetpb.SquareRootRequest) (*greetpb.SquareRootResponse, error) {
	fmt.Println("Inside SquareRoot")
	number := req.GetNumber()

	if number < 0 {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Received a negative number: %v", number))
	}
	return &greetpb.SquareRootResponse{
		Result: math.Sqrt(float64(number)),
	}, nil
}

func (c *calcService) PrimeNumberDecompostionList(num int64) []int64 {
	res := []int64{}
	var k int64
	for k = 2; num > 1; k++ {
		fmt.Printf("k = %d, num%%k = %d, num = %d\n", k, (num % k), num)
		time.Sleep(1000 * time.Millisecond)
		if num%k == 0 {
			res = append(res, k)
			num = num / k
			fmt.Printf("PrimeNumberDecompostionList: %d", k)
		}
	}
	return res
}
func (c *calcService) ComputeAverage(stream greetpb.CalculatorService_ComputeAverageServer) error {
	fmt.Println("Inside ComputeAverage server")
	reqs := []int64{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Client done sending messages. Compute the average
			var sum int64 = 0
			for _, num := range reqs {
				sum += num
			}
			avg := float32(float32(sum) / float32(len(reqs)))
			stream.SendAndClose(&greetpb.ComputeAverageResponse{
				Result: avg,
			})
			break
		} else if err != nil {
			log.Fatalf("Error getting request: %v\n", err)
		}
		reqs = append(reqs, req.GetDigit_1())
	}

	return nil
}

func (c *calcService) PrimeNumberDecompostion(req *greetpb.PNDRequest, stream greetpb.CalculatorService_PrimeNumberDecompostionServer) error {
	fmt.Printf("PrimeNumberDecompostion with %v\n", req)
	digit_1 := req.GetDigit_1()
	decList := c.PrimeNumberDecompostionList(digit_1)

	var k int64
	for _, k = range decList {
		stream.Send(&greetpb.PNDResponse{
			Result: k,
		})
	}
	return nil
}

func (c *calcService) Sum(ctx context.Context, req *greetpb.SumRequest) (*greetpb.SumResponse, error) {
	fmt.Printf("In CalculateService Sum: %v\n", req)
	digit_1 := req.Digit_1
	digit_2 := req.Digit_2
	result := digit_1 + digit_2

	return &greetpb.SumResponse{
		Result: result,
	}, nil
}

func main() {
	fmt.Println("Hello World")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	greetpb.RegisterGreetServiceServer(s, &server{})
	greetpb.RegisterCalculatorServiceServer(s, &calcService{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
