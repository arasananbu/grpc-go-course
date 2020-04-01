package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/arasananbu/grpc-go-course/greet/greetpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	fmt.Println("Hello I`am a client")

	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}

	defer cc.Close()

	c := greetpb.NewGreetServiceClient(cc)
	// fmt.Printf("Created client: %f", c)
	doUnary(c)
	doServerStream(c)
	doClientStream(c)
	doBidirectionalStream(c)
	doUnaryDeadline(c, 5*time.Second)
	doUnaryDeadline(c, 1*time.Second)

	calcClient := greetpb.NewCalculatorServiceClient(cc)
	findSum(calcClient)
	primeNumberDec(calcClient)
	computeAverage(calcClient)
	doErrorUnary(calcClient)
}

func doErrorUnaryHandleErr(c greetpb.CalculatorServiceClient, n int32) {
	res, err := c.SquareRoot(context.Background(), &greetpb.SquareRootRequest{
		Number: n,
	})

	if err != nil {
		if respErr, ok := status.FromError(err); ok {
			fmt.Println(respErr.Code())
			fmt.Println(respErr.Message())
			if respErr.Code() == codes.InvalidArgument {
				fmt.Println("We probably sent negative number!")
			}
			return
		} else {
			log.Fatalf("Error calling SquareRoot")
		}
	}
	fmt.Printf("Result of %v: %v\n", n, res.GetResult())

}

func doErrorUnary(c greetpb.CalculatorServiceClient) {
	fmt.Println("Inside doErrorUnary")
	doErrorUnaryHandleErr(c, 10)
	doErrorUnaryHandleErr(c, -32)
}

func computeAverage(c greetpb.CalculatorServiceClient) {
	fmt.Println("Inside computeAverage")
	reqs := []*greetpb.ComputeAverageRequest{
		&greetpb.ComputeAverageRequest{
			Digit_1: 1,
		},
		&greetpb.ComputeAverageRequest{
			Digit_1: 2,
		},
		&greetpb.ComputeAverageRequest{
			Digit_1: 3,
		},
		&greetpb.ComputeAverageRequest{
			Digit_1: 4,
		},
	}
	stream, err := c.ComputeAverage(context.Background())
	if err != nil {
		log.Fatalf("Error getting stream computeAverage: %v\n", err)
	}
	for _, req := range reqs {
		stream.Send(req)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error getting response computeAverage: %v\n", err)
	}
	fmt.Printf("Response computeAverage: %f\n", resp.GetResult())
}

func primeNumberDec(c greetpb.CalculatorServiceClient) {
	fmt.Println("Finding primeNumberDec")
	stream, err := c.PrimeNumberDecompostion(context.Background(), &greetpb.PNDRequest{
		Digit_1: 120,
	})
	if err != nil {
		log.Fatalf("Error getting stream primeNumberDec: %v", err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error getting response primeNumberDec: %v", err)
		}
		fmt.Printf("PrimeNumberDecomposed: %d\n", res.GetResult())
	}
}

func findSum(c greetpb.CalculatorServiceClient) {
	fmt.Println("Finding Sum")
	req := &greetpb.SumRequest{
		Digit_1: 10,
		Digit_2: 20,
	}
	res, err := c.Sum(context.Background(), req)
	if err != nil {
		log.Fatalf("Error finding sum: %v", err)
	}
	fmt.Printf("The sum = %d\n", res.GetResult())
}

func doBidirectionalStream(c greetpb.GreetServiceClient) {
	fmt.Println("Starting doBidirectionalStream RPC")
	requests := []*greetpb.GreetEveryoneRequest{
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Arockia",
				LastName:  "Irudayaraj",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Nancy",
				LastName:  "John",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Navira",
				LastName:  "Arockia",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Anicha",
				LastName:  "Arockia",
			},
		},
	}

	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("Error creating stream doBidirectionalStream: %v\n", err)
	}

	waitCh := make(chan struct{})
	go func() {
		for _, req := range requests {
			fmt.Printf("Sending data: %s\n", req.Greeting.FirstName)
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("Error while sending request doBidirectionalStream: %v\n", err)
			}
			time.Sleep(1000 * time.Millisecond)
		}
		stream.CloseSend()
	}()

	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(waitCh)
				return
			} else if err != nil {
				log.Fatalf("Error getting response doBidirectionalStream: %v\n", err)
				close(waitCh)
				return
			}
			fmt.Printf("Response doBidirectionalStream: %s\n", resp.GetResult())
		}
	}()

	<-waitCh
}

func doServerStream(c greetpb.GreetServiceClient) {
	fmt.Println("Starting ServerStreaming RPC")

	stream, err := c.GreetManyTimes(context.Background(), &greetpb.GreetManyTimesRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Arockia",
			LastName:  "Irudayaraj",
		},
	})

	if err != nil {
		log.Fatalf("Error on GreetManyTimes RPC: %v", err)
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// We reached the end
			break
		} else if err != nil {
			log.Fatalf("Error while reading stream: %v", err)
		}
		log.Printf("Response form GreetManyTimes: %s\n", msg.GetResult())
	}
}

func doClientStream(c greetpb.GreetServiceClient) {
	fmt.Println("Starting doClientStream")
	requests := []*greetpb.LongGreetRequest{
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Arockia",
				LastName:  "Irudayaraj",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Nancy",
				LastName:  "John",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Navira",
				LastName:  "Arockia",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Anicha",
				LastName:  "Arockia",
			},
		},
	}
	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("Error while creating stream: %v", err)
	}
	for _, req := range requests {
		stream.Send(req)
		time.Sleep(1000 * time.Millisecond)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error getting response longGreet: %v", err)
	}
	fmt.Printf("LongGreetRes: %s\n", resp.GetResult())
}

func doUnary(c greetpb.GreetServiceClient) {
	fmt.Println("Starting Unary")
	req := &greetpb.GreetRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Arockia",
			LastName:  "Irudayaraj",
		},
	}
	res, err := c.Greet(context.Background(), req)
	if err != nil {
		log.Fatalf("Error while calling Greet RPC: %v", err)
	}

	log.Printf("Response %v", res.Result)

}

func doUnaryDeadline(c greetpb.GreetServiceClient, timeout time.Duration) {
	fmt.Println("Starting doUnaryDeadline")
	req := &greetpb.GreetWithDeadlineRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Arockia",
			LastName:  "Irudayaraj",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := c.GreetWithDeadline(ctx, req)
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			fmt.Println(statusErr.Code())
			fmt.Println(statusErr.Message())
			if statusErr.Code() == codes.DeadlineExceeded {
				fmt.Println("Context deadline exceeded")
			}
		} else {
			log.Fatalf("Error while calling GreetWithDeadline RPC: %v", statusErr)
		}
		return
	}

	log.Printf("Response %v", res.Result)

}
