package main
import (
        "bufio"
        "fmt"
        "net"
        "time"
        "os"
        "sync"
        "strings"
        "strconv"
)

const RECEIVE_FROM_SERVER = 0
const QUEUE_SERVER_MESSEGE = 1
const SEND_ACK_TO_SERVER = 2
const DEQUEUE_SERVER_MESSEGE = 3

const SEND_READY_TO_CLIENT = 0
const RECEIVE_READY_FROM_CLIENT = 1
const SEND_TO_CLIENT = 2
const RECEIVE_FROM_CLIENT = 3
var mu sync.Mutex

var ERROR_PORT_NUMBER_EMPTY string = "CONNECTION ELEMNT ERROR"
var ERROR_SERVER_CONNECTION_DAIL error = nil
var ERROR_CLIENT_CONNECTION_LISTEN error = nil
var ERROR_CLIENT_CONNECTION_ACCEPT error = nil
var ERROR_RECEIVE_FROM_CLIENT error = nil
var ERROR_RECEIVE_FROM_SERVER error = nil
var ERROR_PARSING_PACKET error = nil

var queue[] string;
var AckQueue[] string;
var messages = make(map[string]int)
var clientIndex int;
var clientSides[] net.Conn;
var serverSideStauts int = RECEIVE_FROM_SERVER
var clientSideStauts int = SEND_READY_TO_CLIENT
var serverMessage string = ""
var clientMessage string = ""
var clientReady  = [3]bool{false, false, false}

func enqueue(queue[] string, element string) []string {
  queue = append(queue, element); // Simply append to enqueue.
  return queue
}

func dequeue(queue[] string) ([]string) {
  return queue[1:]; // Slice off the element once it is dequeued.

}

func indexOfStar(s string) int {
  return strings.Index(s,"*")
}

func indexOfColon(s string) int {
  return strings.Index(s,":")
}

func serverSideFunc(serverSideInput net.Conn){
  for {
    if(serverSideStauts == RECEIVE_FROM_SERVER){
        serverMessage, ERROR_RECEIVE_FROM_SERVER = bufio.NewReader(serverSideInput).ReadString('\n')
        if(serverMessage == "ov\n"){
          if(len(queue) >= 4){
            fmt.Fprintf(serverSideInput, "y\n")
          } else {
            fmt.Fprintf(serverSideInput, "n" + strconv.Itoa(5 - len(queue)) + "#" +"\n")
          }
        } else {
            clientIndex, _ = strconv.Atoi(serverMessage[indexOfStar(serverMessage)+1:indexOfColon(serverMessage)])
            messages[serverMessage] = clientIndex
            if ERROR_RECEIVE_FROM_SERVER != nil {
              fmt.Println(ERROR_RECEIVE_FROM_SERVER)
              return
            }
            fmt.Print("RECEVIED FROM SERVER -> ")
            fmt.Println(serverMessage)
            serverSideStauts = QUEUE_SERVER_MESSEGE
          }
    }
    if(serverSideStauts == QUEUE_SERVER_MESSEGE){
      isFound := false
      for i := 0; i < len(queue); i++ {
          if(serverMessage == queue[i]){
            isFound = true
          }
      }
      if(!isFound){
        queue = enqueue(queue, serverMessage)
        fmt.Println("CURRENT QUEUE:")
        fmt.Println(queue)
      }
      serverSideStauts = RECEIVE_FROM_SERVER
    }
  }
}

func clientSideFunc(serverSideInput net.Conn, clientSideInputs[] net.Conn){
  for {
    if(len(queue) > 0){
      if(clientReady[messages[queue[0]]] == false){
        clientSideStauts = SEND_READY_TO_CLIENT
      } else {
          clientSideStauts = SEND_TO_CLIENT
      }
      if(clientSideStauts == SEND_READY_TO_CLIENT){
        clientSideInputs[messages[queue[0]]].Write([]byte("READY FOR TRANSFER...?\n"))
        clientSideStauts = RECEIVE_READY_FROM_CLIENT
      }

      if(clientSideStauts == RECEIVE_READY_FROM_CLIENT){
        clientMessage, ERROR_RECEIVE_FROM_CLIENT = bufio.NewReader(clientSideInputs[messages[queue[0]]]).ReadString('\n')
        if ERROR_RECEIVE_FROM_CLIENT != nil {
          fmt.Println(ERROR_RECEIVE_FROM_CLIENT)
          return
        }
        if(clientMessage == "yes\n"){
          clientSideStauts = SEND_TO_CLIENT
          clientReady[messages[queue[0]]] = true
        } else if (clientMessage == "no\n"){
            clientSideStauts = SEND_READY_TO_CLIENT
        }
      }
      if(clientSideStauts == SEND_TO_CLIENT){
        isNotReady := false
        for i := 0; i < len(queue); i++ {
        	if(clientReady[messages[queue[0]]] == true){
            	clientSideInputs[messages[queue[0]]].Write([]byte(queue[0]))
            	clientMessage, ERROR_RECEIVE_FROM_CLIENT = bufio.NewReader(clientSideInputs[messages[queue[0]]]).ReadString('\n')
            	if ERROR_RECEIVE_FROM_CLIENT != nil {
                	fmt.Println(ERROR_RECEIVE_FROM_CLIENT)
                	return
            	}
            	queue = dequeue(queue)
           		AckQueue = enqueue(AckQueue, clientMessage)
              fmt.Fprintf(serverSideInput, "n" + strconv.Itoa(5 - len(queue)) + "#" + "\n")
              time.Sleep(2*time.Second)
            	fmt.Fprintf(serverSideInput, AckQueue[0])
            	AckQueue = dequeue(AckQueue)
          } else {
            clientSideStauts = SEND_READY_TO_CLIENT
          	isNotReady = true
          	break
          }
        }
        if(!isNotReady){
          clientSideStauts = SEND_TO_CLIENT
        }
      }
    } else{
      continue
    }
  } 
}             

func main() {

  arguments := os.Args
  connectionNumber := len(arguments) - 2
  CONNECT := arguments[1]
  serverSide, ERROR_SERVER_CONNECTION_DAIL := net.Dial("tcp", CONNECT)
  if ERROR_SERVER_CONNECTION_DAIL != nil {
          fmt.Println(ERROR_SERVER_CONNECTION_DAIL)
          return
  }
  fmt.Println("CONNECTION TO SERVER HAS BEEN ESTABLISHED")
  fmt.Println("-----------------------------------------")
  for i := 0; i < connectionNumber ; i++ {
  	PORT := ":" + arguments[i+2]
  	l, ERROR_CLIENT_CONNECTION_LISTEN := net.Listen("tcp", PORT)
  	if ERROR_CLIENT_CONNECTION_LISTEN != nil {
          fmt.Println(ERROR_CLIENT_CONNECTION_LISTEN)
          return
  	}
  	defer l.Close()
    fmt.Print("BROKER START LISTENING AT ")
    fmt.Print("127.0.0.1:")
    fmt.Println(arguments[i+2])
    fmt.Println("-----------------------------------------")
  	clientSide, ERROR_CLIENT_CONNECTION_ACCEPT := l.Accept()
  	if ERROR_CLIENT_CONNECTION_ACCEPT != nil {
          fmt.Println(ERROR_CLIENT_CONNECTION_ACCEPT)
          return
  	}
    fmt.Print("NEW CLIENT FROM: ")
    fmt.Println(arguments[i+2])
    fmt.Println("-----------------------------------------")
  	clientSides = append(clientSides, clientSide)
  }
  go serverSideFunc(serverSide)
  go clientSideFunc(serverSide, clientSides)
  time.Sleep(7200 * time.Second)           
}