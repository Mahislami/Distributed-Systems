package main
import (
        "bufio"
        "fmt"
        "net"
        "os"
        "strings"
        "time"
        "strconv"
)

const RECEIVE_FROM_INPUT = 0
const PRODUCING_PACKET = 1
const SEND_TO_BROKER = 2
const RECEIVE_FROM_BROKER = 3
const CHECK_ACKNOWLEDGE_NUMBER = 4
const SEND_OVERFLOW_BIT = 5

var ERROR_ACKNOWLEDGE string= ""
var ERROR_PORT_NUMBER_EMPTY string = "CONNECTION ELEMNT ERROR"
var ERROR_BROKER_CONNECTION_LISTEN error = nil
var ERROR_BROKER_CONNECTION_ACCEPT error = nil
var ERROR_RECEIVE_FROM_BROKER error = nil
var ERROR_RECEIVE_FROM_INPUT error = nil
var queue[] string;
var status int = 0
var overFlow bool = false
var isAsync = false
var messageInex  int = 0
var packet string = ""
var text string = ""
var reader *bufio.Reader = nil
var netData string = ""
var messages = make(map[int]string)
var sentContinuous int = 1
var sentlimit int = 5

func enqueue(queue[] string, element string) []string {
  queue = append(queue, element); // Simply append to enqueue.
  return queue
}

func dequeue(queue[] string) ([]string) {
  return queue[1:]; // Slice off the element once it is dequeued.

}

func indexOfHashtag(s string) int {
    return strings.Index(s,"#")
}

func indexOfA(s string) int {
    return strings.Index(s,"a")
}
func indexOfN(s string) int {
    return strings.Index(s,"n")
}

func receiveFromBrokerFunc(socket net.Conn){
    for {
    	isFound := false
        if(status == RECEIVE_FROM_BROKER) {
            netData, ERROR_RECEIVE_FROM_BROKER = bufio.NewReader(socket).ReadString('\n')
            if ERROR_RECEIVE_FROM_BROKER != nil {
                    fmt.Println(ERROR_RECEIVE_FROM_BROKER)
                    return
            }
            if(netData == "y\n"){
            overFlow = true
            socket.Write([]byte("ov\n"))
            status = RECEIVE_FROM_BROKER
        } else if(string(netData[0]) == "n"){
                overFlow = false
                sentlimit,_ = strconv.Atoi(netData[indexOfN(netData)+1:indexOfHashtag(netData)])
                if(isAsync){
                    status = RECEIVE_FROM_INPUT
                } else {
                    fmt.Println("RECEIVE_FROM_BROKER")
                    status = RECEIVE_FROM_BROKER
                }
        } else {
            fmt.Print("RECEVIED FROM BROKER -> ")
            fmt.Print(netData)
            queue = enqueue(queue,"ACK#" + strconv.Itoa(messageInex)+ "\n")
            status = CHECK_ACKNOWLEDGE_NUMBER
            }
        }        
        
        if(status == CHECK_ACKNOWLEDGE_NUMBER){
            for i := 0; i < len(queue); i++ {
            	if(netData == queue[i]){
            		isFound = true
            	}
            }
            if(isFound){
                fmt.Println("CORRECT ACK NUMBER")
                status = RECEIVE_FROM_INPUT
            } else {
                socket.Write([]byte(messages[messageInex]))
                status = RECEIVE_FROM_BROKER
            }
        }
    }
}

func sendToBrokerFunc(socket net.Conn){
    for {
        if(!overFlow){
            if(status == RECEIVE_FROM_INPUT) {
            	time.Sleep(time.Second)
                fmt.Print("INSERT COMMAND >> ")
                reader = bufio.NewReader(os.Stdin)
                text, _ = reader.ReadString('\n')
                status = PRODUCING_PACKET
            }
            if(status == PRODUCING_PACKET) {
                messageInex++
                packet = "PACKET#" + strconv.Itoa(messageInex) + "*" + string(text[0]) + ":" + text[1:] + "\n"
                messages[messageInex] = packet
                status = SEND_TO_BROKER
            } 
            if(status == SEND_TO_BROKER) {
                if(sentContinuous == sentlimit){
                    status = SEND_OVERFLOW_BIT
                } else {
                    socket.Write([]byte(packet))
                    sentContinuous++
                    if(text[len(text)-2] == 'a'){
                        isAsync = true
                    	status = RECEIVE_FROM_INPUT
                    } else {
                        isAsync = false
                    	fmt.Print("WAITING FOR ACK ")
                    	fmt.Print(messageInex)
                    	fmt.Println("...")
                    	status = RECEIVE_FROM_BROKER
                    }
                }
            }
            if(status == SEND_OVERFLOW_BIT){
                fmt.Println("BROKER OVERFLOW...?")
                socket.Write([]byte("ov\n"))
                status = RECEIVE_FROM_BROKER
            }
        }
    }
}   

func main() {
    arguments := os.Args
    if len(arguments) == 1 {
            fmt.Println(ERROR_PORT_NUMBER_EMPTY)
            return
    }
    PORT := ":" + arguments[1]
    l, ERROR_BROKER_CONNECTION_LISTEN := net.Listen("tcp", PORT)
    if ERROR_BROKER_CONNECTION_LISTEN != nil {
            fmt.Println(ERROR_BROKER_CONNECTION_LISTEN)
            return
    }
    defer l.Close()
    fmt.Println("SERVER START LISTENING AT 127.0.0.1:1234")
    fmt.Println("-------------------------------------------")
    c, ERROR_BROKER_CONNECTION_ACCEPT := l.Accept()
    if ERROR_BROKER_CONNECTION_ACCEPT != nil {
            fmt.Println(ERROR_BROKER_CONNECTION_ACCEPT)
            return
    }
    fmt.Println("CONNECTION FROM BROKER HAS BEEN ESTABLISHED")
    fmt.Println("-------------------------------------------")
    go sendToBrokerFunc(c)
    go receiveFromBrokerFunc(c)
    time.Sleep(7205 * time.Second)        
}