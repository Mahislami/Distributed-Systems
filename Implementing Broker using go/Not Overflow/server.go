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

var ERROR_ACKNOWLEDGE string= ""
var ERROR_PORT_NUMBER_EMPTY string = "CONNECTION ELEMNT ERROR"
var ERROR_BROKER_CONNECTION_LISTEN error = nil
var ERROR_BROKER_CONNECTION_ACCEPT error = nil
var ERROR_RECEIVE_FROM_BROKER error = nil
var ERROR_RECEIVE_FROM_INPUT error = nil
var queue[] string;
var status int = 0
var messageInex  int = 0
var packet string = ""
var text string = ""
var reader *bufio.Reader = nil
var netData string = ""
var messages = make(map[int]string)

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

func receiveFromBrokerFunc(socket net.Conn){
        for {
        		isFound := false
                if(status == RECEIVE_FROM_BROKER) {
                        netData, ERROR_RECEIVE_FROM_BROKER = bufio.NewReader(socket).ReadString('\n')
                        
                        //time.Sleep(2*time.Second)
                        //fmt.Println
                        if ERROR_RECEIVE_FROM_BROKER != nil {
                                fmt.Println(ERROR_RECEIVE_FROM_BROKER)
                                return
                        }
                        fmt.Print("RECEVIED FROM BROKER -> ")
                        fmt.Print(netData)
                        queue = enqueue(queue,"ACK#" + strconv.Itoa(messageInex)+ "\n")
                        status = CHECK_ACKNOWLEDGE_NUMBER
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
                               // ERROR_ACKNOWLEDGE := "ERROR! RESEND MESSAGE: " + strconv.Itoa(messageInex)
                               //fmt.Println(messages[messageInex])
                                socket.Write([]byte(messages[messageInex]))
                                //messageInex--
                                status = RECEIVE_FROM_BROKER
                                //fmt.Println(ERROR_ACKNOWLEDGE)
                        }
                }
        }
}

func sendToBrokerFunc(socket net.Conn){
        for {
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
                    socket.Write([]byte(packet))
                    if(text[len(text)-2] == 'a'){
                    	status = RECEIVE_FROM_INPUT
                    } else {
                    	fmt.Print("WAITING FOR ACK ")
                    	fmt.Print(messageInex)
                    	fmt.Println("...")
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






