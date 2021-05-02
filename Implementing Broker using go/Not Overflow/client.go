package main

import (
        "bufio"
        "fmt"
        "net"
        "os"
        "strconv"
        "strings"
)

const RECEIVE_READY_FROM_BROKER = 0
const SEND_READY_TO_BROKER = 1
const RECEIVE_FROM_BROKER = 2
const PARSING_PACKET = 3
const ANALYSE_PACKET = 4
const PRODUCING_PACKET = 5
const SEND_TO_BROKER = 6
var ERROR_PORT_NUMBER_EMPTY string = "CONNECTION ELEMNT ERROR"
var ERROR_BROKER_CONNECTION_DAIL error = nil
var ERROR_RECEIVE_FROM_BROKER error = nil
var ERROR_PARSING_PACKET error = nil

func indexOfColon(s string) int {
        return strings.Index(s,":")
}

func indexOfHashtag(s string) int {
        return strings.Index(s,"#")
}
func indexOfStar(s string) int {
        return strings.Index(s,"*")
}

func main() {
    
    arguments := os.Args
    if len(arguments) == 1 {
            fmt.Println(ERROR_PORT_NUMBER_EMPTY)
            return
    }
    CONNECT := arguments[1]
    c, ERROR_BROKER_CONNECTION_DAIL := net.Dial("tcp", CONNECT)
    if ERROR_BROKER_CONNECTION_DAIL != nil {
            fmt.Println(ERROR_BROKER_CONNECTION_DAIL)
            return
    }
    fmt.Println("CONNECTION TO BROKER HAS BEEN ESTABLISHED")
    fmt.Println("-----------------------------------------")
    var stauts int = 0
    var acknowledge int = 0
    var message string = ""
    var message2 string = ""
    var packet string = ""
    
    for {

        if(stauts == RECEIVE_READY_FROM_BROKER){

            message, ERROR_RECEIVE_FROM_BROKER := bufio.NewReader(c).ReadString('\n')
            if ERROR_RECEIVE_FROM_BROKER != nil {
                    fmt.Println(ERROR_RECEIVE_FROM_BROKER)
                    return
            }
            fmt.Print(message)
            fmt.Print("INSERT yes TO START TRANSFERING >> ")
            stauts = SEND_READY_TO_BROKER

        }

        if(stauts == SEND_READY_TO_BROKER){

            message, ERROR_RECEIVE_FROM_BROKER = bufio.NewReader(os.Stdin).ReadString('\n')
            if ERROR_RECEIVE_FROM_BROKER != nil {
                    fmt.Println(ERROR_RECEIVE_FROM_BROKER)
                    return
            }
            fmt.Fprintf(c, message)
            if (message == "yes\n"){
                    stauts = RECEIVE_FROM_BROKER
            } else {
                    stauts = RECEIVE_READY_FROM_BROKER
            }
        }

        if(stauts == RECEIVE_FROM_BROKER){

            message2, ERROR_RECEIVE_FROM_BROKER = bufio.NewReader(c).ReadString('\n')
            if ERROR_RECEIVE_FROM_BROKER != nil {
                    fmt.Println(ERROR_RECEIVE_FROM_BROKER)
                    return
            }
            stauts = PARSING_PACKET

        }

        if(stauts == PARSING_PACKET) {
        	//"Packet#" + strconv.Itoa(messageInex) + "*" + string(text[0]) + ":" + text[1:] + "\n"
            acknowledge, ERROR_PARSING_PACKET = strconv.Atoi(message2[indexOfHashtag(message2)+1:indexOfStar(message2)])
            fmt.Println(acknowledge)
            if ERROR_PARSING_PACKET != nil {
                   fmt.Println(ERROR_PARSING_PACKET)
                 return
            }
            stauts = ANALYSE_PACKET

        }

        if(stauts == ANALYSE_PACKET){
            fmt.Print("RECEIVED FROM BROKER -> " + message2)
            stauts = PRODUCING_PACKET

        }

        if( stauts == PRODUCING_PACKET) {
            //Packet Format Sample: ACK#56
            packet = "ACK#" + strconv.Itoa(acknowledge) + "\n"
            stauts = SEND_TO_BROKER

        }

        if(stauts == SEND_TO_BROKER){
            fmt.Fprintf(c, packet)
            stauts = RECEIVE_FROM_BROKER
        }
    }                    
}
       







