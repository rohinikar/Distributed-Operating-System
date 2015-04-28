import akka.actor._
import java.util.HashMap
import java.security.MessageDigest
import scala.util.Random
import java.lang.StringBuffer

case object stop
case class init(id : String,nodes : List[String], nodes_ref : List[ActorRef])
case class routeMessage(message : Message, nodeId : String)
case object MaxReached
case class MsgReached(no_of_hops :Int)
case class Requests(no_of_requests : Int)
case class Check_Requests(requestsMade: Int)
case class Sending(message : Message)

object Project3 extends App{

   var no_of_nodes=args(0).toInt
   var no_of_requests=args(1).toInt
   val system = ActorSystem("Main")
   var net = system.actorOf(Props(new Network(no_of_nodes)))
}

class Network(n : Int) extends Actor
{
  var arr_nodes : Array[Node]= Array[Node]()
  var nodes: List[String] = List()
  var idLength = 8; 
  var nodes_ref:List[ActorRef] = Nil
  var md = MessageDigest.getInstance("MD5")
  var nid:Int=0
  var max_reached_nodes : Int = 0
  var total_no_of_hops : Int = 0
  var total_no_of_messages : Int = 0
  var maxRequests = 0
   
   
   def receive = {
     case MaxReached =>
       {
         max_reached_nodes += 1
         if(max_reached_nodes == nodes.length)
         {
           
           if(this.total_no_of_messages!=0)
           {
           println("Process for all nodes completed, system exiting")
           println("Average number of hops:" + (total_no_of_hops/(100*total_no_of_messages)))
           context.system.shutdown();
           }
           else
             println("done")
         }
       }
     case MsgReached(no_of_hops : Int) =>
       {
         this.total_no_of_hops += no_of_hops
         this.total_no_of_messages += 1
       }
       case stop => {}
   }
    
    
    
    //code to generate node id's
    for(i <- 0 to n-1)
    {
        nodes_ref ::= context.actorOf(Props(new Node))
        var nid=node_Id()
        nodes ::=nid
        
    }
    //println("ids generated="+nodes)
    def startRouting(no_of_requests: Int)
    {
        maxRequests = no_of_requests
        for (i <- 0 to nodes.length-1)
        {
            nodes_ref(i) ! Requests(no_of_requests)
        }
        //println("after routing req sent")
    }
    
    //initialize routing table
    for(i <- 0 to nodes.length-1)
    {
      nodes_ref(i) ! init(nodes(i),nodes,nodes_ref)
    }
    startRouting(Project3.no_of_requests)
    def node_Id() : String = {

      var byteData : Array[Byte] = md.digest().toString().getBytes()
      var sb = new StringBuffer()
        for (i <- 0 to byteData.length-1) {
         sb.append(Integer.toString((byteData(i) & 0xff) + 0x100, 8).substring(2))
        }
        var sub_id=sb.toString().substring(0,8)      
      return sub_id
    } 
}

class Node extends Actor
{
  var nodeId: String=null
  var isAlive= true
  var no_of_hops : Int = 0
  var rTable=new RoutingTable
  var lnodes: List[String] = List()
  var totalNumOfHops : Int = 0
  var nodes_ref : List[ActorRef]= List()
  var newSortedList : Array[String]  = null
  var maxRequests : Int = 0
  var count : Int=0
  
  def StartReq(requests : Int)
  {
    maxRequests = requests
    nodes_ref(lnodes.indexOf(nodeId)) ! Check_Requests(0)
  }
  
  def CheckRequests(requests : Int)
  {
    var destNodeId = selectNode()
    nodes_ref(lnodes.indexOf(nodeId)) ! routeMessage(new Message(nodeId), destNodeId)
    
    if (requests == maxRequests)
    {
        Project3.net ! MaxReached
    }
    else    
    {
        Thread.sleep(1000)
        nodes_ref(lnodes.indexOf(nodeId)) ! Check_Requests(requests+1)
    }

  }
  
  def selectNode(): String = 
  {
  var list = lnodes
  var destNodeId : String = null
  do{
    destNodeId = Random.shuffle(lnodes).head
  }while(nodeId.equals(destNodeId))
  destNodeId
  }

  def receive ={
      
  case init(id : String, nodes: List[String], nodes_ref : List[ActorRef]) => 
    {
        this.nodeId=id
        this.lnodes=nodes
        var length : Int = this.lnodes.length
        this.nodes_ref=nodes_ref
        rTable.initialiseRoutingTable(nodeId, lnodes)
        rTable.initializeLeafSet(nodeId, lnodes)
    }
      case Requests(numRequests: Int) => 
        {
          StartReq(numRequests)
         
        }
    case Sending(message : Message) =>
        {
           Project3.net ! MsgReached(message.no_of_hops)
           
        }
     case Check_Requests(requestsMade: Int) => {
       
       CheckRequests(requestsMade)
     }
    case routeMessage(message : Message, destNodeId : String) =>
          {        
            message.no_of_hops += 1
            if(destNodeId.equals(nodeId))
            {
              totalNumOfHops += no_of_hops
            }
            if(destNodeId.toInt < nodeId.toInt)
            {
              var i : Int = 0
              var id : String = null
              var difference : Int = 0
              var minimum : Int = 0
              for(id <- rTable.lowerLeafSet)
                {
                  if(destNodeId.toInt == id.toInt)
                  {
                    nodes_ref(lnodes.indexOf(destNodeId)) ! Sending(message)
                  }
                  else
            {
            var commonCharacters : Int = rTable.matchDigits(nodeId,destNodeId)
            var rowNumber : Int = commonCharacters        
            //println("Common characters:"+rowNumber+"of node id"+nodeId+"dest id "+destNodeId)
            var colNumber : Int = nodeId.charAt(rowNumber).toString.toInt 
            var nextNodeId : String = rTable.rowSet(rowNumber)(colNumber)         
            if((nextNodeId != null)&&(nextNodeId.toInt == destNodeId.toInt))
            {
              nodes_ref(lnodes.indexOf(destNodeId)) ! Sending(message)
            }          
            else
            {
              var allNodeIdsList:List[String] = List()
              var id : String = null
              var i : Int = 0
              var diffValue : Int = math.abs(nodeId.toInt - destNodeId.toInt)
              var minimum : Int = 0
              for(id <- rTable.lowerLeafSet)
              {
                if(id!=null)
                allNodeIdsList ::= id
              }
              for(id <- rTable.higherLeafSet)
              {
                if(id!=null)
                allNodeIdsList ::= id
              }
              for(id <- rTable.rowSet(rowNumber))
              {
                if(id!=null)
                allNodeIdsList ::= id
              }
 
              this.newSortedList = allNodeIdsList.toArray.distinct
              scala.util.Sorting.quickSort(newSortedList)
              for(i <- 0 to (newSortedList.length - 1))
              {
              if(math.abs(newSortedList(i).toInt - destNodeId.toInt) < diffValue)
              {
                minimum = newSortedList(i).toInt
                diffValue = math.abs(newSortedList(i).toInt - destNodeId.toInt)
              }
              }
              nodes_ref(lnodes.indexOf(newSortedList(i))) ! routeMessage(message,destNodeId)
            }
            }
                }
            }
            if(destNodeId.toInt > nodeId.toInt)
            {
              var i : Int = 0
              var id : String = null
              var difference : Int = 0
              var minimum : Int = 0
                for(id <- rTable.higherLeafSet)
                {
                  if(destNodeId.toInt == id.toInt)
                  {
                    nodes_ref(lnodes.indexOf(destNodeId)) ! Sending(message)
                  }
                  else
            {
            var commonCharacters : Int = rTable.matchDigits(nodeId,destNodeId)
            var rowNumber : Int = commonCharacters        
            var colNumber : Int = nodeId.charAt(rowNumber).toString.toInt 
            var nextNodeId : String = rTable.rowSet(rowNumber)(colNumber)         
            if((nextNodeId != null)&&(nextNodeId.toInt == destNodeId.toInt))
            {
              nodes_ref(lnodes.indexOf(destNodeId)) ! Sending(message)
            }          
            else
            {
              var allNodeIdsList:List[String] = List()
              var id : String = null
              var i : Int = 0
              var diffValue : Int = math.abs(nodeId.toInt - destNodeId.toInt)
              var minimum : Int = 0
              for(id <- rTable.lowerLeafSet)
              {
                if(id!=null)
                allNodeIdsList ::= id
              }
              for(id <- rTable.higherLeafSet)
              {
                if(id!=null)
                allNodeIdsList ::= id
              }
              for(id <- rTable.rowSet(rowNumber))
              {
                if(id!=null)
                allNodeIdsList ::= id
              }
 
              this.newSortedList = allNodeIdsList.toArray.distinct
              scala.util.Sorting.quickSort(newSortedList)
              for(i <- 0 to (newSortedList.length - 1))
              {
              if(math.abs(newSortedList(i).toInt - destNodeId.toInt) < diffValue)
              {
                minimum = newSortedList(i).toInt
                diffValue = math.abs(newSortedList(i).toInt - destNodeId.toInt)
              }
              }
              nodes_ref(lnodes.indexOf(newSortedList(i))) ! routeMessage(message,destNodeId)
            }
            }
                }
            }          
            
      }
    }
    }
  
  
class RoutingTable
{
    val b=4
    val rowSize =Math.max((Math.pow(2,b)).toInt - 1, 8)  
    val columnSize = (Math.pow(2,b)).toInt - 1
    val leafSetSize = (Math.pow(2,b)).toInt
  var neighborSet: Array[String]= Array()
  var lowerLeafSet : Array[String] = Array()
  var higherLeafSet : Array[String] = Array()
  var nodes_ref:List[ActorRef] = Nil
  var Lvalue : Int = Math.pow(2,b).toInt
  var rowSet : Array[Array[String]] = Array.ofDim(rowSize, columnSize)
  var colSet : Array[Int] =null
  var countDigits : Int=0
  var row: Int=0
  
  def initializeLeafSet(node_id : String, node_ids : List[String]){
      var nodesInArray : Array[String] = node_ids.toArray
      scala.util.Sorting.quickSort(nodesInArray)
      var i : Int = 0
      var j : Int = 0
      var first : Int = 0
      var last : Int = 0
      for(i <- 0 to (node_ids.length - 1))
      {
        if(nodesInArray(i) == node_id)
        {
          if(i>0)
          {
            last = i-1
            if((i-(Lvalue/2)) >= 0)
            {
              first = i - (Lvalue/2)
            }
            else
              first = 0
          }
               for(j <- first to last)
               {
                 if(inDistance(node_id,nodesInArray(j)))
                     lowerLeafSet :+= nodesInArray(j)
               }
        if(i<node_ids.length - 1)
        {
          first = i + 1
          if((i+(Lvalue/2))<=(node_ids.length-1))
          {
            last = i + (Lvalue/2)
          }
          else
            last = node_ids.length-1
        }
        for(j <- first to last)
               {
                 if(inDistance(node_id,nodesInArray(j)))
                    higherLeafSet :+= nodesInArray(j)
               }
         }
      }
    }
  
  def initialiseRoutingTable(node_id : String, node_ids : List[String])
    {
      this.row=0
      colSet=Array.ofDim(columnSize)
      for (rows <- 0 to rowSize - 1)
      {
          for (cols <- 0 to columnSize - 1)
              rowSet(rows)(cols) = null
      }        
      for(i <- 0 to node_ids.length-1)
      {
        
        if(node_ids(i) != node_id && !CheckInLeafset(node_ids(i)))
        {
          this.row=matchDigits(node_id,node_ids(i))
          if(colSet(row)<columnSize)
              rowSet(row)(colSet(row))=node_ids(i)
          colSet(row)+=1
            
        }
      }
    }
    
    def matchDigits(node1 : String , node2 : String) : Int =
    {
      node1.zip(node2).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.length()
    }
    
    def inDistance(nod1 : String, nod2 : String) : Boolean ={
      var d : Int=0;
      d=(nod1.toInt - nod2.toInt);
      if(d<10000)
        return true
        
      else
          return false
    }
    
    def CheckInLeafset(nodeId: String) : Boolean = {
    
      for(i <- 0 to lowerLeafSet.length-1)
      {
      println("in check node"+lowerLeafSet(i)+"\t"+nodeId)
      if (nodeId.toInt == lowerLeafSet(i).toInt)
      {
        println("in check lower")
        return true
      }
    }
      for(i <- 0 to higherLeafSet.length-1)
      {
      if (nodeId.toInt == higherLeafSet(i).toInt)
      {
        println("in check higher")
        return true
      }
    }
    false
  }
    
    
  }
class Message(id: String){
  var msg="sending message"
  var no_of_hops = 0
}