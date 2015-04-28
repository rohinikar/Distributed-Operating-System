import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import scala.util.Random
import scala.collection.mutable.ListBuffer
import sun.security.util.Length
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.Date
import java.io.IOException;


case object gossip
case class msg_start(id:Int, ref_list:List[ActorRef], neighbors_List:List[Int])
case object Stop
case class check(id:Int)




object Project4 extends App{
  var date=new Date;
  def time() : String =
  {
    var newtime:String = null
     newtime= date.getMonth()+"/"+date.getDate()+"\t\t"+date.getHours()+":"+date.getMinutes()+":"+date.getSeconds()
    return newtime
  }
  val system=ActorSystem("main")
  if(args.length==3)
  {
  val reference=system.actorOf(Props(new Master(args(0).toInt,args(1).toString,args(2).toString)),"master")
  }
  else
  {
  actorLog.writeToFile("./Logs/Boss.txt","DATESTAMP \t TIMESTAMP \t MESSAGE \n")
  actorLog.appendToFile("./Logs/Boss.txt",time()+"\t ERROR!!! : proper number of arguments not provided \n")
  println("Program will not run, please check logs")
  }
  }


class Master(nodes: Int, topo: String, alg: String) extends Actor 
{
  var date=new Date;
  //println(date.getMonth()+"/"+date.getDate()+"\t"+date.getHours()+":"+date.getMinutes()+":"+date.getSeconds())
  
 
  actorLog.writeToFile("./Logs/Boss.txt","DATESTAMP \t TIMESTAMP \t MESSAGE \t\t\t\t\t\t\tNODE SELECTED \n")
  actorLog.appendToFile("./Logs/Boss.txt","\t\t\t\t"+time()+"\t\t Boss Actor started \n")
  actorLog.writeToFile("./Files/Boss.gv", "\t digraph G{\n")
  actorLog.writeToFile("./Files/Convergence.gv", "\t digraph G1{\n")
  var no_of_nodes:Int=nodes
  var topology:String=topo
  var algorithm:String=alg
  var nodes_ref:List[ActorRef] = Nil
  var converge_nodes:List[Int]=Nil
  var numNodesRoot:Int=0
  var flag2:Int=0
  var conv_id:Int=0
  var START_TIME=0L
  var failed_node : Int = 0
  def time() : String =
  {
    var newtime:String = null
     newtime= date.getMonth()+"/"+date.getDate()+"\t\t"+date.getHours()+":"+date.getMinutes()+":"+date.getSeconds()
    return newtime
  }
  if (topology.equalsIgnoreCase("2DGrid"))
  {
    if (math.sqrt(no_of_nodes.toDouble) % 1 == 0)
    {
      actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNumber of nodes entered is a perfect square : "+no_of_nodes)
      numNodesRoot =  math.sqrt(no_of_nodes.toDouble).toInt
       
    }
    else
    {
          actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNumber of nodes entered is not a perfect square : "+no_of_nodes)
          var newNumNodes : Int = no_of_nodes
          while (math.sqrt(newNumNodes.toDouble) % 1 != 0)
          {
           newNumNodes = newNumNodes + 1 
          }
          no_of_nodes = newNumNodes
          actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tThe incremented number of nodes : "+no_of_nodes)
          numNodesRoot = math.sqrt(no_of_nodes.toDouble).toInt
          
    }
   }
  for(i <- 0 to no_of_nodes-1)
  {
    nodes_ref ::= context.actorOf(Props(new Node))
  } 
  if (topology.equalsIgnoreCase("2DGrid"))
   {
     for (i <-0 to no_of_nodes-1)
     {
       var neighbors_list:List[Int] = Nil
       if (!(i >= (no_of_nodes - numNodesRoot)))
       {
         neighbors_list ::= i + numNodesRoot
       }
       if (!(i < numNodesRoot))
       {
        neighbors_list ::= i - numNodesRoot 
       }
       if (!(i % numNodesRoot ==0))
       {
         neighbors_list ::= i - 1
       }
       if (!((i + 1) % numNodesRoot ==0))
       {
         neighbors_list ::= i + 1
       }
       actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has sent the worker "+i+" its initialization information"+"\t\t\t "+i)
       actorLog.appendToFile("./Files/Boss.gv","\tBoss ->"+i+";")
       nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
     }
   }
  else if (topology.equalsIgnoreCase("full")) 
  {	  
          for(i<- 0 to no_of_nodes-1)
          {            
             var neighbors_list:List[Int] = Nil
            for(j<- 0 to no_of_nodes-1)
            {
              if(j!=i)
              {
                neighbors_list ::= j
              }
            }
            actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has sent the worker "+i+" its initialization information"+"\t\t\t "+i)
            actorLog.appendToFile("./Files/Boss.gv","\tBoss ->"+i+";")
            nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
          } 
  }
  else
  {
    actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\t ERROR!!! : proper arguments not provided, check the argument provided for topology \n") 
    println("Program will not run, please check logs")
    context.system.shutdown
  }
//failure node code
   var random_node=Random.nextInt(nodes_ref.length)
   failed_node=random_node
   actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has chosen the worker "+failed_node+" randomly as the Failure node."+"\t\t\t "+failed_node)
   actorLog.appendToFile("./Files/Boss.gv","\t"+failed_node+"[color=red,style=filled];")
   context.stop(nodes_ref(failed_node))
   
   //END
   
   if(algorithm.equalsIgnoreCase("gossip"))
   {
     START_TIME=System.currentTimeMillis
     println("--------START_TIME--------" +START_TIME)
     var random_node=Random.nextInt(nodes_ref.length)
     while(nodes_ref(random_node) == nodes_ref(failed_node))
     {
     random_node=Random.nextInt(nodes_ref.length)
     
     }
     actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tBoss Actor has chosen the worker "+random_node+" randomly to start Gossip algorithm"+"\t\t "+random_node)
     actorLog.appendToFile("./Files/Boss.gv","\t"+random_node+"[color=green,style=filled];")
     actorLog.appendToFile("./Files/Boss.gv", "\t }\n")
     if(nodes_ref(random_node) == nodes_ref(failed_node) )
     {
       actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tERROR!!! Actor selection by Boss for initiating gossip algorithm is wrong")
       context.system.shutdown
     }
     nodes_ref(random_node) ! gossip
   } 
   else
   {
     actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\t ERROR!!! : proper arguments not provided, check the argument provided for algorithm \n") 
     println("Program will not run, please check logs")
     context.system.shutdown
   }
   
   
  def receive ={  
  case check(id:Int) =>
    {
      actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tchecking the convergence status")
      conv_id=id
      if(!(converge_nodes.contains(conv_id)))
         {
        actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNode"+conv_id+" is added to converge nodes list"+"\t\t\t\t\t\t "+conv_id)
        actorLog.appendToFile("./Files/Convergence.gv",+conv_id+"->")
        converge_nodes ::= conv_id
          
         }
        if(flag2==0)
        {
          if((converge_nodes.length.toDouble/no_of_nodes)>=0.90)
          {
                   flag2=1
         //actorLog.appendToFile("./Files/Convergence.gv","end;")
         actorLog.appendToFile("./Files/Convergence.gv", "\tend; }\n")
         actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\t***System is converged now***")
         actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tExiting from Boss Actor")    
         
         println("-----------ALGORITHM CONVERGED----------")
    	 context.system.shutdown
    	 var END_TIME=System.currentTimeMillis
    	 println("-----END_TIME-----=" +END_TIME)
         println("TIME IN MILLISECONDS="+(END_TIME-START_TIME))
         
        }
          else if((no_of_nodes - converge_nodes.length.toDouble) == 1)
          {
            flag2=1
            actorLog.appendToFile("./Files/Convergence.gv", "\tend; }\n")
            actorLog.appendToFile("./Logs/Boss.txt",time()+"\t\tNo more nodes can be added into the converge nodes list")
            context.system.shutdown
    	    var END_TIME=System.currentTimeMillis
    	    println("-----END_TIME-----=" +END_TIME)
    	    println("TIME IN MILLISECONDS="+(END_TIME-START_TIME))
    	    
          }
      }
    }
 // }
  }
}
class Node() extends Actor
{
  //properties of code class
  var node_id:Int=0
  var nodes_ref:List[ActorRef] = Nil
  var neighbors:List[Int] = Nil
  var counter:Int=0
  var s : Double = 0
    var w : Double = 1
    var oldSWValue : Double = 0
    var newSWValue : Double = 0
    var random_node : Int = 0
    var noOfRounds :Int = 0
    var noOfTimes : Int = 0
    var i : Int = 0    
    var flag : Int = 0
    var date=new Date;
    var fname:String=null
    var fname1:String=null
  def time() : String =
  {
    var newtime:String = null
     newtime= date.getMonth()+"/"+date.getDate()+"\t\t"+date.getHours()+":"+date.getMinutes()+":"+date.getSeconds()
    return newtime
  }
  def receive = 
  {
    case msg_start(id:Int, ref_list:List[ActorRef], neighbors_List:List[Int]) =>
      {
       node_id=id
       nodes_ref=ref_list
       neighbors=neighbors_List
       //s = id
       this.fname="./Logs/node"+id+" log.txt"
       var fp= new File(fname)
       actorLog.writeToFile(fname,"DATESTAMP \t TIMESTAMP \t\t MESSAGE \t\t\t\t\tSOURCE NODE \tDESTINATION NODE \n")
       actorLog.appendToFile(fname,"\t\t"+time()+"\t\tLog created for node "+node_id+"\n")
       this.fname1="./Files/node"+id+" log.gv"
       var fp1= new File(fname1)
       actorLog.writeToFile(fname1, "\t digraph G2{\n")
      }
    case `gossip` =>
      {
        
        if(counter<10)
        { 
         context.parent ! check(node_id)
         counter=counter+1    
         actorLog.appendToFile(fname,time()+"\t\t Gossip being performed for Node "+node_id+" & count for this node at present ="+counter)
         for(i <- 0 to 1)
          {
        var random_node=Random.nextInt(neighbors.length)
        actorLog.appendToFile(fname,time()+"\t\t Gossip being called for Node "+random_node+" from node "+node_id+"\t\t\t\t"+node_id+"\t\t"+random_node)
        actorLog.appendToFile(fname1,"\t"+node_id+"->"+random_node+";")
        nodes_ref(neighbors(random_node)) ! gossip  
        
          }
        }
        else {
              actorLog.appendToFile(fname1, "\t }") 
        }
      } 
      
   }
}
