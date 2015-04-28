import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import scala.util.Random
import scala.collection.mutable.ListBuffer
import sun.security.util.Length


case object gossip
case class msg_start(id:Int, ref_list:List[ActorRef], neighbors_List:List[Int])
case class PushSum (svalue : Double, wvalue : Double)
case object Stop
case class check(id:Int)



object Project2 extends App{
  val system=ActorSystem("main")
  val reference=system.actorOf(Props(new Master(args(0).toInt,args(1).toString,args(2).toString)),"master")
  }

class Master(nodes: Int, topo: String, alg: String) extends Actor
{
  var no_of_nodes:Int=nodes
  var topology:String=topo
  var algorithm:String=alg
  var nodes_ref:List[ActorRef] = Nil
  var converge_nodes:List[Int]=Nil
  var numNodesRoot:Int=0
  var flag2:Int=0
  var conv_id:Int=0
  var START_TIME=0L
  if (topology.equalsIgnoreCase("2DGrid") || topology.equalsIgnoreCase("Imperfect2DGrid"))
  {
    if (math.sqrt(no_of_nodes.toDouble) % 1 == 0)
    {
       numNodesRoot =  math.sqrt(no_of_nodes.toDouble).toInt
    }
    else
    {
          var newNumNodes : Int = no_of_nodes
          while (math.sqrt(newNumNodes.toDouble) % 1 != 0)
          {
           newNumNodes = newNumNodes + 1 
          }
          no_of_nodes = newNumNodes
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
       nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
     }
   }
   
   if (topology.equalsIgnoreCase("Imperfect2DGrid"))
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
       var random : Int = -1
       do
      {
         random = Random.nextInt(nodes_ref.length)
       for (x <- neighbors_list)
       {
         if (x == random)
         {
     random = -1
         }
       }
      } while (random == -1)
       neighbors_list ::= random
       nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
     }
   }  
   if (topology.equalsIgnoreCase("Line"))
   {
    for(i <- 0 to no_of_nodes-1)
    {
      var neighbors_list:List[Int] = Nil
            if(i>0)
            {
              neighbors_list ::= (i-1)
            }
            if(i<nodes_ref.length-1)
            {
              neighbors_list ::= (i+1)
            }
            nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
    }  
  }
  if (topology.equalsIgnoreCase("full")) 
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
            nodes_ref(i) ! msg_start(i, nodes_ref, neighbors_list)
          } 
  }
     
   if(algorithm.equalsIgnoreCase("gossip"))
   {
     START_TIME=System.currentTimeMillis
     println("--------START_TIME--------" +START_TIME)
     var random_node=Random.nextInt(nodes_ref.length)
     nodes_ref(random_node) ! gossip
   } 
    if(algorithm.equalsIgnoreCase("PushSum"))
    {
     START_TIME=System.currentTimeMillis
     println("--------START_TIME--------" +START_TIME)
     var random_node=Random.nextInt(nodes_ref.length)
     nodes_ref(random_node) ! PushSum(0,0)
    } 
    
  def receive ={  
  case check(id:Int) =>
    {
      conv_id=id
      if(!(converge_nodes.contains(conv_id)))
         {
          converge_nodes ::= conv_id
         }
      println(converge_nodes)
        if(flag2==0)
        {
          if((converge_nodes.length.toDouble/no_of_nodes)>=0.90)
          {
                   flag2=1
          println("-----------ALGORITHM CONVERGED----------")
    	 context.system.shutdown
    	 var END_TIME=System.currentTimeMillis
    	 println("-----END_TIME-----=" +END_TIME)
         println("TIME IN MILLISECONDS="+(END_TIME-START_TIME))
        }
      }
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
  def receive = 
  {
    case msg_start(id:Int, ref_list:List[ActorRef], neighbors_List:List[Int]) =>
      {
       node_id=id
       nodes_ref=ref_list
       neighbors=neighbors_List
       s = id
      }
    case `gossip` =>
      {
        if(counter<10)
        { 
          context.parent ! check(node_id)
        counter=counter+1        
        for(i <- 0 to 1)
          {
        var random_node=Random.nextInt(neighbors.length)
        nodes_ref(neighbors(random_node)) ! gossip  
        }
        }
                 
      }  
    case PushSum(svalue: Double, wvalue : Double) =>
      {
        context.parent ! check(node_id)
        oldSWValue = s/w
        s = (s + svalue)/2        
        w = (w + wvalue)/2
        newSWValue = s/w
        if (flag == 0)
        {
        if (newSWValue - oldSWValue > math.pow(10,(-10)) && counter<3)
        {
        counter = 0
        for(i <- 0 to 1)
        {
        random_node = Random.nextInt(neighbors.length)
        nodes_ref(neighbors(random_node)) ! PushSum(s,w)
        }
        }
       else if (newSWValue - oldSWValue < math.pow(10,(-10)) && counter>=3)
       {
        flag = 1
       }
       else if (newSWValue - oldSWValue < math.pow(10,(-10)) && counter<3)
       {
        counter = counter + 1
        for(i <- 0 to 1)
        {
        random_node = Random.nextInt(neighbors.length)
        nodes_ref(neighbors(random_node)) ! PushSum(s,w)
        }
       }
      }
      }
      }
  }
}