import java.io.*;

public class files {
	public static void main(String []args)
	{
	 
	try{
		RandomAccessFile in = new RandomAccessFile("C:/Users/ROHINI/Downloads/scala-SDK-3.0.1-vfinal-2.10-win32.win32.x86_64 (1)/eclipse/workspace/First/Logs/Boss.txt", "r");
		BufferedWriter writer = new BufferedWriter(new FileWriter("C:/Users/ROHINI/Downloads/scala-SDK-3.0.1-vfinal-2.10-win32.win32.x86_64 (1)/eclipse/workspace/First/Files/Graph.gv"));
		BufferedWriter secondwriter = new BufferedWriter(new FileWriter("C:/Users/ROHINI/Downloads/scala-SDK-3.0.1-vfinal-2.10-win32.win32.x86_64 (1)/eclipse/workspace/First/Files/Converge.gv"));
		String line;
		int i=0;
		writer.write("digraph{\n");
		secondwriter.write("digraph{\n");
		//while(true) {
		for(i=0;i<3;i++)
		{
		in.readLine();
		writer.newLine();
		secondwriter.newLine();
		}
		while((line = in.readLine()) != null) {
		String[] fields = line.split("\\s+");
		//String ninthField = fields[8];
		//String thirdField = fields[2];
		System.out.println(line);
		//System.out.println("Hey!!!"+fields[1]);
		//System.out.println("Hey!!!"+fields[8]);
		if(fields[2].equalsIgnoreCase("Boss"))
		{
		writer.write("Boss -> "+fields[8]+";");
		//writer.write(line);
		writer.newLine();
		fields = null;
		}
		else if
		(fields[4].equalsIgnoreCase("added"))
		{
			secondwriter.write("Boss -> "+fields[2]+";");
		//writer.write(line);
			secondwriter.newLine();
		fields = null;
		}
		} 
		//else {
		//Thread.sleep(2000); 
		
		//}
				//}
		writer.write("}\n");
		secondwriter.write("}\n");
		in.close();
		writer.close();
		secondwriter.close();
		}
	      catch (IOException e) {
        System.err.println("Error: " + e.getMessage());
    }
	       //catch (InterruptedException e1) {
	    	 //  System.err.println("Error: " + e1.getMessage());
	       //}
	}
}