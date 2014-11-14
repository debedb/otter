
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Putall {

	public Putall() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String []a){ 
		
		List l1 = new ArrayList();
		l1.add("one");
		l1.add("two");
		Map m1 = new HashMap();
		m1.put("a",l1);
		
		List l2 = new ArrayList();
		l2.add("ein");
		l2.add("zwei");
		Map m2 = new HashMap();
		m2.put("a",l2);
		
		
		Map big = new HashMap();
		big.putAll(m1);
		System.out.println(big);
		big.putAll(m2);
		System.out.println(big);
		
	}

}
