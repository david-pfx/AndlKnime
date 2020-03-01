import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

/**
 * 
 */

/**
 * @author david
 *
 */
public class try_jexl {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        System.out.println("Hello, World");
        
        final JexlEngine jexl = new JexlBuilder().cache(512).strict(true).silent(false).create();
        
        // Assuming we have a JexlEngine instance initialized in our class named 'jexl':
        // Create an expression object for our calculation
        String calculateTax = "((G1 + G2 + G3) * 0.1) + G4";  // taxManager.getTaxCalc(); //e.g. 
        JexlExpression e = jexl.createExpression( calculateTax );

        // populate the context
        JexlContext context = new MapContext();
        
        context.set("G1", 100000.0);
        context.set("G2", 5555.0);
        context.set("G3", 4444.0);
        context.set("G4", -3333.33);
        // ...

        // work it out
        Number result = (Number) e.evaluate(context);
        System.out.println("Result = " + result);
	}

}
