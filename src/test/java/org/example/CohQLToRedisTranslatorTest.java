package org.example;

import net.sf.jsqlparser.JSQLParserException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Map.entry;

public class CohQLToRedisTranslatorTest {
    private static final Map<String, String> FIELD_TYPES = Map.ofEntries(
            entry("propName1", "TEXT"),
            entry("propName2", "TEXT"),
            entry("propName3", "TEXT"),
            entry("propName4", "TEXT"),
            entry("propName5", "TEXT"),
            entry("propName6", "TEXT"),
            entry("propName7", "TEXT"),
            entry("propName8", "TEXT"),
            entry("propName9", "TEXT"),
            entry("propName10", "TEXT"),
            entry("propName11", "TEXT"),
            entry("propName12", "TEXT"),
            entry("propName13", "TEXT"),
            entry("category", "TEXT"),
            entry("status", "TEXT"),
            entry("username", "TEXT"),
            entry("department", "TEXT"),
            entry("role", "TEXT"),
            entry("price", "NUMERIC"),
            entry("amount", "NUMERIC"),
            entry("productId", "NUMERIC"),
            entry("customerId", "NUMERIC")   ,
            entry("location", "TEXT") ,
            entry("restockDate", "TEXT"),
            entry("orderDate", "TEXT"),
            entry("lastPurchase", "TEXT"),
            entry("city", "TEXT")   ,
            entry("name", "TEXT")
    );
    public static void main(String[] args) throws JSQLParserException {
        CohQLToRedisTranslator translator = new CohQLToRedisTranslator(FIELD_TYPES);

        // Test 1: Simple equality
        String cohql1 = "propName1 = 'value1'";
        System.out.println("Test 1: " + translator.translate(cohql1));
        // Expected: @propName1:value1

        // Test 2: AND, OR, and grouping
        String cohql2 = "(propName1 = 'value1' AND propName2 = 12) OR (propName3 != 'value3')";
        System.out.println("Test 2: " + translator.translate(cohql2));
        // Expected: ((@propName1:value1 @propName2:12)|(-@propName3:value3))

        // Test 3: Ranges and IN
        String cohql3 = "propName4 >= 10 AND propName5 IN (1, 2, 3)";
        System.out.println("Test 3: " + translator.translate(cohql3));
        // Expected: (@propName4:[10 +inf] @propName5:{1|2|3})

        // Test 4: NOT IN and LIKE
        String cohql4 = "propName6 NOT IN ('a', 'b') OR propName7 LIKE '%foo%'";
        System.out.println("Test 4: " + translator.translate(cohql4));
        // Expected: (-@propName6:{a|b}|@propName7:*foo*)

        // Test 5: Full complex example
        String cohql5 = "(propName1 = 'value1') and (propName2 = 12) and (propName3 != 'value3') and (propName4 != 13) and (propName5 >= 14) and (propName6 > 15) or (propName7 < 16) or (propName8 <= 17) or (propName9 in ('firstValue9', 'value9.1', 'value9.2', 'value9.3')) or (propName10 in (18, 19, 20, 21)) or (propName11 not in ('firstValue11', 'value11.1', 'value11.2', 'value11.3')) or (propName12 not in (22, 23, 24, 25)) or (propName13 like 'value13')";
        System.out.println("Test 5: " + translator.translate(cohql5));
        // Expected: (see previous assistant's example)

        // Test 6: Full complex example
        String cohql6 = "SELECT * FROM Users WHERE username = 'alice'";
        System.out.println("Test 6: " + translator.translate(cohql6));
        // Expected: FT.SEARCH Users_index @username:alice

        String cohql7 = "SELECT * FROM Orders WHERE customerId = 1001 AND status = 'PAID' AND amount > 50";
        System.out.println("Test 7: " + translator.translate(cohql7));
        // Expected: FT.SEARCH Orders_index @customerId:1001 @status:PAID @amount:[(50 +inf]

        String cohql8 = "SELECT * FROM Products WHERE (category = 'electronics' OR category = 'appliances') AND price < 1000";
        System.out.println("Test 8: " + translator.translate(cohql8));
        // Expected: FT.SEARCH Products_index ((@category:electronics|@category:appliances) @price:[-inf (1000])

        String cohql9 = "SELECT * FROM Employees WHERE department IN ('HR', 'IT', 'Finance') AND name LIKE '%son%' AND role NOT IN ('Intern', 'Contractor')";
        System.out.println("Test 9: " + translator.translate(cohql9));
        // Expected: FT.SEARCH Employees_index @department:{HR|IT|Finance} @name:*son* -@role:{Intern|Contractor}

        String cohql10 = "SELECT * FROM Transactions WHERE (status = 'FAILED' OR status = 'CANCELLED') AND amount >= 100 AND amount <= 5000 AND description LIKE '%refund%' AND userId != 42";
        System.out.println("Test 10: " + translator.translate(cohql10));
        // Expected: FT.SEARCH Transactions_index ((@status:FAILED|@status:CANCELLED) @amount:[100 5000] @description:*refund* -@userId:42)

        String cohql11 = "SELECT * FROM Inventory WHERE (location = 'NY' AND (stock < 50 OR restockDate <= '2025-06-01')) OR (location = 'LA' AND stock > 100)";
        System.out.println("Test 11: " + translator.translate(cohql11));
        // Expected: FT.SEARCH Inventory_index ((@location:NY (@stock:[-inf (50]|@restockDate:[-inf 2025-06-01]))|(@location:LA @stock:[(100 +inf]))

        String cohql12 = "SELECT * FROM Orders WHERE status != 'SHIPPED' AND orderDate BETWEEN '2025-05-01' AND '2025-05-31'";
        System.out.println("Test 12: " + translator.translate(cohql12));
        // Expected: FT.SEARCH Orders_index -@status:SHIPPED @orderDate:[2025-05-01 2025-05-31]

        String cohql13 = "SELECT * FROM Customers WHERE (age > 21 AND age < 65) OR (city = 'London' AND lastPurchase >= '2025-01-01') AND name LIKE 'J%'\n";
        System.out.println("Test 13: " + translator.translate(cohql13));
        // Expected: FT.SEARCH Customers_index (((@age:[(21 65])|(@city:London @lastPurchase:[2025-01-01 +inf])) @name:J*)

        String cohql14 = "SELECT * FROM Products WHERE productId IN (101, 102, 103) OR name IN ('WidgetA', 'WidgetB')";
        System.out.println("Test 14: " + translator.translate(cohql14));
        // Expected: FT.SEARCH Products_index (@productId:{101|102|103}|@name:{WidgetA|WidgetB})

        String cohql15 = "SELECT * FROM Users WHERE username NOT IN ('admin', 'root', 'superuser')";
        System.out.println("Test 15: " + translator.translate(cohql15));
        // Expected: FT.SEARCH Users_index -@username:{admin|root|superuser}

    }
}

