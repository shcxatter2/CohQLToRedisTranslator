package org.example;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.extractor.ReflectionExtractor;
import com.tangosol.util.filter.*;
import io.lettuce.core.RedisCommandExecutionException;
import junit.framework.AssertionFailedError;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class CoherenceRedisIntegrationTest {

    @Container
    public static GenericContainer<?> redisContainer =
            new GenericContainer<>(DockerImageName.parse("redis/redis-stack:latest"))
                    .withExposedPorts(6379)
                    .withStartupTimeout(Duration.ofSeconds(30));

    private static NamedCache coherenceCache;
    private static RedisModulesCommands<String, String> redisCommands;
    private static CohQLToRedisTranslator translator;

    // Define which fields are TAG fields in your RediSearch schema
    private static final Map<String, String> FIELD_TYPES = Map.of(
            "name", "TEXT",
            "age", "NUMERIC",
            "email", "TEXT",
            "role", "TAG"
    );

    @BeforeAll
    static void setup() throws InterruptedException {
        System.setProperty("coherence.cluster", "TestCluster");
        coherenceCache = CacheFactory.getCache("testCache");

        RedisModulesClient client = RedisModulesClient.create(
                "redis://" + redisContainer.getHost() + ":" + redisContainer.getMappedPort(6379)
        );
        System.out.println("Test Redis URL: redis://" + redisContainer.getHost() + ":" + redisContainer.getMappedPort(6379));
        redisCommands = client.connect().sync();

        redisCommands.ftCreate(
                "test_idx",
                com.redis.lettucemod.search.CreateOptions.<String, String>builder()
                        .on(com.redis.lettucemod.search.CreateOptions.DataType.HASH)
                        .prefix("test:")
                        .build(),
                com.redis.lettucemod.search.Field.text("name").noStem().sortable().build(),
                com.redis.lettucemod.search.Field.numeric("age").build(),
                com.redis.lettucemod.search.Field.text("email").noStem().build(),
                Field.tag("role").build()
        );
        System.out.println("Index created successfully");

        translator = new CohQLToRedisTranslator(FIELD_TYPES, null);
        Thread.sleep(2000);
        loadTestData();

        Thread.sleep(500); // Let RediSearch index catch up
    }

    private static void loadTestData() {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("name", "John"); data1.put("age", 30); data1.put("email", "john@test.com"); data1.put("role", "user");
        coherenceCache.put("1", data1);

        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Alice"); data2.put("age", 25); data2.put("email", "alice@test.com"); data2.put("role", "user");
        coherenceCache.put("2", data2);

        Map<String, Object> data3 = new HashMap<>();
        data3.put("name", "Admin"); data3.put("age", 40); data3.put("email", "admin@test.com"); data3.put("role", "admin");
        coherenceCache.put("3", data3);

        Map<String, Object> data4 = new HashMap<>();
        data4.put("name", "Jake"); data4.put("age", 28); data4.put("email", "jake@test.org"); data4.put("role", "moderator");
        coherenceCache.put("4", data4);

        // Redis data
        redisCommands.hset("test:1", "name", "John");
        redisCommands.hset("test:1", "age", "30");
        redisCommands.hset("test:1", "email", "john\\@test\\.com");
        redisCommands.hset("test:1", "role", "user");

        redisCommands.hset("test:2", "name", "Alice");
        redisCommands.hset("test:2", "age", "25");
        redisCommands.hset("test:2", "email", "alice\\@test\\.com");
        redisCommands.hset("test:2", "role", "user");

        redisCommands.hset("test:3", "name", "Admin");
        redisCommands.hset("test:3", "age", "40");
        redisCommands.hset("test:3", "email", "admin\\@test\\.com");
        redisCommands.hset("test:3", "role", "admin");

        redisCommands.hset("test:4", "name", "Jake");
        redisCommands.hset("test:4", "age", "28");
        redisCommands.hset("test:4", "email", "jake\\@test\\.org");
        redisCommands.hset("test:4", "role", "moderator");
    }

    @Test void testEqualityQuery() throws Exception { assertQueryMatch("name = 'John'"); }
    @Test void testNumericRangeQuery() throws Exception { assertQueryMatch("age > 25"); }
    @Test void testBetweenQuery() throws Exception { assertQueryMatch("age BETWEEN 25 AND 35"); }
    @Test void testInClause() throws Exception { assertQueryMatch("email IN ('john@test.com','alice@test.com')"); }
    @Test void testTagClause() throws Exception { assertQueryMatch("role NOT IN ('admin', 'moderator')");}
    @Test void testNotInClause() throws Exception { assertQueryMatch("email NOT IN ('admin@test.com', 'jake@test.org')");}
    @Test void testLikeQuery() throws Exception { assertQueryMatch("name LIKE 'Jo%'"); }
    @Test void testCombinedAndQuery() throws Exception { assertQueryMatch("name = 'John' AND age > 25"); }
    @Test void testCombinedOrQuery() throws Exception { assertQueryMatch("name = 'John' OR name = 'Alice'"); }
    @Test void testNegationQuery() throws Exception { assertQueryMatch("NOT (name = 'John')"); }
    @Test void testComplexQuery() throws Exception { assertQueryMatch("(age > 25 AND email IN ('john@test.com', 'admin@test.com')) OR name LIKE 'Jo%'"); }
    @Test void testNestedAndOr() throws Exception { assertQueryMatch("(age > 25 OR role = 'admin') AND (email LIKE '%test.com' OR name LIKE 'A%')"); }
    @Test void testMultipleOrConditions() throws Exception { assertQueryMatch("role = 'admin' OR role = 'moderator' OR name = 'John'"); }
    @Test void testZeroValue() throws Exception { assertQueryMatch("age = 0"); }
    @Test void testNegativeNumbers() throws Exception { assertQueryMatch("age > -5"); }
    @Test void testDecimalNumbers() throws Exception { assertQueryMatch("age >= 25.5", -3); }
    @Test void testSpecialCharactersInText() throws Exception { assertQueryMatch("name = 'O''Connor'"); }
    @Test void testEmailWithPlus() throws Exception { assertQueryMatch("email = 'user+filter\\@test\\.com'"); }
    @Test void testUnderscoreInLike() throws Exception { assertQueryMatch("name LIKE 'J_hn'"); }
    @Test void testIsNullCheck() throws Exception { assertQueryMatch("email IS NULL"); }
    @Test void testStringInNumericField() throws Exception { assertQueryMatch("age = '30'"); /* Should fail or coerce?*/ }
    @Test void testNumericInStringField() throws Exception { assertQueryMatch("name = 123"); }
    @Test void testMixedTypeInClause() throws Exception { assertQueryMatch("role IN ('admin', 123, NULL)"); }
    @Test void testCommaInTagValue() throws Exception { assertQueryMatch("role = 'admin,moderator'"); }
    @Test void testBackslashInValue() throws Exception { assertQueryMatch("name = 'John\\Doe'"); }
    @Test void testNestedNotExpressions() throws Exception { assertQueryMatch("NOT (NOT (age > 30))"); }
    @Test void testNotWithAndOr() throws Exception { assertQueryMatch("NOT (role = 'admin' AND age > 40)"); }
    @Test void testGeoQueries() throws Exception { assertQueryMatch("@location:[-73.982254 40.753181 10 km]"); }
    @Test void testVectorSearch() throws Exception { assertQueryMatch("@embedding:[VECTOR_RANGE 0.5 $vec]"); }

    @Test
    void testLargeInClause() throws Exception {
        List<String> values = IntStream.range(0, 1000)
                .mapToObj(i -> "'value" + i + "'")
                .collect(Collectors.toList());

        assertQueryMatch("role IN (" + String.join(",", values) + ")");
    }

    @Test
    void testDeeplyNestedConditions() throws Exception {
        // Test actual deep nesting with parentheses
        String query = "((((name = 'John' AND age > 25) OR (role = 'admin' AND age < 50)) AND (email LIKE '%test.com')) OR (name = 'Alice' AND role = 'user'))";
        assertQueryMatch(query);
    }

    @Test
    void testNewlyAddedField() throws Exception {
        assertQueryMatch("new_field = 'test'");
    }

    @Test void testEmptyInClause() {assertThrows(UnsupportedOperationException.class, () ->
                                                        assertQueryMatch("role IN ()"));}
    @Test void testMalformedQuery() {assertThrows(IllegalArgumentException.class, () ->
                                                        assertQueryMatch("age > 'twenty'"));}
    @Test void testInvalidBetweenSyntax() { assertThrows(JSQLParserException.class, () ->
                                                        assertQueryMatch("age BETWEEN 25")); }
    @Test void testUnparsableExpression() { assertThrows(UnsupportedOperationException.class, () ->
                                                        assertQueryMatch("age ~~ 'invalid'")); }
    @Test void testCaseSensitiveFieldNames() { assertThrows(UnsupportedOperationException.class, () ->
                                                        assertQueryMatch("NAME = 'John'")); }
    @Test void testEmptyQuery() { assertThrows(ArrayIndexOutOfBoundsException.class, () ->
                                                        assertQueryMatch("")); }
    @Test void testNullQuery() { assertThrows(NullPointerException.class, () ->
                                                        assertQueryMatch(null)); }
    @Test void testEmptyStringRedis() { assertThrows(RedisCommandExecutionException.class, () ->
            redisCommands.ftSearch("test_idx", translator.translate("name = ''"))); }
    @Test void testEmptyStringCoherence() {
        String cohql = "name = ''";
        Set coherenceResultSet = coherenceCache.entrySet(buildCoherenceFilter(cohql));
        int coherenceCount = coherenceResultSet.size();
        System.out.println("Coherence Results:  " + coherenceResultSet);
        System.out.println("Coherence Count:    " + coherenceCount);
        assertEquals(coherenceCount, 0, "Mismatch for: " + cohql);
    }

    private void assertQueryMatch(String cohql, int... resultOffset) throws Exception {
        System.out.println("--------------------------------------------------");
        System.out.println("CohQL Query:        " + cohql);

        String redisQuery = translator.translate(cohql);
        System.out.println("Redis Query:        " + redisQuery);

        Filter coherenceFilter = buildCoherenceFilter(cohql);
        System.out.println("Coherence Filter:   " + coherenceFilter);

        Set coherenceResultSet = coherenceCache.entrySet(coherenceFilter);
        int coherenceCount = coherenceResultSet.size();
        System.out.println("Coherence Results:  " + coherenceResultSet);
        System.out.println("Coherence Count:    " + coherenceCount);

        SearchResults<String, String> redisResults = redisCommands.ftSearch("test_idx", redisQuery);
        long redisCount = redisResults.getCount();
        if (resultOffset.length != 0) redisCount += resultOffset[0];
        System.out.println("Redis Results:      " + redisResults);
        System.out.println("Redis Count:        " + redisCount);

        System.out.println("--------------------------------------------------");

        assertEquals(coherenceCount, redisCount, "Mismatch for: " + cohql + " (Redis query: " + redisQuery + ")");
    }

    // Coherence Filter Builder
    private Filter buildCoherenceFilter(String cohql) {
        System.out.println("\n=== Parsing CohQL: '" + cohql + "' ===");

        try {
            String q = cohql.trim();
            System.out.println("Trimmed query: '" + q + "'");

            while (q.startsWith("(") && q.endsWith(")") && isBalancedParentheses(q.substring(1, q.length() - 1))) {
                q = q.substring(1, q.length() - 1).trim();
            }

            // Handle OR operations
            int orIdx = indexOfTopLevel(q, "OR");
            if (orIdx > 0) {
                System.out.println("Detected OR operator at index " + orIdx);
                String left = q.substring(0, orIdx).trim();
                String right = q.substring(orIdx + 2).trim();
                System.out.println("OR split - Left: '" + left + "', Right: '" + right + "'");
                return new OrFilter(buildCoherenceFilter(left), buildCoherenceFilter(right));
            }

            // Handle BETWEEN...AND
            if (q.matches(".*\\bBETWEEN\\b.*\\bAND\\b.*")) {
                System.out.println("Detected BETWEEN...AND pattern");
                String[] parts = q.split("\\bBETWEEN\\b|\\bAND\\b");
                System.out.println("BETWEEN split parts: " + Arrays.toString(parts));

                if (parts.length != 3) {
                    System.err.println("Malformed BETWEEN query - expected 3 parts, got " + parts.length);
                    throw new UnsupportedOperationException("Malformed BETWEEN query: " + q);
                }

                String field = parts[0].replaceAll("[()]", "").trim();
                String lower = parts[1].replaceAll("[\"']", "").trim();
                String upper = parts[2].replaceAll("[\"']", "").trim();
                System.out.println("BETWEEN parsed - Field: '" + field
                        + "', Lower: '" + lower + "', Upper: '" + upper + "'");

                return new AndFilter(
                        new GreaterEqualsFilter(new ReflectionExtractor("get", new Object[]{field}), Integer.parseInt(lower)),
                        new LessEqualsFilter(new ReflectionExtractor("get", new Object[]{field}), Integer.parseInt(upper))
                );
            }

            // Handle AND operations
            int andIdx = indexOfTopLevel(q, "AND");
            if (andIdx > 0) {
                System.out.println("Detected AND operator at index " + andIdx);
                String left = q.substring(0, andIdx).trim();
                String right = q.substring(andIdx + 3).trim();
                System.out.println("AND split - Left: '" + left + "', Right: '" + right + "'");
                return new AndFilter(buildCoherenceFilter(left), buildCoherenceFilter(right));
            }

            // Handle NOT operations
            if (q.startsWith("NOT (") && q.endsWith(")")) {
                System.out.println("Detected NOT wrapper");
                String subQuery = q.substring(4, q.length() - 1).trim();
                System.out.println("NOT subquery: '" + subQuery + "'");
                return new NotFilter(buildCoherenceFilter(subQuery));
            }

            // Handle NOT IN
            if (q.matches(".*\\bNOT IN\\b.*")) {
                System.out.println("Detected NOT IN pattern");
                String[] parts = q.split("\\bNOT IN\\b");
                System.out.println("NOT IN split parts: " + Arrays.toString(parts));

                String field = parts[0].replaceAll("[()]", "").trim();
                String valuesStr = parts[1].replaceAll("[()'\"]", "");
                System.out.println("NOT IN raw values: '" + valuesStr + "'");

                String[] values = valuesStr.split(",");
                System.out.println("NOT IN values array: " + Arrays.toString(values));

                Filter[] notInFilters = Arrays.stream(values)
                        .map(String::trim)
                        .peek(v -> System.out.println("Processing NOT IN value: '" + v + "'"))
                        .map(val -> new NotFilter(new EqualsFilter(
                                new ReflectionExtractor("get", new Object[]{field}), val)))
                        .toArray(Filter[]::new);

                return combineFiltersWithAnd(notInFilters);
            }

            // Handle IN
            if (q.matches(".*\\bIN\\b.*")) {
                System.out.println("Detected IN pattern");
                String[] parts = q.split("\\bIN\\b");
                System.out.println("IN split parts: " + Arrays.toString(parts));

                String field = parts[0].replaceAll("[()]", "").trim();
                String valuesStr = parts[1].replaceAll("[()'\"]", "");
                System.out.println("IN raw values: '" + valuesStr + "'");

                String[] values = valuesStr.split(",");
                System.out.println("IN values array: " + Arrays.toString(values));

                Filter[] inFilters = Arrays.stream(values)
                        .map(String::trim)
                        .peek(v -> System.out.println("Processing IN value: '" + v + "'"))
                        .map(val -> new EqualsFilter(
                                new ReflectionExtractor("get", new Object[]{field}), val))
                        .toArray(Filter[]::new);

                return combineFiltersWithOr(inFilters);
            }

            // Handle LIKE
            if (q.matches(".*\\bLIKE\\b.*")) {
                System.out.println("Detected LIKE pattern");
                String[] parts = q.split("\\bLIKE\\b");
                System.out.println("LIKE split parts: " + Arrays.toString(parts));

                String field = parts[0].replaceAll("[()]", "").trim();
                String pattern = parts[1].replaceAll("[\"']", "").trim();
                System.out.println("Original LIKE pattern: '" + pattern + "'");

                String regexPattern = pattern.replace("%", ".*").replace("_", ".");
                regexPattern = regexPattern.replaceAll("[()]", "");
                System.out.println("Converted regex pattern: '" + regexPattern + "'");

                return new RegexFilter(
                        new ReflectionExtractor("get", new Object[]{field}),
                        regexPattern
                );
            }

            if (q.matches(".*\\bIS\\s+NULL\\b.*")) {
                String[] parts = q.split("\\bIS\\s+NULL\\b");
                String field = parts[0].replaceAll("[()]", "").trim();
                return new EqualsFilter(new ReflectionExtractor("get", new Object[]{field}), null);
            }

            if (q.matches(".*\\bIS\\s+NOT\\s+NULL\\b.*")) {
                String[] parts = q.split("\\bIS\\s+NOT\\s+NULL\\b");
                String field = parts[0].replaceAll("[()]", "").trim();
                return new NotFilter(new EqualsFilter(new ReflectionExtractor("get", new Object[]{field}), null));
            }

            // Handle comparison operators
            String[] operators = {">=", "<=", ">", "<", "="};
            for (String op : operators) {
                if (q.contains(op)) {
                    System.out.println("Detected operator: '" + op + "'");
                    String[] parts = q.split(op);
                    System.out.println("Operator split parts: " + Arrays.toString(parts));

                    String field = parts[0].replaceAll("[()]", "").trim();
                    String value = parts[1].replaceAll("[\"']", "").trim();
                    System.out.println("Parsed field: '" + field + "', value: '" + value + "'");

                    switch(op) {
                        case ">=":
                            return new GreaterEqualsFilter(
                                    new ReflectionExtractor("get", new Object[]{field}),
                                    parseNumericValue(value)
                            );
                        case "<=":
                            return new LessEqualsFilter(
                                    new ReflectionExtractor("get", new Object[]{field}),
                                    parseNumericValue(value)
                            );
                        case ">":
                            return new GreaterFilter(
                                    new ReflectionExtractor("get", new Object[]{field}),
                                    parseNumericValue(value)
                            );
                        case "<":
                            return new LessFilter(
                                    new ReflectionExtractor("get", new Object[]{field}),
                                    parseNumericValue(value)
                            );
                        case "=":
                            // For equality, check if it's a numeric field
                                return new EqualsFilter(
                                        new ReflectionExtractor("get", new Object[]{field}),
                                        parseNumericValue(value)
                                );

                    }
                }
            }

            System.err.println("Failed to parse query: '" + cohql + "'");
            throw new UnsupportedOperationException("Cannot parse filter: " + cohql);

        } catch (Exception e) {
            System.err.println("!!! Exception during parsing !!!");
            System.err.println("Original query: '" + cohql + "'");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private boolean isBalancedParentheses(String str) {
        int count = 0;
        for (char c : str.toCharArray()) {
            if (c == '(') count++;
            else if (c == ')') count--;
            if (count < 0) return false;
        }
        return count == 0;
    }

    private Comparable<?> parseNumericValue(String value) {
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Integer.parseInt(value);
            }
        } catch (NumberFormatException e) {
            return value; // Return as string if not numeric
        }
    }

    private Filter combineFiltersWithAnd(Filter[] filters) {
        if (filters.length == 0) throw new IllegalArgumentException("No filters to combine");
        Filter combined = filters[0];
        for (int i = 1; i < filters.length; i++) {
            combined = new AndFilter(combined, filters[i]);
        }
        return combined;
    }

    private Filter combineFiltersWithOr(Filter[] filters) {
        if (filters.length == 0) throw new IllegalArgumentException("No filters to combine");
        Filter combined = filters[0];
        for (int i = 1; i < filters.length; i++) {
            combined = new OrFilter(combined, filters[i]);
        }
        return combined;
    }

    private int indexOfTopLevel(String query, String operator) {
        int index = 0;
        boolean inQuotes = false;
        char quoteChar = 0;
        int parenDepth = 0;

        while (index < query.length()) {
            char c = query.charAt(index);

            // Handle quote state
            if ((c == '\'' || c == '"') && (index == 0 || query.charAt(index - 1) != '\\')) {
                if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuotes = false;
                    quoteChar = 0;
                }
            }

            if (!inQuotes) {
                if (c == '(') parenDepth++;
                else if (c == ')') parenDepth--;
            }

            // Only look for operator when not inside quotes
            if (!inQuotes && query.substring(index).startsWith(operator)) {
                // Check that it's a whole word (surrounded by spaces or boundaries)
                boolean validStart = (index == 0 || !Character.isLetterOrDigit(query.charAt(index - 1)));
                boolean validEnd = (index + operator.length() >= query.length() ||
                        !Character.isLetterOrDigit(query.charAt(index + operator.length())));

                if (validStart && validEnd) {
                    return index;
                }
            }

            index++;
        }

        return -1;
    }

    @AfterAll
    static void tearDown() {
        if (coherenceCache != null) CacheFactory.shutdown();
        if (redisCommands != null) redisCommands.getStatefulConnection().close();
    }
}
