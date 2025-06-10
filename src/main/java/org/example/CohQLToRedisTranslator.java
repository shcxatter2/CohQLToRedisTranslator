package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CohQLToRedisTranslator {

    private final Map<String, String> fieldTypes;

    public CohQLToRedisTranslator(Map<String, String> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public String translate(String cohql) throws JSQLParserException {
        try {
            // Attempt to parse as full SQL statement first
            Statement statement = CCJSqlParserUtil.parse(cohql);
            if (statement instanceof Select) {
                Select select = (Select) statement;
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                String tableName = ((Table) plainSelect.getFromItem()).getName();
                Expression whereExpr = plainSelect.getWhere();

                String queryString = whereExpr != null ?
                        processExpression(whereExpr) : "*";

                return "FT.SEARCH " + tableName + "_index " + queryString;
            }
        } catch (JSQLParserException e) {
            // Fallback to condition parsing if full statement parse fails
            Expression expr = CCJSqlParserUtil.parseCondExpression(cohql);
            return processExpression(expr);
        }

        throw new UnsupportedOperationException("Unsupported query type");
    }


    private String removeRedundantParentheses(String query) {
        if (query == null || query.isEmpty()) return query;
        String current = query;
        String previous;
        do {
            previous = current;
            if (current.startsWith("(") && current.endsWith(")") && hasBalancedParentheses(current)) {
                String stripped = current.substring(1, current.length() - 1);
                if (hasBalancedParentheses(stripped)) {
                    current = stripped;
                }
            }
        } while (!current.equals(previous));
        return current;
    }

    private boolean hasBalancedParentheses(String s) {
        int balance = 0;
        for (char c : s.toCharArray()) {
            if (c == '(') balance++;
            else if (c == ')') balance--;
            if (balance < 0) return false;
        }
        return balance == 0;
    }

    private String processExpression(Expression expr) {
        if (expr instanceof AndExpression andExpression) {
            return handleLogical(andExpression, " ");
        } else if (expr instanceof OrExpression orExpression) {
            return processOr(orExpression.getLeftExpression(), orExpression.getRightExpression());
        } else if (expr instanceof Parenthesis parenthesis) {
            return "(" + processExpression(parenthesis.getExpression()) + ")";
        } else if (expr instanceof NotExpression notExpression) {
            String inner = processExpression(notExpression.getExpression());
            if (inner.contains(" ") || inner.contains("|")) {
                return "-(" + inner + ")";
            }
            return "-" + inner;
        }
        return processComparison(expr);
    }

    private String handleLogical(BinaryExpression expr, String operator) {
        String left = processExpression(expr.getLeftExpression());
        String right = processExpression(expr.getRightExpression());
        return "|".equals(operator) ? left + " | " + right : left + " " + right;
    }

    private String optimizeSingleFieldTagOrs(String query) {
        // Pattern to match multiple tag queries for the same field
        Pattern pattern = Pattern.compile("@(\\w+):\\{([^}]+)\\}(\\s*\\|\\s*@\\1:\\{([^}]+)\\})+");
        Matcher matcher = pattern.matcher(query);

        while (matcher.find()) {
            String field = matcher.group(1);
            String fullMatch = matcher.group(0);

            // Extract all values for this field
            Pattern valuePattern = Pattern.compile("@" + field + ":\\{([^}]+)\\}");
            Matcher valueMatcher = valuePattern.matcher(fullMatch);
            List<String> values = new ArrayList<>();

            while (valueMatcher.find()) {
                values.add(valueMatcher.group(1));
            }

            // Replace with combined tag query
            String combined = "@" + field + ":{" + String.join(",", values) + "}";
            query = query.replace(fullMatch, combined);
        }

        return query;
    }

    private String processOr(Expression left, Expression right) {
        String leftQuery = processExpression(left);
        String rightQuery = processExpression(right);

        // Always wrap each condition in parentheses for RediSearch
        return "(" + leftQuery + ") | (" + rightQuery + ")";
    }

    private String processComparison(Expression expr) {
        if (expr instanceof EqualsTo) {
            return processEquals((EqualsTo) expr);
        } else if (expr instanceof NotEqualsTo) {
            return processNotEquals((NotEqualsTo) expr);
        } else if (expr instanceof GreaterThan gt) {
            String fieldName = ((Column) gt.getLeftExpression()).getColumnName();
            String value = gt.getRightExpression().toString().replaceAll("'", "");
            return processRange(fieldName, ">", value);  // Added return
        } else if (expr instanceof GreaterThanEquals gte) {
            String fieldName = ((Column) gte.getLeftExpression()).getColumnName();
            String value = gte.getRightExpression().toString().replaceAll("'", "");
            return processRange(fieldName, ">=", value); // Added return
        } else if (expr instanceof MinorThan mt) {
            String fieldName = ((Column) mt.getLeftExpression()).getColumnName();
            String value = mt.getRightExpression().toString().replaceAll("'", "");
            return processRange(fieldName, "<", value);  // Added return
        } else if (expr instanceof MinorThanEquals mte) {
            String fieldName = ((Column) mte.getLeftExpression()).getColumnName();
            String value = mte.getRightExpression().toString().replaceAll("'", "");
            return processRange(fieldName, "<=", value); // Added return
        } else if (expr instanceof InExpression) {
            return processIn((InExpression) expr);
        } else if (expr instanceof LikeExpression) {
            return processLike((LikeExpression) expr);
        } else if (expr instanceof IsNullExpression) {
            return processIsNull((IsNullExpression) expr);
        } else if (expr instanceof Between) {
            return processBetween((Between) expr);
        }
        throw new UnsupportedOperationException("Unsupported expression: " + expr.getClass());
    }

    private String processIsNull(IsNullExpression expr) {
        String fieldName = expr.getLeftExpression().toString();
        boolean isNot = expr.isNot(); // Handle IS NOT NULL

        // Redis doesn't index null values, so:
        // IS NULL should match nothing (since nulls aren't indexed)
        // IS NOT NULL should match everything (since only non-nulls are indexed)

        if (isNot) {
            // IS NOT NULL - match all documents (since Redis only indexes non-null values)
            return "*";
        } else {
            // IS NULL - match nothing (since Redis doesn't index null values)
            return "@" + fieldName + ":__NEVER_MATCH_NULL__";
        }
    }

    private String processBetween(Between between) {
        String field = formatField(between.getLeftExpression());
        String lower = formatValue(between.getBetweenExpressionStart());
        String upper = formatValue(between.getBetweenExpressionEnd());
        return String.format("%s:[%s %s]", field, lower, upper);
    }

    private String processEquals(EqualsTo expr) {
        String fieldName = expr.getLeftExpression().toString().replaceAll("^@+", "");
        String value = formatValue(expr.getRightExpression());

        if (isNumericField(fieldName)) {
            // For numeric fields, use range syntax for exact match
            return "@" + fieldName + ":[" + value + " " + value + "]";
        } else if (isTextField(fieldName)) {
            // For text fields, use quoted exact match
            return "@" + fieldName + ":\"" + escapeValue(value) + "\"";
        } else if (isTagField(fieldName)) {
            // For tag fields, use tag syntax
            return "@" + fieldName + ":{" + escapeValue(value) + "}";
        }

        throw new UnsupportedOperationException("Unknown field type for: " + fieldName);
    }

    private boolean isTextField(String field) {
        return FieldType.TEXT.name().equals(fieldTypes.get(field));
    }

    private boolean isTagField(String field) {
        return FieldType.TAG.name().equals(fieldTypes.get(field));
    }

    private boolean isNumericField(String field) {
        return FieldType.NUMERIC.name().equals(fieldTypes.get(field));
    }
    private String processNotEquals(NotEqualsTo expr) {
        String fieldName = expr.getLeftExpression().toString().replaceAll("^@+", "");
        boolean isTag = isTagField(fieldName);
        String value = formatValue(expr.getRightExpression());
        if (isTag) {
            value = escapeValue(value);
        }
        return "-@" + fieldName + ":" + value;
    }

    private String processRange(String fieldName, String operator, String value) {
        String fieldType = fieldTypes.getOrDefault(fieldName, "TEXT");

        // Handle date and text ranges
        if (fieldType.equals("TEXT") || value.contains("-")) {
            return switch (operator) {
                case ">=" -> "@%s:[%s +inf]".formatted(fieldName, value);
                case ">" -> "@%s:[(%s +inf]".formatted(fieldName, value);
                case "<=" -> "@%s:[-inf %s]".formatted(fieldName, value);
                case "<" -> "@%s:[-inf (%s]".formatted(fieldName, value);
                default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
            };
        }

        // Handle numeric ranges
        try {
            Double.parseDouble(value);
            return switch (operator) {
                case ">=" -> "@%s:[%s +inf]".formatted(fieldName, value);
                case ">" -> "@%s:[(%s +inf]".formatted(fieldName, value);
                case "<=" -> "@%s:[-inf %s]".formatted(fieldName, value);
                case "<" -> "@%s:[-inf (%s]".formatted(fieldName, value);
                default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric range value: " + value);
        }
    }


    private String processIn(InExpression inExpr) {
        String fieldName = inExpr.getLeftExpression().toString();
        ExpressionList rightList = (ExpressionList) inExpr.getRightItemsList();

        List<String> values = rightList.getExpressions().stream()
                .map(expr -> expr.toString().replaceAll("'", ""))
                .filter(value -> !value.equalsIgnoreCase("null")) // Filter out NULL values
                .collect(Collectors.toList());

        if (values.isEmpty()) {
            // If only NULL values, return query that matches nothing
            return "@" + fieldName + ":__NEVER_MATCH__";
        }

        if (isTagField(fieldName)) {
            // Use parenthesized OR syntax instead of comma-separated
            String orClauses = values.stream()
                    .map(this::escapeValue)
                    .map(value -> "(@" + fieldName + ":{" + escapeValue(value) + "})")
                    .collect(Collectors.joining(" | "));
            String prefix = inExpr.isNot() ? "-" : "";
            return prefix + "(" + orClauses + ")";
        } else if (isTextField(fieldName)) {
            String orClauses = values.stream()
                    .map(this::escapeValue)
                    .map(value -> "(@" + fieldName + ":\"" + value + "\")")
                    .collect(Collectors.joining(" | "));
            String prefix = inExpr.isNot() ? "-" : "";
            return prefix + "(" + orClauses + ")";
        } else if (isNumericField(fieldName)) {
            String orClauses = values.stream()
                    .map(value -> "(@" + fieldName + ":[" + value + " " + value + "])")
                    .collect(Collectors.joining(" | "));
            String prefix = inExpr.isNot() ? "-" : "";
            return prefix + "(" + orClauses + ")";
        }

        throw new UnsupportedOperationException("Unknown field type for: " + fieldName);
    }

    private String processLike(LikeExpression expr) {
        String fieldName = expr.getLeftExpression().toString().replaceAll("^@+", "");
        String pattern = formatValue(expr.getRightExpression())
                .replace("%", "*")
                .replace("_", "?");
        return "@" + fieldName + ":" + pattern; // Add quotes for exact prefix match
    }

    private List<String> extractValues(ItemsList items) {
        List<String> values = new ArrayList<>();
        if (items instanceof ExpressionList) {
            for (Expression expr : ((ExpressionList) items).getExpressions()) {
                String val = formatValue(expr);
                values.add(val);
            }
        }
        System.out.println("extractValues: " + values);
        return values;
    }

    private String formatField(Expression expr) {
        String field = expr.toString().replaceAll("^@+", "");
        return "@" + field;
    }

    private String formatValue(Expression expr) {
        if (expr instanceof StringValue) {
            return ((StringValue) expr).getValue();
        } else if (expr instanceof LongValue) {
            return String.valueOf(((LongValue) expr).getValue());
        } else if (expr instanceof DoubleValue) {
            return String.valueOf(((DoubleValue) expr).getValue());
        }
        return expr.toString();
    }

    private String escapeValue(String value) {
        return value.replace("\\", "\\\\")
                .replace("@", "\\@")
                .replace(".", "\\.")
                .replace(",", "\\,")
                .replace(" ", "\\ ")
                .replace("_", "\\_")
                .replace("+", "\\+");
    }

    static enum FieldType {
        TEXT,
        TAG,
        NUMERIC
    }
}
