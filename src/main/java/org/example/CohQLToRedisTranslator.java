package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

import java.util.*;
import java.util.stream.Collectors;

public class CohQLToRedisTranslator {

    public String translate(String cohql) throws JSQLParserException {
        Expression expr = CCJSqlParserUtil.parseCondExpression(cohql);
        return processExpression(expr);
    }
    
    private String processExpression(Expression expr) {
        if (expr instanceof AndExpression andExpression) {
            return processAnd(andExpression);
        } else if (expr instanceof OrExpression orExpression) {
            return processOr(orExpression);
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

    private String processAnd(BinaryExpression expr) {
        String left = processExpression(expr.getLeftExpression());
        String right = processExpression(expr.getRightExpression());
        return left + " " + right;
    }

    private String processOr(BinaryExpression expr) {
        String leftQuery = processExpression(expr.getLeftExpression());
        String rightQuery = processExpression(expr.getRightExpression());

        return "(" + leftQuery + ") | (" + rightQuery + ")";
    }

    private String processComparison(Expression expr) {
        if (expr instanceof EqualsTo) {
            return processEquals((EqualsTo) expr);
        } else if (expr instanceof NotEqualsTo) {
            return processNotEquals((NotEqualsTo) expr);
        } else if (expr instanceof ComparisonOperator comparisonOperator) {
            if (!isNumericField(comparisonOperator.getRightExpression())) {
                throw new IllegalArgumentException("Comparison is supported only for numeric fields");
            }
            String fieldName = ((Column) comparisonOperator.getLeftExpression()).getColumnName();
            String value = comparisonOperator.getRightExpression().toString();
            return processRange(fieldName, comparisonOperator.getStringExpression(), value);
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
        String fieldName = expr.getLeftExpression().toString();
        String value = formatValue(expr.getRightExpression());

        if (isNumericField(expr.getRightExpression())) {
            // For numeric fields, use range syntax for exact match
            return "@" + fieldName + ":[" + value + " " + value + "]";
        } else if (isTagField(expr.getRightExpression())) {
            // For tag fields, use tag syntax
            return "@" + fieldName + ":{" + escapeValue(value) + "}";
        }

        throw new UnsupportedOperationException("Unknown field type for: " + fieldName);
    }

    private boolean isTagField(Expression expression) {
        return expression instanceof StringValue;
    }

    private boolean isNumericField(Expression expression) {
        if (expression instanceof SignedExpression) {
            expression = ((SignedExpression) expression).getExpression();
        }
        return expression instanceof DoubleValue || expression instanceof LongValue;
    }
    private String processNotEquals(NotEqualsTo expr) {
        String fieldName = expr.getLeftExpression().toString();
        boolean isTag = isTagField(expr.getRightExpression());
        String value = formatValue(expr.getRightExpression());
        if (isTag) {
            value = escapeValue(value);
        }
        return "-@" + fieldName + ":" + value;
    }

    private String processRange(String fieldName, String operator, String value) {
        return switch (operator) {
            case ">=" -> "@%s:[%s +inf]".formatted(fieldName, value);
            case ">" -> "@%s:[(%s +inf]".formatted(fieldName, value);
            case "<=" -> "@%s:[-inf %s]".formatted(fieldName, value);
            case "<" -> "@%s:[-inf (%s]".formatted(fieldName, value);
            default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
        };
    }

    private String processIn(InExpression inExpr) {
        String fieldName = inExpr.getLeftExpression().toString();
        ExpressionList expressionList = (ExpressionList) inExpr.getRightItemsList();

        if (expressionList.getExpressions().isEmpty()) {
            return "@" + fieldName + ":__NEVER_MATCH__";
        }
        
        Expression firstExpression = expressionList.getExpressions().get(0);
        String inClause;
        if (isNumericField(firstExpression)) {
            inClause = processNumericIn(fieldName, expressionList);
        } else if (isTagField(firstExpression)) {
            inClause = processTagIn(fieldName, expressionList);
        } else {
            throw new UnsupportedOperationException("Unknown field type for: " + fieldName);
        }
        String prefix = inExpr.isNot() ? "-" : "";
        return prefix + "(" + inClause + ")";
    }
    
    private String processNumericIn(String fieldName, ExpressionList rightList) {
        List<String> values = rightList.getExpressions().stream()
                .filter(this::isNumericField) // We ignore non-numeric fields
                .map(Object::toString)
                .toList();

        if (values.isEmpty()) {
            return "@" + fieldName + ":__NEVER_MATCH__";
        }
        
        return values.stream()
                .map(value -> "(@" + fieldName + ":[" + value + " " + value + "])")
                .collect(Collectors.joining(" | "));
    }
    
    private String processTagIn(String fieldName, ExpressionList rightList) {
        List<String> values = rightList.getExpressions().stream()
                .map(this::formatValue)
                .filter(value -> !value.equalsIgnoreCase("null"))
                .toList();

        if (values.isEmpty()) {
            // If only NULL values, return query that matches nothing
            return "@" + fieldName + ":__NEVER_MATCH__";
        }

        return values.stream()
                    .map(this::escapeValue)
                    .map(value -> "(@" + fieldName + ":{" + value + "})")
                    .collect(Collectors.joining(" | "));
    }

    private String processLike(LikeExpression expr) {
        String fieldName = expr.getLeftExpression().toString();
        String pattern = formatValue(expr.getRightExpression())
                .replace("%", "*")
                .replace("_", "?");
        return "@" + fieldName + ":{" + escapeValue(pattern) + "}";
    }

    private String formatField(Expression expr) {
        String field = expr.toString();
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
}
