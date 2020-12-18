package com.github.util.sql;

import com.google.common.collect.Lists;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * https://github.com/hibernate/hibernate-orm/blob/2964ecbf1b74003fcc337b0574487c724638fe94/hibernate-core/src/main/java/org/hibernate/engine/jdbc/internal/BasicFormatterImpl.java
 */
class SqlFormat {

    private static final Set<String> BEGIN_CLAUSES = new HashSet<>();
    private static final Set<String> END_CLAUSES = new HashSet<>();
    private static final Set<String> LOGICAL = new HashSet<>();
    private static final Set<String> QUANTIFIERS = new HashSet<>();
    private static final Set<String> DML = new HashSet<>();
    private static final Set<String> MISC = new HashSet<>();

    static {
        BEGIN_CLAUSES.add("left");
        BEGIN_CLAUSES.add("right");
        BEGIN_CLAUSES.add("inner");
        BEGIN_CLAUSES.add("outer");
        BEGIN_CLAUSES.add("group");
        BEGIN_CLAUSES.add("order");
        BEGIN_CLAUSES.add("limit");

        END_CLAUSES.add("where");
        END_CLAUSES.add("set");
        END_CLAUSES.add("having");
        END_CLAUSES.add("from");
        END_CLAUSES.add("by");
        END_CLAUSES.add("join");
        END_CLAUSES.add("into");
        END_CLAUSES.add("union");

        LOGICAL.add("and");
        LOGICAL.add("or");
        LOGICAL.add("when");
        LOGICAL.add("else");
        LOGICAL.add("end");

        QUANTIFIERS.add("in");
        QUANTIFIERS.add("all");
        QUANTIFIERS.add("exists");
        QUANTIFIERS.add("some");
        QUANTIFIERS.add("any");

        DML.add("insert");
        DML.add("update");
        DML.add("delete");

        MISC.add("select");
        MISC.add("on");
    }

    private static final String INDENT_STRING = "  ";
    private static final String INITIAL = "  ";

    private static final Pattern PARAM_REGEX = Pattern.compile("(?s)('[^']*?\\s{2,}?.*?')");
    private static final Pattern BLANK_REGEX = Pattern.compile("\\s{2,}");
    private static final String SCRIPT_PLACE = String.format("___%s-%s===", SqlFormat.class.getName(), "PARam");
    private static final Pattern SCRIPT_PLACE_REGEX = Pattern.compile(SCRIPT_PLACE);

    static String format(String source) {
        if (source == null || "".equals(source.trim())) {
            return "";
        }
        return new FormatProcess(handlerBlank(source)).perform();
    }

    private static String handlerBlank(String sql) {
        Matcher match = PARAM_REGEX.matcher(sql);
        List<String> list = Lists.newArrayList();
        while (match.find()) {
            list.add(match.group());
        }
        if (list.size() == 0) {
            return sql;
        }

        sql = match.replaceAll(Matcher.quoteReplacement(SCRIPT_PLACE));
        sql = BLANK_REGEX.matcher(sql).replaceAll(" ");
        while (sql.contains(SCRIPT_PLACE)) {
            String place = list.remove(0);
            Matcher m = SCRIPT_PLACE_REGEX.matcher(sql);
            if (m.find()) {
                sql = m.replaceFirst(Matcher.quoteReplacement(place));
            }
        }
        return sql;
    }

    private static class FormatProcess {
        boolean beginLine = true;
        boolean afterBeginBeforeEnd;
        boolean afterByOrSetOrFromOrSelect;
        boolean afterValues;
        boolean afterOn;
        boolean afterBetween;
        boolean afterInsert;
        int inFunction;
        int parensSinceSelect;
        private final LinkedList<Integer> parenCounts = new LinkedList<>();
        private final LinkedList<Boolean> afterByOrFromOrSelects = new LinkedList<>();

        int indent = 1;

        StringBuilder result = new StringBuilder();
        StringTokenizer tokens;
        String lastToken;
        String token;
        String lcToken;

        private FormatProcess(String sql) {
            tokens = new StringTokenizer(
                    sql,
                    "()+*/-=<>'`\"[]," + " ",
                    true
            );
        }

        private String perform() {
            result.append(INITIAL);

            while (tokens.hasMoreTokens()) {
                token = tokens.nextToken();
                lcToken = token.toLowerCase(Locale.ROOT);

                if ("'".equals(token)) {
                    String t;
                    do {
                        t = tokens.nextToken();
                        token += t;
                    }
                    // cannot handle single quotes
                    while (!"'".equals(t) && tokens.hasMoreTokens());
                } else if ("\"".equals(token)) {
                    String t;
                    do {
                        t = tokens.nextToken();
                        token += t;
                    }
                    while (!"\"".equals(t));
                }

                if (afterByOrSetOrFromOrSelect && ",".equals(token)) {
                    commaAfterByOrFromOrSelect();
                } else if (afterOn && ",".equals(token)) {
                    commaAfterOn();
                } else if ("(".equals(token)) {
                    openParen();
                } else if (")".equals(token)) {
                    closeParen();
                } else if (BEGIN_CLAUSES.contains(lcToken)) {
                    beginNewClause();
                } else if (END_CLAUSES.contains(lcToken)) {
                    endNewClause();
                } else if ("select".equals(lcToken)) {
                    select();
                } else if (DML.contains(lcToken)) {
                    updateOrInsertOrDelete();
                } else if ("values".equals(lcToken)) {
                    values();
                } else if ("on".equals(lcToken)) {
                    on();
                } else if (afterBetween && "and".equals(lcToken)) {
                    misc();
                    afterBetween = false;
                } else if (LOGICAL.contains(lcToken)) {
                    logical();
                } else if (isWhitespace(token)) {
                    white();
                } else {
                    misc();
                }
                if (!isWhitespace(token)) {
                    lastToken = lcToken;
                }
            }
            return result.toString();
        }

        private void commaAfterOn() {
            out();
            indent--;
            newline();
            afterOn = false;
            afterByOrSetOrFromOrSelect = true;
        }

        private void commaAfterByOrFromOrSelect() {
            out();
            newline();
        }

        private void logical() {
            if ("end".equals(lcToken)) {
                indent--;
            }
            newline();
            out();
            beginLine = false;
        }

        private void on() {
            indent++;
            afterOn = true;
            newline();
            out();
            beginLine = false;
        }

        private void misc() {
            out();
            if ("between".equals(lcToken)) {
                afterBetween = true;
            }
            if (afterInsert) {
                newline();
                afterInsert = false;
            } else {
                beginLine = false;
                if ("case".equals(lcToken)) {
                    indent++;
                }
            }
        }

        private void white() {
            if (!beginLine) {
                result.append(" ");
            }
        }

        private void updateOrInsertOrDelete() {
            out();
            indent++;
            beginLine = false;
            if ("update".equals(lcToken)) {
                newline();
            }
            if ("insert".equals(lcToken)) {
                afterInsert = true;
            }
        }

        private void select() {
            out();
            indent++;
            newline();
            parenCounts.addLast(parensSinceSelect);
            afterByOrFromOrSelects.addLast(afterByOrSetOrFromOrSelect);
            parensSinceSelect = 0;
            afterByOrSetOrFromOrSelect = true;
        }

        private void out() {
            result.append(token);
        }

        private void endNewClause() {
            if (!afterBeginBeforeEnd) {
                indent--;
                if (afterOn) {
                    indent--;
                    afterOn = false;
                }
                newline();
            }
            out();
            if (!"union".equals(lcToken)) {
                indent++;
            }
            newline();
            afterBeginBeforeEnd = false;
            afterByOrSetOrFromOrSelect = "by".equals(lcToken)
                    || "set".equals(lcToken)
                    || "from".equals(lcToken);
        }

        private void beginNewClause() {
            if (!afterBeginBeforeEnd) {
                if (afterOn) {
                    indent--;
                    afterOn = false;
                }
                indent--;
                newline();
            }
            out();
            beginLine = false;
            afterBeginBeforeEnd = true;
        }

        private void values() {
            indent--;
            newline();
            out();
            indent++;
            newline();
            afterValues = true;
        }

        private void closeParen() {
            parensSinceSelect--;
            if (parensSinceSelect < 0) {
                indent--;
                parensSinceSelect = parenCounts.removeLast();
                afterByOrSetOrFromOrSelect = afterByOrFromOrSelects.removeLast();
            }
            if (inFunction > 0) {
                inFunction--;
            } else {
                if (!afterByOrSetOrFromOrSelect) {
                    indent--;
                    newline();
                }
            }
            out();
            beginLine = false;
        }

        private void openParen() {
            if (isFunctionName(lastToken) || inFunction > 0) {
                inFunction++;
            }
            beginLine = false;
            if (inFunction > 0) {
                out();
            } else {
                out();
                if (!afterByOrSetOrFromOrSelect) {
                    indent++;
                    newline();
                    beginLine = true;
                }
            }
            parensSinceSelect++;
        }

        private static boolean isFunctionName(String tok) {
            final char begin = tok.charAt(0);
            final boolean isIdentifier = Character.isJavaIdentifierStart(begin) || '"' == begin;
            return isIdentifier &&
                    !LOGICAL.contains(tok) &&
                    !END_CLAUSES.contains(tok) &&
                    !QUANTIFIERS.contains(tok) &&
                    !DML.contains(tok) &&
                    !MISC.contains(tok);
        }

        private static boolean isWhitespace(String token) {
            return " ".contains(token);
        }

        private void newline() {
            result.append("\n");
            for (int i = 0; i < indent; i++) {
                result.append(INDENT_STRING);
            }
            beginLine = true;
        }
    }
}
