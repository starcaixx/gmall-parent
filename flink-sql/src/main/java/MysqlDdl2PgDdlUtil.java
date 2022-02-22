import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlDdl2PgDdlUtil {

    public void traverseFolder(String path) throws JSQLParserException, IOException {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder(file2.getAbsolutePath());
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                        String mysqlDDLPath = file2.getAbsolutePath();
                        transMysql2Pg(mysqlDDLPath);
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    public void testListFile() throws IOException, JSQLParserException {
        String mysqlDDLPath = "D:\\work\\PycharmProjects\\data_platform\\schema\\ods\\";
        traverseFolder(mysqlDDLPath);
    }

    public static void main(String[] args) throws IOException, JSQLParserException {
        // 你的MySQL DDL路径
//        String mysqlDDLPath = "D:\\input\\mysql.sql";
        String mysqlDDLPath = "D:\\work\\PycharmProjects\\data_platform\\schema\\ods\\ODS_UPCHINA__PUB_COM_INDU_CHAN.sql";

        transMysql2Pg(mysqlDDLPath);
//        transMysql2Pg(dDLs);
    }

    public static void transMysql2Pg(String path) throws JSQLParserException, IOException {
        String dDLs = FileUtils.readFileToString(new File(path)).toLowerCase();
        System.out.println(dDLs);
        System.out.println("++++++++++开始转换SQL语句+++++++++++++");

        Statements statements = CCJSqlParserUtil.parseStatements(dDLs);

        statements.getStatements()
                .stream()
                .map(statement -> (CreateTable) statement).forEach(ct -> {
            Table table = ct.getTable();
            List<ColumnDefinition> columnDefinitions = ct.getColumnDefinitions();
            List<String> comments = new ArrayList<>();
            List<ColumnDefinition> collect = columnDefinitions.stream()
                    .peek(columnDefinition -> {
//                        System.out.println("columnDefinition:"+columnDefinition);
                        List<String> columnSpecStrings = columnDefinition.getColumnSpecStrings();

                        if (columnSpecStrings != null) {
//                            System.out.println("columnSpecStrings:"+columnSpecStrings);
                            int commentIndex = getCommentIndex(columnSpecStrings);

                            if (commentIndex != -1) {
                                int commentStringIndex = commentIndex + 1;
                                String commentString = columnSpecStrings.get(commentStringIndex);

                                String commentSql = genCommentSql(table.toString(), columnDefinition.getColumnName(), commentString);
                                comments.add(commentSql);
                                columnSpecStrings.remove(commentStringIndex);
                                columnSpecStrings.remove(commentIndex);
                            }
                            columnDefinition.setColumnSpecStrings(columnSpecStrings);
                        }

                    }).collect(Collectors.toList());
            ct.setColumnDefinitions(collect);
            System.out.println("size:"+collect.size());

            String createSQL = ct.toString().replaceAll("(\\D),\\s","$1,\n")
                    .replaceAll("`", "\"")
                    .replaceAll("BIGINT UNIQUE NOT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("BIGINT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("BIGINT NOT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("INT NOT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("INT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("IF NOT EXISTS", "")
                    .replaceAll("TINYINT", "SMALLINT")
                    .replaceAll("DATETIME", "TIMESTAMP")
                    .replaceAll(", PRIMARY KEY \\(\"id\"\\)", "");

            // 如果存在表注释
            if (createSQL.contains("COMMENT")) {
                createSQL = createSQL.substring(0, createSQL.indexOf("COMMENT"));
            }
            System.out.println(createSQL + ";");


            comments.forEach(t -> System.out.println(t.replaceAll("`", "\"") + ";"));
            FileWriter fw = null;
            try {
                fw = new FileWriter(path,true);
                fw.write("\n\n\n");
                fw.write(createSQL+";\n");
                for (String comment : comments) {
                    fw.write(comment.replaceAll("`", "\"")+";\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if (fw != null) {
                    try {
                        fw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        });
    }

    /**
     * 获得注释的下标
     *
     * @param columnSpecStrings columnSpecStrings
     * @return 下标
     */
    private static int getCommentIndex(List<String> columnSpecStrings) {
        for (int i = 0; i < columnSpecStrings.size(); i++) {
            if ("COMMENT".equalsIgnoreCase(columnSpecStrings.get(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 生成COMMENT语句
     *
     * @param table        表名
     * @param column       字段名
     * @param commentValue 描述文字
     * @return COMMENT语句
     */
    private static String genCommentSql(String table, String column, String commentValue) {
        return String.format("COMMENT ON COLUMN %s.%s IS %s", table, column, commentValue);
    }
}
