#include <gtest/gtest.h>

#include <Interpreters/EliminateSubqueryVisitor.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

using namespace DB;

static String optimize_subquery(String query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    ParserQuery parser(end);
    ASTPtr ast = parseQuery(parser, start, query.data() + query.size(), "", 0, 0);
    EliminateSubqueryVisitor().visit(ast);
    return serializeAST(*ast);
}

TEST(EliminateSubquery, OptimizeQuery)
{
    EXPECT_EQ(optimize_subquery("SELECT count(*) FROM (SELECT * FROM access1)"), "SELECT count(*) FROM access1");
    EXPECT_EQ(optimize_subquery("SELECT count(1) FROM (SELECT * FROM access1 a)"), "SELECT count(1) FROM access1 AS a");
    EXPECT_EQ(
        optimize_subquery("SELECT count(*) from (SELECT a.* FROM infoflow_url_data_dws a)"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a");
    EXPECT_EQ(
        optimize_subquery("SELECT count(*) from (SELECT a.sign FROM infoflow_url_data_dws a)"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a");
    EXPECT_EQ(
        optimize_subquery("SELECT count(*) FROM (SELECT a.sign FROM infoflow_url_data_dws a WHERE a.sign = 'abcd')"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a WHERE a.sign = 'abcd'");
    EXPECT_EQ(
        optimize_subquery("SELECT count(*) FROM (SELECT a.sign, a.channel FROM infoflow_url_data_dws a WHERE a.sign = 'abcd') WHERE "
                          "channel = 'online'"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a WHERE (a.sign = 'abcd') AND (channel = 'online')");
    EXPECT_EQ(
        optimize_subquery("SELECT product_name, Code, sum(asBaseName0) asBaseName0, sum(asBaseName01) asBaseName01 FROM (SELECT "
                          "price asBaseName0, "
                          "sale_price asBaseName01, product_name product_name, Code Code FROM price) GROUP BY product_name, Code"),
        "SELECT product_name, Code, sum(price) AS asBaseName0, sum(sale_price) AS asBaseName01 FROM price GROUP BY product_name, "
        "Code");
    EXPECT_EQ(
        optimize_subquery("SELECT count(a.productid) FROM (SELECT a.*, b.* FROM access1 a, db2.product_info b WHERE (a.productid = "
                          "b.productid))"),
        "SELECT count(a.productid) FROM access1 AS a , db2.product_info AS b WHERE a.productid = b.productid");
    EXPECT_EQ(
        optimize_subquery("SELECT count(*) FROM (SELECT SIGN FROM (SELECT SIGN, query FROM tablea WHERE query_id='mock-1') WHERE "
                          "query = "
                          "'query_sql') WHERE SIGN = 'external'"),
        "SELECT count(*) FROM tablea WHERE ((query_id = 'mock-1') AND (query = 'query_sql')) AND (SIGN = 'external')");
    EXPECT_EQ(
        optimize_subquery("SELECT sum(a.price), sum(b.sale) FROM (SELECT a.*, b.* FROM access1 a, product_info b WHERE (a.productid "
                          "= "
                          "b.productid)) GROUP BY a.name, b.sign"),
        "SELECT sum(a.price), sum(b.sale) FROM access1 AS a , product_info AS b WHERE a.productid = b.productid GROUP BY a.name, "
        "b.sign");
    EXPECT_EQ(
        optimize_subquery("SELECT sum(a.price), sum(b.sale) FROM (SELECT a.*, b.* FROM access1 a JOIN product_info b ON a.productid "
                          "= "
                          "b.productid) WHERE a.name = 'foo' GROUP BY a.name, b.sign"),
        "SELECT sum(a.price), sum(b.sale) FROM access1 AS a INNER JOIN product_info AS b ON a.productid = b.productid WHERE a.name = "
        "'foo' "
        "GROUP BY a.name, b.sign");
    EXPECT_EQ(optimize_subquery("SELECT a, b FROM (SELECT * FROM (SELECT 1)) WHERE y < 1"), "SELECT a, b FROM (SELECT 1) WHERE y < 1");
    EXPECT_EQ(
        optimize_subquery("SELECT a, ssp FROM (SELECT sum(show_pv) ssp, a FROM (SELECT * FROM infoflow_url_data_dws))"),
        "SELECT a, ssp FROM (SELECT sum(show_pv) AS ssp, a FROM infoflow_url_data_dws)");
    EXPECT_EQ(
        optimize_subquery("SELECT a, b from (select * from users1) UNION SELECT c, d from (select * from users2)"),
        "SELECT a, b FROM users1 UNION  SELECT c, d FROM users2");
}
