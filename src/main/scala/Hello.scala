import java.sql.DriverManager
import org.apache.spark.sql.SparkSession
import java.util.Properties

object DownloadDataFrameToRDD {
  def main(args: Array[String]): Unit = {

    // cria uma sessão do Spark
    val spark = SparkSession.builder()
      .appName("myapp")
      .master("local[*]")
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306/mysql"
    val user = "root"
    val password = ""
    val connection = DriverManager.getConnection(url, user, password)
    val query = "CREATE TABLE IF NOT EXISTS mytable (id INT PRIMARY KEY, name VARCHAR(255))"
    
    // executa a consulta SQL para criar a tabela
    val statement = connection.createStatement()
    // statement.execute(query)

    // // insere os dados na tabela
    // val data = Seq(
    //   (1, "John"),
    //   (2, "Jane"),
    //   (3, "Bob")
    // )
    // val columns = Seq("id", "name")
    // val tableName = "mytable"

    // val insertSqls = data.map { row =>
    //   val values = row.productIterator.map(_.toString).map("'" + _ + "'").mkString(", ")
    //   s"INSERT INTO $tableName (${columns.mkString(", ")}) VALUES ($values);"
    // }
    // insertSqls.foreach(statement.execute)

    // executa a consulta SQL para selecionar os dados da tabela
    val query_sel = "SELECT * from ml_more"
    val resultSet = statement.executeQuery(query_sel)
    while (resultSet.next()) {
      val id = resultSet.getString(1)
      val name = resultSet.getString(2)
      println(s"id: $id, name: $name")
    }
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    // Leitura da tabela cde Iphones para processar

    val df = spark.read.jdbc(url,"ml_more",connectionProperties)

    //Remoção de coluna desnecessária
    val df_fix = df.drop("valor_parcelado")
    // Printando tabela
    df_fix.show()
    //Criação de view temp para spark.sql
    df_fix.createOrReplaceTempView("ml_more")
    // Usando spark sql para tratar dados
    spark.sql("""select * from ml_more where price < 9.000
    """).show( )
    // fecha a conexão e libera os recursos
    resultSet.close()
    statement.close()
    connection.close()
  }
}
